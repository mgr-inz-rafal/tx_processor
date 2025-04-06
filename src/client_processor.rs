use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use tokio::sync::{mpsc, oneshot};

use crate::{
    Balances,
    balances::BalanceUpdater,
    db::ValueCache,
    error::Error,
    transaction::{Chargeback, Deposit, Dispute, Resolve, Transaction, Tx, Withdrawal},
};

pub(super) enum TxProcessingOutcome {
    LockAccount,
    NoAction,
}

pub(super) trait TransactionKind<Database, MonetaryValue>
where
    Database: ValueCache<MonetaryValue>,
    MonetaryValue: BalanceUpdater + Copy,
{
    fn process(
        self,
        processor: &mut ClientProcessor<Database, MonetaryValue>,
    ) -> Result<TxProcessingOutcome, Error>;
}

impl<Database, MonetaryValue> TransactionKind<Database, MonetaryValue>
    for Transaction<Deposit, MonetaryValue>
where
    Database: ValueCache<MonetaryValue>,
    MonetaryValue: BalanceUpdater + Copy,
{
    fn process(
        self,
        processor: &mut ClientProcessor<Database, MonetaryValue>,
    ) -> Result<TxProcessingOutcome, Error> {
        let amount = self.amount();
        let id = self.tx();
        processor
            .balances
            .deposit(*amount)
            .map_err(|_| Error::InvalidTransaction { id })?;
        processor
            .db
            .insert(self.tx(), self)
            .map_err(|_| Error::DuplicatedTransaction { id })?;
        Ok(TxProcessingOutcome::NoAction)
    }
}

impl<Database, MonetaryValue> TransactionKind<Database, MonetaryValue>
    for Transaction<Withdrawal, MonetaryValue>
where
    Database: ValueCache<MonetaryValue>,
    MonetaryValue: BalanceUpdater + Copy,
{
    fn process(
        self,
        processor: &mut ClientProcessor<Database, MonetaryValue>,
    ) -> Result<TxProcessingOutcome, Error> {
        let amount = self.amount();
        processor.balances.withdrawal(*amount)?;
        Ok(TxProcessingOutcome::NoAction)
    }
}

impl<Database, MonetaryValue> TransactionKind<Database, MonetaryValue>
    for Transaction<Dispute, MonetaryValue>
where
    Database: ValueCache<MonetaryValue>,
    MonetaryValue: BalanceUpdater + Copy,
{
    fn process(
        self,
        processor: &mut ClientProcessor<Database, MonetaryValue>,
    ) -> Result<TxProcessingOutcome, Error> {
        if processor.disputed.contains_key(&self.tx()) {
            return Ok(TxProcessingOutcome::NoAction);
        }
        if let Some(amount) = processor.db.get(&self.tx()) {
            processor.balances.dispute(*amount)?;
            // TODO: Attack vector. One could try to dispute millions of transactions
            // and never submit `resolve` or `chargeback`, trying to grow this map
            // indefinitely. We should probably limit the amount of simultaneous disputes.
            processor.disputed.insert(self.tx(), *amount);
        };
        Ok(TxProcessingOutcome::NoAction)
    }
}

impl<Database, MonetaryValue> TransactionKind<Database, MonetaryValue>
    for Transaction<Resolve, MonetaryValue>
where
    Database: ValueCache<MonetaryValue>,
    MonetaryValue: BalanceUpdater + Copy,
{
    fn process(
        self,
        processor: &mut ClientProcessor<Database, MonetaryValue>,
    ) -> Result<TxProcessingOutcome, Error> {
        if let Some(amount) = processor.disputed.get(&self.tx()) {
            processor.balances.resolve(*amount)?;
            processor.disputed.remove(&self.tx());
        };
        Ok(TxProcessingOutcome::NoAction)
    }
}

impl<Database, MonetaryValue> TransactionKind<Database, MonetaryValue>
    for Transaction<Chargeback, MonetaryValue>
where
    Database: ValueCache<MonetaryValue>,
    MonetaryValue: BalanceUpdater + Copy,
{
    fn process(
        self,
        processor: &mut ClientProcessor<Database, MonetaryValue>,
    ) -> Result<TxProcessingOutcome, Error> {
        if let Some(amount) = processor.disputed.get(&self.tx()) {
            processor.balances.chargeback(*amount)?;
            processor.disputed.remove(&self.tx());
            return Ok(TxProcessingOutcome::LockAccount);
        };
        Ok(TxProcessingOutcome::NoAction)
    }
}

pub(super) struct ClientState<MonetaryValue>
where
    MonetaryValue: BalanceUpdater + Copy,
{
    client: u16,
    locked: bool,
    balances: Balances<MonetaryValue>,
}

impl<MonetaryValue> ClientState<MonetaryValue>
where
    MonetaryValue: BalanceUpdater + Copy,
{
    pub(super) fn balances(&self) -> &Balances<MonetaryValue> {
        &self.balances
    }

    pub(super) fn client(&self) -> u16 {
        self.client
    }

    pub(super) fn locked(&self) -> bool {
        self.locked
    }
}

pub(super) struct ClientProcessor<Database, MonetaryValue>
where
    Database: ValueCache<MonetaryValue>,
    MonetaryValue: BalanceUpdater + Copy,
{
    // Client ID
    client: u16,
    // Each client takes care of its own balance.
    balances: Balances<MonetaryValue>,
    // The account is locked if there was a chargeback.
    locked: bool,
    // Abstracted database. It could be anything that can store and retrieve
    // values. For smaller sets we can use in-mem HashMap, but for more
    // heavy task this should be a proper storage solution.
    db: Database,
    // The map of amounts being disputed. It is not abstracted due to the
    // assumption that there will be a limited number of active disputes
    // compared to the total number of transactions.
    disputed: HashMap<u32, MonetaryValue>,
    // The channel to receive transactions from the stream processor.
    tx_receiver: mpsc::Receiver<Tx<MonetaryValue>>,
    // The channel to send the result back to the stream processor.
    result_sender: Option<oneshot::Sender<ClientState<MonetaryValue>>>,
}

impl<Database, MonetaryValue> ClientProcessor<Database, MonetaryValue>
where
    Database: ValueCache<MonetaryValue>,
    MonetaryValue: BalanceUpdater + Copy,
{
    pub(super) fn new(
        client: u16,
        db: Database,
        tx_receiver: mpsc::Receiver<Tx<MonetaryValue>>,
        result_sender: oneshot::Sender<ClientState<MonetaryValue>>,
    ) -> Self {
        Self {
            client,
            balances: Balances::new(),
            disputed: HashMap::new(),
            db,
            locked: false,
            tx_receiver,
            result_sender: Some(result_sender),
        }
    }

    fn process_tx<Kind>(
        &mut self,
        tx: Transaction<Kind, MonetaryValue>,
    ) -> Result<TxProcessingOutcome, Error>
    where
        Transaction<Kind, MonetaryValue>: TransactionKind<Database, MonetaryValue>,
    {
        tx.process(self)
    }

    pub(super) async fn crank(&mut self, tx_counter: Arc<AtomicUsize>) -> Result<(), Error> {
        while let Some(tx) = self.tx_receiver.recv().await {
            if !self.locked {
                let tx_process_result = match tx {
                    Tx::Deposit(tx) => self.process_tx(tx),
                    Tx::Withdrawal(tx) => self.process_tx(tx),
                    Tx::Dispute(tx) => self.process_tx(tx),
                    Tx::Resolve(tx) => self.process_tx(tx),
                    Tx::Chargeback(tx) => self.process_tx(tx),
                };
                match tx_process_result {
                    Ok(outcome) => {
                        if let TxProcessingOutcome::LockAccount = outcome {
                            self.locked = true;
                        }
                    }
                    Err(_e) => {
                        // tracing::error!("Error processing transaction: {:?}", _e);
                    }
                }
            }
            tx_counter.fetch_sub(1, Ordering::SeqCst);
        }

        if let Some(sender) = self.result_sender.take() {
            sender
                .send(ClientState {
                    client: self.client,
                    locked: self.locked,
                    balances: self.balances.clone(),
                })
                .unwrap_or(
                    // tracing::error!("failed to send result for client {}", self.client);
                    (),
                );
        }

        Ok(())
    }
}
