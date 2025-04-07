//! A client processor is responsible for processing transactions for a single client.
//!
//! It works with any value that implements the `BalanceUpdater` trait. It does not store
//! the `total` balance as it can always be derived from `held` and `available`.

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
    db::DepositValueCache,
    error::Error,
    transaction::{
        Chargeback, Deposit, Dispute, Resolve, Transaction, TransactionPayload, Withdrawal,
    },
};

pub(super) enum TransactionProcessingOutcome {
    LockAccount,
    NoAction,
}

pub(super) trait TransactionProcessor<Database, MonetaryValue>
where
    Database: DepositValueCache<MonetaryValue>,
    MonetaryValue: BalanceUpdater + Copy,
{
    fn process(
        self,
        processor: &mut ClientProcessor<Database, MonetaryValue>,
    ) -> Result<TransactionProcessingOutcome, Error>;
}

impl<Database, MonetaryValue> TransactionProcessor<Database, MonetaryValue>
    for TransactionPayload<Deposit, MonetaryValue>
where
    Database: DepositValueCache<MonetaryValue>,
    MonetaryValue: BalanceUpdater + Copy,
{
    fn process(
        self,
        processor: &mut ClientProcessor<Database, MonetaryValue>,
    ) -> Result<TransactionProcessingOutcome, Error> {
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
        Ok(TransactionProcessingOutcome::NoAction)
    }
}

impl<Database, MonetaryValue> TransactionProcessor<Database, MonetaryValue>
    for TransactionPayload<Withdrawal, MonetaryValue>
where
    Database: DepositValueCache<MonetaryValue>,
    MonetaryValue: BalanceUpdater + Copy,
{
    fn process(
        self,
        processor: &mut ClientProcessor<Database, MonetaryValue>,
    ) -> Result<TransactionProcessingOutcome, Error> {
        let amount = self.amount();
        processor.balances.withdrawal(*amount)?;
        Ok(TransactionProcessingOutcome::NoAction)
    }
}

impl<Database, MonetaryValue> TransactionProcessor<Database, MonetaryValue>
    for TransactionPayload<Dispute, MonetaryValue>
where
    Database: DepositValueCache<MonetaryValue>,
    MonetaryValue: BalanceUpdater + Copy,
{
    fn process(
        self,
        processor: &mut ClientProcessor<Database, MonetaryValue>,
    ) -> Result<TransactionProcessingOutcome, Error> {
        if processor.disputed.contains_key(&self.tx()) {
            return Ok(TransactionProcessingOutcome::NoAction);
        }
        if let Some(amount) = processor.db.get(&self.tx()) {
            processor.balances.dispute(*amount)?;
            // TODO: Attack vector. One could try to dispute millions of transactions
            // and never submit `resolve` or `chargeback`, trying to grow this map
            // indefinitely. We should probably limit the amount of simultaneous disputes.
            processor.disputed.insert(self.tx(), *amount);
        };
        Ok(TransactionProcessingOutcome::NoAction)
    }
}

impl<Database, MonetaryValue> TransactionProcessor<Database, MonetaryValue>
    for TransactionPayload<Resolve, MonetaryValue>
where
    Database: DepositValueCache<MonetaryValue>,
    MonetaryValue: BalanceUpdater + Copy,
{
    fn process(
        self,
        processor: &mut ClientProcessor<Database, MonetaryValue>,
    ) -> Result<TransactionProcessingOutcome, Error> {
        if let Some(amount) = processor.disputed.get(&self.tx()) {
            processor.balances.resolve(*amount)?;
            processor.disputed.remove(&self.tx());
        };
        Ok(TransactionProcessingOutcome::NoAction)
    }
}

impl<Database, MonetaryValue> TransactionProcessor<Database, MonetaryValue>
    for TransactionPayload<Chargeback, MonetaryValue>
where
    Database: DepositValueCache<MonetaryValue>,
    MonetaryValue: BalanceUpdater + Copy,
{
    fn process(
        self,
        processor: &mut ClientProcessor<Database, MonetaryValue>,
    ) -> Result<TransactionProcessingOutcome, Error> {
        if let Some(amount) = processor.disputed.get(&self.tx()) {
            processor.balances.chargeback(*amount)?;
            processor.disputed.remove(&self.tx());
            return Ok(TransactionProcessingOutcome::LockAccount);
        };
        Ok(TransactionProcessingOutcome::NoAction)
    }
}

/// Represents the final client state after all transactions have been processed.
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
    Database: DepositValueCache<MonetaryValue>,
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
    tx_receiver: mpsc::Receiver<Transaction<MonetaryValue>>,
    // The channel to send the result back to the stream processor.
    result_sender: Option<oneshot::Sender<ClientState<MonetaryValue>>>,
}

impl<Database, MonetaryValue> ClientProcessor<Database, MonetaryValue>
where
    Database: DepositValueCache<MonetaryValue>,
    MonetaryValue: BalanceUpdater + Copy,
{
    pub(super) fn new(
        client: u16,
        db: Database,
        tx_receiver: mpsc::Receiver<Transaction<MonetaryValue>>,
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

    fn process<Kind>(
        &mut self,
        tx: TransactionPayload<Kind, MonetaryValue>,
    ) -> Result<TransactionProcessingOutcome, Error>
    where
        TransactionPayload<Kind, MonetaryValue>: TransactionProcessor<Database, MonetaryValue>,
    {
        tx.process(self)
    }

    pub(super) async fn crank(&mut self, tx_counter: Arc<AtomicUsize>) -> Result<(), Error> {
        while let Some(tx) = self.tx_receiver.recv().await {
            if !self.locked {
                let tx_process_result = match tx {
                    Transaction::Deposit(tx) => self.process(tx),
                    Transaction::Withdrawal(tx) => self.process(tx),
                    Transaction::Dispute(tx) => self.process(tx),
                    Transaction::Resolve(tx) => self.process(tx),
                    Transaction::Chargeback(tx) => self.process(tx),
                };
                match tx_process_result {
                    Ok(outcome) => {
                        if let TransactionProcessingOutcome::LockAccount = outcome {
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
