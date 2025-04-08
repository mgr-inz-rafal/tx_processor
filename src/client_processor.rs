//! A client processor is responsible for processing transactions for a single client.
//!
//! It does not process the `total` balance as it can always be derived from `held` and `available`.

use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use tokio::sync::{mpsc, oneshot};

use crate::{
    Balances, NonZero,
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

pub(super) trait TransactionProcessor<Database>
where
    Database: DepositValueCache<NonZero>,
{
    fn process(
        self,
        processor: &mut ClientProcessor<Database>,
    ) -> Result<TransactionProcessingOutcome, Error>;
}

impl<Database> TransactionProcessor<Database> for TransactionPayload<Deposit>
where
    Database: DepositValueCache<NonZero>,
{
    fn process(
        self,
        processor: &mut ClientProcessor<Database>,
    ) -> Result<TransactionProcessingOutcome, Error> {
        let amount = self.amount();
        let id = self.tx();
        processor
            .balances
            .deposit(amount.into())
            .map_err(|_| Error::InvalidTransaction { id })?;
        processor
            .db
            .insert(self.tx(), self)
            .map_err(|_| Error::DuplicatedTransaction { id })?;
        Ok(TransactionProcessingOutcome::NoAction)
    }
}

impl<Database> TransactionProcessor<Database> for TransactionPayload<Withdrawal>
where
    Database: DepositValueCache<NonZero>,
{
    fn process(
        self,
        processor: &mut ClientProcessor<Database>,
    ) -> Result<TransactionProcessingOutcome, Error> {
        let amount = self.amount();
        processor.balances.withdrawal(amount.into())?;
        Ok(TransactionProcessingOutcome::NoAction)
    }
}

impl<Database> TransactionProcessor<Database> for TransactionPayload<Dispute>
where
    Database: DepositValueCache<NonZero>,
{
    fn process(
        self,
        processor: &mut ClientProcessor<Database>,
    ) -> Result<TransactionProcessingOutcome, Error> {
        if processor.disputed.contains_key(&self.tx()) {
            return Ok(TransactionProcessingOutcome::NoAction);
        }
        if let Some(amount) = processor.db.get(&self.tx()) {
            processor.balances.dispute(amount.into())?;
            // TODO: Attack vector. One could try to dispute millions of transactions
            // and never submit `resolve` or `chargeback`, trying to grow this map
            // indefinitely. We should probably limit the amount of simultaneous disputes.
            processor.disputed.insert(self.tx(), *amount);
        };
        Ok(TransactionProcessingOutcome::NoAction)
    }
}

impl<Database> TransactionProcessor<Database> for TransactionPayload<Resolve>
where
    Database: DepositValueCache<NonZero>,
{
    fn process(
        self,
        processor: &mut ClientProcessor<Database>,
    ) -> Result<TransactionProcessingOutcome, Error> {
        if let Some(amount) = processor.disputed.get(&self.tx()) {
            processor.balances.resolve(amount.into())?;
            processor.disputed.remove(&self.tx());
        };
        Ok(TransactionProcessingOutcome::NoAction)
    }
}

impl<Database> TransactionProcessor<Database> for TransactionPayload<Chargeback>
where
    Database: DepositValueCache<NonZero>,
{
    fn process(
        self,
        processor: &mut ClientProcessor<Database>,
    ) -> Result<TransactionProcessingOutcome, Error> {
        if let Some(amount) = processor.disputed.get(&self.tx()) {
            processor.balances.chargeback(amount.into())?;
            processor.disputed.remove(&self.tx());
            return Ok(TransactionProcessingOutcome::LockAccount);
        };
        Ok(TransactionProcessingOutcome::NoAction)
    }
}

/// Represents the final client state after all transactions have been processed.
pub(super) struct ClientState {
    client: u16,
    locked: bool,
    balances: Balances,
}

impl ClientState {
    pub(super) fn balances(&self) -> &Balances {
        &self.balances
    }

    pub(super) fn client(&self) -> u16 {
        self.client
    }

    pub(super) fn locked(&self) -> bool {
        self.locked
    }
}

pub(super) struct ClientProcessor<Database>
where
    Database: DepositValueCache<NonZero>,
{
    // Client ID
    client: u16,
    // Each client takes care of its own balance.
    balances: Balances,
    // The account is locked if there was a chargeback.
    locked: bool,
    // Abstracted database. It could be anything that can store and retrieve
    // values. For smaller sets we can use in-mem HashMap, but for more
    // heavy task this should be a proper storage solution.
    db: Database,
    // The map of amounts being disputed. It is not abstracted due to the
    // assumption that there will be a limited number of active disputes
    // compared to the total number of transactions.
    disputed: HashMap<u32, NonZero>,
    // The channel to receive transactions from the stream processor.
    tx_receiver: mpsc::Receiver<Transaction>,
    // The channel to send the result back to the stream processor.
    result_sender: Option<oneshot::Sender<ClientState>>,
}

impl<Database> ClientProcessor<Database>
where
    Database: DepositValueCache<NonZero>,
{
    pub(super) fn new(
        client: u16,
        db: Database,
        tx_receiver: mpsc::Receiver<Transaction>,
        result_sender: oneshot::Sender<ClientState>,
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
        tx: TransactionPayload<Kind>,
    ) -> Result<TransactionProcessingOutcome, Error>
    where
        TransactionPayload<Kind>: TransactionProcessor<Database>,
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
