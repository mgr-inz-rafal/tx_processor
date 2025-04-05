use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use tokio::sync::mpsc;

use crate::{
    Balances,
    db::ValueCache,
    error::Error,
    traits::BalanceUpdater,
    transaction::{TxPayload, TxType},
};

enum TxProcessingOutcome {
    LockAccount,
    NoAction,
}

pub(super) struct ClientProcessor<D, V>
where
    D: ValueCache<V>,
    V: BalanceUpdater + Copy,
{
    client: u16,
    balances: Balances<V>,
    locked: bool,
    db: D,
    // Not abstracted, assuming there will be a limited number of active disputes
    // compared to the total number of transactions.
    disputed: HashMap<u32, V>,
    tx_receiver: mpsc::Receiver<TxPayload<V>>,
    result_sender: mpsc::Sender<Balances<V>>,
}

impl<D, V> ClientProcessor<D, V>
where
    D: ValueCache<V>,
    V: BalanceUpdater + Copy,
{
    pub(super) fn new(
        client: u16,
        db: D,
        tx_receiver: mpsc::Receiver<TxPayload<V>>,
        result_sender: mpsc::Sender<Balances<V>>,
    ) -> Self {
        Self {
            client,
            balances: Balances::new(),
            disputed: HashMap::new(),
            db,
            locked: false,
            tx_receiver,
            result_sender,
        }
    }

    fn process_tx(&mut self, tx: TxPayload<V>) -> Result<TxProcessingOutcome, Error> {
        match tx.kind() {
            TxType::Deposit => {
                let amount = tx
                    .amount()
                    .ok_or(Error::InvalidTransaction { id: tx.tx() })?;
                self.balances.deposit(amount)?;
                self.db.insert(tx.tx(), amount);
            }
            TxType::Withdrawal => {
                let amount = tx.amount().expect("Withdrawal must have an amount");
                self.balances.withdrawal(amount)?;
            }
            TxType::Dispute => {
                if let Some(amount) = self.db.get(&tx.tx()) {
                    self.balances.dispute(*amount)?;
                    self.disputed.insert(tx.tx(), *amount);
                };
            }
            TxType::Resolve => {
                if let Some(amount) = self.disputed.get(&tx.tx()) {
                    self.balances.resolve(*amount)?;
                    self.disputed.remove(&tx.tx());
                };
            }
            TxType::Chargeback => {
                if let Some(amount) = self.disputed.get(&tx.tx()) {
                    self.balances.chargeback(*amount)?;
                    self.disputed.remove(&tx.tx());
                    return Ok(TxProcessingOutcome::LockAccount);
                };
            }
        }

        Ok(TxProcessingOutcome::NoAction)
    }

    pub(super) async fn crank(&mut self, tx_counter: Arc<AtomicUsize>) -> Result<(), Error> {
        loop {
            match self.tx_receiver.recv().await {
                Some(tx) => {
                    if !self.locked {
                        if let TxProcessingOutcome::LockAccount = self.process_tx(tx)? {
                            self.locked = true;
                        }
                    } 
                    tx_counter.fetch_sub(1, Ordering::SeqCst);
                }
                None => {
                    println!("client {} shutting down", self.client);
                    break;
                }
            }
        }

        println!(
            "client {} processed all transactions, sending results",
            self.client
        );
        self.result_sender
            .send(self.balances.clone())
            .await
            .unwrap_or_else(|_| {
                println!("failed to send result for client {}", self.client);
            });

        Ok(())
    }
}
