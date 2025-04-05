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

pub(super) struct ClientState<V>
where
    V: BalanceUpdater + Copy,
{
    client: u16,
    locked: bool,
    balances: Balances<V>,
}

impl<V> ClientState<V>
where
    V: BalanceUpdater + Copy,
{
    pub(super) fn balances(&self) -> &Balances<V> {
        &self.balances
    }

    pub(super) fn client(&self) -> u16 {
        self.client
    }

    pub(super) fn locked(&self) -> bool {
        self.locked
    }
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
    result_sender: mpsc::Sender<ClientState<V>>,
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
        result_sender: mpsc::Sender<ClientState<V>>,
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
                if self.disputed.contains_key(&tx.tx()) {
                    return Ok(TxProcessingOutcome::NoAction);
                }
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
        while let Some(tx) = self.tx_receiver.recv().await {
            if !self.locked {
                let tx_process_result = self.process_tx(tx);
                match tx_process_result {
                    Ok(outcome) => {
                        if let TxProcessingOutcome::LockAccount = outcome {
                            self.locked = true;
                        }
                    }
                    Err(_e) => {
                        // TODO: Proper error handling
                        // tracing::error!("Error processing transaction: {:?}", _e);
                    }
                }
            }
            tx_counter.fetch_sub(1, Ordering::SeqCst);
        }

        self.result_sender
            .send(ClientState {
                client: self.client,
                locked: self.locked,
                balances: self.balances.clone(),
            })
            .await
            .unwrap_or_else(|_| {
                println!("failed to send result for client {}", self.client);
            });

        Ok(())
    }
}
