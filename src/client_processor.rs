use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use rust_decimal::Decimal;
use tokio::sync::mpsc;

use crate::{
    Balances,
    db::ValueCache,
    error::Error,
    transaction::{TxPayload, TxType},
};

pub(super) struct ClientProcessor<D>
where
    D: ValueCache,
{
    client: u16,
    balances: Balances,
    db: D,
    // Not abstracted, assuming there will be a limited number of active disputes
    // compared to the total number of transactions.
    disputed: HashMap<u32, Decimal>,
    tx_receiver: mpsc::Receiver<TxPayload>,
    result_sender: mpsc::Sender<Balances>,
}

impl<D> ClientProcessor<D>
where
    D: ValueCache,
{
    pub(super) fn new(
        client: u16,
        db: D,
        tx_receiver: mpsc::Receiver<TxPayload>,
        result_sender: mpsc::Sender<Balances>,
    ) -> Self {
        Self {
            client,
            balances: Balances::new(),
            disputed: HashMap::new(),
            db,
            tx_receiver,
            result_sender,
        }
    }

    fn process_tx(&mut self, tx: TxPayload) -> Result<(), Error> {
        match tx.kind() {
            TxType::Deposit => {
                let amount = tx
                    .amount()
                    .ok_or(Error::InvalidTransaction { id: tx.tx() })?;
                self.balances.deposit(amount);
                self.db.insert(tx.tx(), amount);
            }
            TxType::Withdrawal => {
                let amount = tx.amount().expect("Withdrawal must have an amount");
                self.balances.withdrawal(amount);
            }
            TxType::Dispute => {
                if let Some(amount) = self.db.get(&tx.tx()) {
                    self.disputed.insert(tx.tx(), *amount);
                    self.balances.dispute(*amount)
                };
            }
            TxType::Resolve => {
                if let Some(amount) = self.disputed.get(&tx.tx()) {
                    self.balances.resolve(*amount);
                    self.disputed.remove(&tx.tx());
                };
            }
            TxType::Chargeback => {
                if let Some(amount) = self.disputed.get(&tx.tx()) {
                    self.balances.chargeback(*amount);
                    self.disputed.remove(&tx.tx());
                };
            }
        }

        Ok(())
    }

    pub(super) async fn crank(&mut self, tx_counter: Arc<AtomicUsize>) -> Result<(), Error> {
        loop {
            match self.tx_receiver.recv().await {
                Some(tx) => {
                    println!("processing tx for client {}: {:?}", self.client, tx);
                    self.process_tx(tx)?;
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
