use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use futures_util::{Stream, StreamExt};
use tokio::sync::mpsc;

use crate::{Balances, ClientProcessor, Record, TxPayload, in_mem};

// TODO: Think about backpressure
const TX_CHANNEL_SIZE: usize = 1000;
const RESULTS_CHANNEL_SIZE: usize = 100;

pub(super) struct StreamProcessor {
    client_processors: HashMap<u16, mpsc::Sender<TxPayload>>,
    result_receivers: HashMap<u16, mpsc::Receiver<Balances>>,
}

impl StreamProcessor {
    pub(super) fn new() -> Self {
        Self {
            client_processors: HashMap::new(),
            result_receivers: HashMap::new(),
        }
    }

    pub(super) async fn process(
        &mut self,
        mut stream: impl Stream<Item = Result<Record, csv_async::Error>> + Unpin,
    ) -> anyhow::Result<()> {
        let active_transactions = Arc::new(AtomicUsize::new(0));
        while let Some(record) = stream.next().await {
            let Record {
                kind,
                client,
                tx,
                amount,
            } = record?;
            let active_tx_clone = Arc::clone(&active_transactions);

            let client_processor = self.client_processors.get(&client);
            match client_processor {
                Some(sender) => {
                    active_tx_clone.fetch_add(1, Ordering::SeqCst);
                    sender.send(TxPayload::new(kind, tx, amount)).await?;
                }
                None => {
                    let (tx_sender, tx_receiver) = mpsc::channel(TX_CHANNEL_SIZE);
                    let (result_sender, result_receiver) = mpsc::channel(RESULTS_CHANNEL_SIZE);
                    // TODO: Consider worker pool? When there are millions of clients the current approach could be problematic.
                    let client_db = in_mem::AmountCache::new();
                    let mut client_processor =
                        ClientProcessor::new(client, client_db, tx_receiver, result_sender);
                    self.client_processors.insert(client, tx_sender.clone());
                    self.result_receivers.insert(client, result_receiver);
                    tokio::spawn(async move {
                        let _result = client_processor.crank(Arc::clone(&active_tx_clone)).await;
                        // TODO: Handle errors gracefully
                    });
                    // TODO: Consider Overflow check for atomic
                    active_transactions.fetch_add(1, Ordering::SeqCst);
                    tx_sender.send(TxPayload::new(kind, tx, amount)).await?;
                }
            }
        }

        // TODO: Busy waiting at the end to make sure all txs are processed.
        // Could potentially be improved with a more elegant solution.
        while active_transactions.load(Ordering::SeqCst) > 0 {
            println!(
                "waiting for {} transactions to finish",
                active_transactions.load(Ordering::SeqCst)
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }

        // We only drop senders after all transactions are processed.
        self.client_processors = HashMap::new();

        // Collect results
        for (client, receiver) in self.result_receivers.iter_mut() {
            while let Some(balances) = receiver.recv().await {
                println!("client {}: {:?}", client, balances);
            }
        }

        Ok(())
    }
}
