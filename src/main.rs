use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use csv_async::AsyncReaderBuilder;
use futures_util::{Stream, StreamExt};
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::{fs::File, sync::mpsc};
use tokio_util::compat::TokioAsyncReadCompatExt;

// TODO: Think about backpressure
const TX_CHANNEL_SIZE: usize = 1000;
const RESULTS_CHANNEL_SIZE: usize = 100;

#[derive(Debug)]
enum TxType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

impl TxType {
    fn from_deserializer<'de, D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // TODO: Maybe we can avoid the String allocation here. Serde internals could give us &str?
        // Idea to be explored.
        let s = String::deserialize(deserializer)?.to_lowercase();
        match s.as_str() {
            "deposit" => Ok(Self::Deposit),
            "withdrawal" => Ok(Self::Withdrawal),
            "dispute" => Ok(Self::Dispute),
            "resolve" => Ok(Self::Resolve),
            "chargeback" => Ok(Self::Chargeback),
            _ => Err(serde::de::Error::custom(format!(
                "Unknown transaction type: {}",
                s
            ))),
        }
    }
}

#[derive(Debug, Clone)]
struct Balances {
    available: Decimal,
    held: Decimal,
    total: Decimal,
    locked: bool,
}

impl Balances {
    fn new() -> Self {
        Self {
            available: Decimal::ZERO,
            held: Decimal::ZERO,
            total: Decimal::ZERO,
            locked: false,
        }
    }

    fn apply(&mut self, tx: TxPayload) {
        match tx.kind {
            TxType::Deposit => self.deposit(tx.amount),
            TxType::Withdrawal => self.withdrawal(tx.amount),
            TxType::Dispute => self.dispute(tx.amount),
            TxType::Resolve => self.resolve(tx.amount),
            TxType::Chargeback => self.chargeback(tx.amount),
        }
    }

    // TODO: Saturating or checked operations. Hmm, rather checked with proper error handling.
    fn deposit(&mut self, amount: Decimal) {
        self.available += amount;
        self.total += amount;
    }

    fn withdrawal(&mut self, amount: Decimal) {
        self.available -= amount;
        self.total -= amount;
    }

    fn dispute(&mut self, amount: Decimal) {
        self.held += amount;
        self.available -= amount;
    }

    fn resolve(&mut self, amount: Decimal) {
        self.held -= amount;
        self.available += amount;
    }

    fn chargeback(&mut self, amount: Decimal) {
        self.held -= amount;
        self.locked = true;
    }
}

struct ClientProcessor {
    client: u16,
    balances: Balances,
    tx_receiver: mpsc::Receiver<TxPayload>,
    result_sender: mpsc::Sender<Balances>,
}

impl ClientProcessor {
    fn new(
        client: u16,
        tx_receiver: mpsc::Receiver<TxPayload>,
        result_sender: mpsc::Sender<Balances>,
    ) -> Self {
        Self {
            client,
            tx_receiver,
            result_sender,
            balances: Balances::new(),
        }
    }

    async fn crank(&mut self, tx_counter: Arc<AtomicUsize>) {
        loop {
            match self.tx_receiver.recv().await {
                Some(tx) => {
                    println!("processing tx for client {}: {:?}", self.client, tx);
                    self.balances.apply(tx);
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
    }
}

struct StreamProcessor {
    client_processors: HashMap<u16, mpsc::Sender<TxPayload>>,
    result_receivers: HashMap<u16, mpsc::Receiver<Balances>>,
}

impl StreamProcessor {
    fn new() -> Self {
        Self {
            client_processors: HashMap::new(),
            result_receivers: HashMap::new(),
        }
    }

    async fn process(
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
                    sender.send(TxPayload { kind, tx, amount }).await?;
                }
                None => {
                    let (tx_sender, tx_receiver) = mpsc::channel(TX_CHANNEL_SIZE);
                    let (result_sender, result_receiver) = mpsc::channel(RESULTS_CHANNEL_SIZE);
                    // TODO: Consider worker pool? When there are millions of clients the current approach could be problematic.
                    let mut client_processor =
                        ClientProcessor::new(client, tx_receiver, result_sender);
                    self.client_processors.insert(client, tx_sender.clone());
                    self.result_receivers.insert(client, result_receiver);
                    tokio::spawn(async move {
                        client_processor.crank(Arc::clone(&active_tx_clone)).await;
                    });
                    //tokio::spawn(async move {client_processor.crank().await});
                    active_transactions.fetch_add(1, Ordering::SeqCst);
                    tx_sender.send(TxPayload { kind, tx, amount }).await?;
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

#[derive(Debug, Deserialize)]
struct Record {
    #[serde(rename = "type", deserialize_with = "TxType::from_deserializer")]
    kind: TxType,
    client: u16,
    tx: u32,
    amount: Decimal,
}

#[derive(Debug)]
struct TxPayload {
    kind: TxType,
    tx: u32,
    amount: Decimal,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let file = File::open("input.csv").await?.compat();
    // Does it also have "to_lowercase()"?
    let mut csv_reader = AsyncReaderBuilder::new()
        .has_headers(true)
        .trim(csv_async::Trim::All)
        .create_deserializer(file);

    let mut input_stream = csv_reader.deserialize::<Record>();

    let mut stream_processor = StreamProcessor::new();
    stream_processor.process(&mut input_stream).await?;

    Ok(())
}

// Overflow checks
// Incorrect, garbage input
// Maximum number of clients handled at the same time
// Do not use anyhow (except maybe returning from main?)
// Test with chained streams from multiple files
// Introduce rate limiting?
// Tracing?

// Share in a public repo?
