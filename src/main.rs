use std::{collections::HashMap, sync::Arc};

use csv_async::AsyncReaderBuilder;
use futures_util::{Stream, StreamExt};
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::{fs::File, sync::{mpsc, Notify}};
use tokio_util::compat::TokioAsyncReadCompatExt;

// TODO: Think about backpressure
const CHANNEL_SIZE: usize = 1000;

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
    shutdown_notify: Arc<Notify>,
}

impl ClientProcessor {
    fn new(client: u16, tx_receiver: mpsc::Receiver<TxPayload>, shutdown_notify: Arc<Notify>) -> Self {
        Self {
            client,
            tx_receiver,
            balances: Balances::new(),
            shutdown_notify,
        }
    }

    async fn crank(&mut self) {
        loop {
            tokio::select! {
                Some(tx) = self.tx_receiver.recv() => {
                    println!("Processing tx: {:?}", tx);
                    self.balances.apply(tx);
                },

                _ = self.shutdown_notify.notified() => {
                    println!("client {} shutting down", self.client);
                    break;
                },
            }
        }
    }
}

struct StreamProcessor {
    client_processors: HashMap<u16, mpsc::Sender<TxPayload>>,
}

impl StreamProcessor {
    fn new() -> Self {
        Self {
            client_processors: HashMap::new(),
        }
    }

    async fn process(
        &mut self,
        mut stream: impl Stream<Item = Result<Record, csv_async::Error>> + Unpin,
    ) -> anyhow::Result<()> {
        while let Some(record) = stream.next().await {
            let Record {
                kind,
                client,
                tx,
                amount,
            } = record?;

            let client_processor = self.client_processors.get(&client);
            match client_processor {
                Some(sender) => {
                    sender.send(TxPayload { kind, tx, amount }).await?;
                }
                None => {
                    let (sender, receiver) = mpsc::channel(CHANNEL_SIZE);
                    let shutdown_notify = Arc::new(Notify::new());
                    // TODO: Consider worker pool? When there are millions of clients the current approach could be problematic.
                    let mut client_processor = ClientProcessor::new(
                        client,
                        receiver,
                        Arc::clone(&shutdown_notify)
                    );
                    tokio::spawn(async move {client_processor.crank().await});
                    sender.send(TxPayload { kind, tx, amount }).await?;
                }
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

// Share in a public repo?
