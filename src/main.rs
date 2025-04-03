use std::collections::HashMap;

use csv_async::AsyncReaderBuilder;
use futures_util::{Stream, StreamExt};
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::fs::File;
use tokio_util::compat::TokioAsyncReadCompatExt;

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

struct ClientProcessor {
    client: u16,
}

impl ClientProcessor {
    fn new(client: u16) -> Self {
        Self { client }
    }

    async fn process(
        &self,
        kind: TxType,
        tx: u32,
        amount: Decimal,
    ) -> anyhow::Result<()> {
        println!("{}: {:?}, tx: {}, amount: {}", self.client, kind, tx, amount);
        Ok(())
    }
}

struct StreamProcessor {
    client_processors: HashMap<u16, ClientProcessor>,
}

impl StreamProcessor {
    fn new() -> Self {
        Self { client_processors: HashMap::new() }
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
                Some(processor) => {
                    processor.process(kind, tx, amount).await?;
                }
                None => {
                    let new_processor = self.spawn_client_processor(client);
                    new_processor.process(kind, tx, amount).await?;
                    self.client_processors.insert(client, new_processor);
                }
            }
        }

        Ok(())
    }

    fn spawn_client_processor(&mut self, client_id: u16) -> ClientProcessor {
        let client_processor = ClientProcessor::new(client_id);
        client_processor
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

// Share in a public repo?
