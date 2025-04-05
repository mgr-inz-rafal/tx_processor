use balances::Balances;
use client_processor::ClientProcessor;
use csv_async::{AsyncReaderBuilder, AsyncSerializer};
use db::in_mem;
use futures_util::StreamExt;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use stream_processor::{ClientState, StreamProcessor};
use tokio::fs::File;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
use traits::BalanceUpdater;
use transaction::{TxPayload, TxType};

mod balances;
mod client_processor;
mod db;
mod error;
mod stream_processor;
mod traits;
mod transaction;

#[derive(Debug, Deserialize)]
struct InputRecord<V> {
    #[serde(rename = "type", deserialize_with = "TxType::from_deserializer")]
    kind: TxType,
    client: u16,
    tx: u32,
    amount: Option<V>,
}

#[derive(Debug, Serialize)]
struct OutputRecord<V> {
    client: u16,
    available: V,
    held: V,
    total: V,
    locked: bool,
}

impl<V> From<ClientState<V>> for OutputRecord<V>
where
    V: BalanceUpdater + Copy,
{
    fn from(client_state: ClientState<V>) -> Self {
        let balances = client_state.balances();
        Self {
            client: client_state.client(),
            available: balances.available(),
            held: balances.held(),
            total: balances.total(),
            locked: false,
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let file = File::open("input.csv").await?.compat();
    // Does it also have "to_lowercase()"?
    let mut csv_reader = AsyncReaderBuilder::new()
        .has_headers(true)
        .trim(csv_async::Trim::All)
        .create_deserializer(file);

    let mut input_stream = csv_reader.deserialize::<InputRecord<Decimal>>();

    let mut stream_processor = StreamProcessor::new();
    let mut result_stream = stream_processor.process(&mut input_stream).await; // ?;

    let output = tokio::io::stdout().compat_write();
    let mut writer = AsyncSerializer::from_writer(output);
    while let Some(client_state) = result_stream.next().await {
        let record: OutputRecord<Decimal> = client_state.into();
        writer.serialize(&record).await?;
    }
    writer.flush().await?;

    Ok(())
}

// Overflow checks
// Incorrect, garbage input
// Maximum number of clients handled at the same time
// Do not use anyhow (except maybe returning from main?)
// Test with chained streams from multiple files
// Introduce rate limiting?
// Tracing?
// Remove all prints and dbg! so the stdout is clean
// Move to lib, leave just main in main.rs

// Share in a public repo?

// Tests:
// Test with garbage input
// 1. withdrawal - no existing client
// 2. withdrawal - not enough balance

// Assumptions:
// 1. The balance can be negative
// 2. Locked account can not process any transactions
// 3. There is a limited time window for the dispute to be raised
// 4. Enough resources to process all clients simultaneously
