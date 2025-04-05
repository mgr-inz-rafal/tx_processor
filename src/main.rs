use balances::Balances;
use client_processor::ClientProcessor;
use csv_async::AsyncReaderBuilder;
use db::in_mem;
use rust_decimal::Decimal;
use serde::Deserialize;
use stream_processor::StreamProcessor;
use tokio::fs::File;
use tokio_util::compat::TokioAsyncReadCompatExt;
use transaction::{TxPayload, TxType};

mod balances;
mod client_processor;
mod db;
mod error;
mod stream_processor;
mod traits;
mod transaction;

#[derive(Debug, Deserialize)]
struct Record<V> {
    #[serde(rename = "type", deserialize_with = "TxType::from_deserializer")]
    kind: TxType,
    client: u16,
    tx: u32,
    amount: Option<V>,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let file = File::open("input.csv").await?.compat();
    // Does it also have "to_lowercase()"?
    let mut csv_reader = AsyncReaderBuilder::new()
        .has_headers(true)
        .trim(csv_async::Trim::All)
        .create_deserializer(file);

    let mut input_stream = csv_reader.deserialize::<Record<Decimal>>();

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
// Remove all prints and dbg! so the stdout is clean
// Move to lib, leave just main in main.rs

// Share in a public repo?

// Tests:
// Test with garbage input
// 1. withdrawal - no existing client
// 2. withdrawal - not enough balance
