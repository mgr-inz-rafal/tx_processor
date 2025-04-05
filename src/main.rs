use std::env;

use balances::Balances;
use client_processor::{ClientProcessor, ClientState};
use csv_async::{AsyncReaderBuilder, AsyncSerializer};
use db::in_mem;
use futures_util::StreamExt;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use stream_processor::StreamProcessor;
use tokio::fs::File;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
use traits::BalanceUpdater;
use transaction::{TxPayload, TxType};

mod balances;
mod client_processor;
mod db;
mod error;
mod stream_processor;
#[cfg(test)]
mod tests;
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
            total: balances.available().add(balances.held()).expect("overflow"),
            locked: client_state.locked(),
        }
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // No need to add dedicated dependency (like 'clap') because we only have a single arg.
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <input_file>", args[0]);
        std::process::exit(1);
    }
    let filename = &args[1];

    let file = File::open(filename)
        .await?
        .compat();
    let mut csv_reader = AsyncReaderBuilder::new()
        .has_headers(true)
        .trim(csv_async::Trim::All)
        .create_deserializer(file);
    let mut input = csv_reader.deserialize::<InputRecord<Decimal>>();

    let mut stream_processor = StreamProcessor::new();
    let mut results = stream_processor.process(&mut input).await; // ?;

    let mut writer = AsyncSerializer::from_writer(tokio::io::stdout().compat_write());
    while let Some(client_state) = results.next().await {
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

// Tests:
// Test with garbage input
// 1. withdrawal - no existing client
// 2. withdrawal - not enough balance

// Assumptions:
// 1. The balance can be negative
// 2. Locked account can not process any transactions (including disputes, resolves, etc.)
// 3. There is a limited time window for the dispute to be raised
// 4. Enough resources to process all clients simultaneously
// 5. Only deposits can be disputed - stems from the fact how the dispute is described in the requirements
// 6. Tx ids are unique
// 7. Dispute can be resolved or charged back
// 8. User can dispute a transaction again after the dispute is resolved
// 9. Transactions that lead to incorrect state (arithmetic overflow, etc.) are silently ignored (not to accidentally pollute stdout). At the end of the day, these would deserve a proper handling.
// 10. Strings representing transaction names are case insensitive (e.g. "Deposit" and "deposit" are the same)

// Scenarios:
// 1. Multiple chargebacks, etc.
// 2. Locked account can not process any transactions
// 3. Differently formatted numbers work
