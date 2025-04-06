use std::env;

use balances::{BalanceUpdater, Balances};
use client_processor::{ClientProcessor, ClientState};
use csv_async::{AsyncReaderBuilder, AsyncSerializer};
use db::in_mem;
use futures_util::StreamExt;
use non_negative_checked_decimal::NonNegativeCheckedDecimal;
use serde::{Deserialize, Serialize};
use stream_processor::StreamProcessor;
use tokio::fs::File;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};
use transaction::TxType;

mod balances;
mod client_processor;
mod db;
mod error;
mod non_negative_checked_decimal;
mod stream_processor;
#[cfg(test)]
mod tests;
mod transaction;

#[derive(Clone, Debug, Deserialize)]
struct InputRecord<MonetaryValue> {
    #[serde(rename = "type", deserialize_with = "TxType::from_deserializer")]
    kind: TxType,
    client: u16,
    tx: u32,
    amount: Option<MonetaryValue>,
}

#[derive(Debug, Serialize)]
struct OutputRecord<MonetaryValue> {
    client: u16,
    available: MonetaryValue,
    held: MonetaryValue,
    total: MonetaryValue,
    locked: bool,
}

impl<MonetaryValue> TryFrom<ClientState<MonetaryValue>> for OutputRecord<MonetaryValue>
where
    MonetaryValue: BalanceUpdater + Copy,
{
    type Error = anyhow::Error;

    fn try_from(client_state: ClientState<MonetaryValue>) -> Result<Self, Self::Error> {
        let balances = client_state.balances();
        let total = balances.available().add(balances.held());
        let Some(total) = total else {
            return Err(anyhow::anyhow!("total balance overflow"));
        };
        Ok(Self {
            client: client_state.client(),
            available: balances.available(),
            held: balances.held(),
            total,
            locked: client_state.locked(),
        })
    }
}

// `anyhow` only used in the main module for easier integration between
// operating system and ?-based error handling.
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // No need to add dedicated dependency (like 'clap') because we only have a single arg.
    let args: Vec<String> = env::args().collect();
    if args.len() < 2 {
        eprintln!("Usage: {} <input_file>", args[0]);
        std::process::exit(1);
    }
    let filename = &args[1];

    let file = File::open(filename).await?.compat();
    let mut csv_reader = AsyncReaderBuilder::new()
        .has_headers(true)
        .trim(csv_async::Trim::All)
        .create_deserializer(file);
    let mut input = csv_reader.deserialize::<InputRecord<NonNegativeCheckedDecimal>>();

    let mut stream_processor = StreamProcessor::new();
    let mut results = stream_processor.process(&mut input).await; // ?;

    let mut writer = AsyncSerializer::from_writer(tokio::io::stdout().compat_write());
    while let Some(client_state) = results.next().await {
        match client_state {
            Ok(client_state) => {
                let Ok(record): Result<OutputRecord<NonNegativeCheckedDecimal>, _> =
                    client_state.try_into()
                else {
                    //tracing::error!(%_err);
                    continue;
                };
                writer.serialize(&record).await?;
            }
            Err(_err) => {
                //tracing::error!(%_err);
            }
        }
    }
    writer.flush().await?;

    Ok(())
}

// Overflow checks
// Test with chained streams from multiple files
// Introduce rate limiting?
// Pin toolchain

// Tests:
// Test with garbage input
// 1. withdrawal - no existing client

// Additional assumptions:
// - The balance can not be negative
// - Locked account can not process any transactions (including disputes, resolves, etc.)
// - There is a limited time window for the dispute to be raised
// - Only deposits can be disputed - stems from the fact how the dispute is described in the requirements
// - User can dispute a transaction again after the dispute is resolved
// - Transactions that lead to incorrect state (arithmetic overflow, etc.) are silently ignored (not to accidentally
//   pollute stdout). At the end of the day, these would deserve a proper handling.
//   Commented `tracing` placeholders are left in the code.
// - Strings representing transaction names are case insensitive (e.g. "Deposit" and "deposit" are the same)
