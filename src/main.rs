use std::env;

use balances::{BalanceUpdater, Balances};
use checked_decimal::{NonNegative, NonZero};
use client_processor::ClientProcessor;
use csv_async::{AsyncReaderBuilder, AsyncSerializer};
use db::in_mem;
use futures_util::StreamExt;
use rust_decimal::Decimal;
use stream_processor::StreamProcessor;
use tokio::fs::File;
use tokio_util::compat::{TokioAsyncReadCompatExt, TokioAsyncWriteCompatExt};

mod balances;
mod checked_decimal;
mod client_processor;
mod csv;
mod db;
mod error;
mod stream_processor;
#[cfg(test)]
mod tests;
mod transaction;

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
    let mut input = csv_reader.deserialize::<csv::InputRecord<Decimal>>();

    let mut stream_processor = StreamProcessor::new();
    let mut results = stream_processor.process(&mut input).await;

    let mut writer = AsyncSerializer::from_writer(tokio::io::stdout().compat_write());
    while let Some(client_state) = results.next().await {
        match client_state {
            Ok(client_state) => {
                let Ok(record): Result<csv::OutputRecord, _> = client_state.try_into() else {
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
