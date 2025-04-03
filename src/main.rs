use csv_async::AsyncReaderBuilder;
use rust_decimal::Decimal;
use serde::Deserialize;
use tokio::fs::File;
use tokio_util::compat::TokioAsyncReadCompatExt;
use futures_util::StreamExt;

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
        // TODO: Maybe we can avoid the allocation here. To be explored.
        let s = String::deserialize(deserializer)?.to_lowercase();
        match s.as_str() {
            "deposit" => Ok(Self::Deposit),
            "withdrawal" => Ok(Self::Withdrawal),
            "dispute" => Ok(Self::Dispute),
            "resolve" => Ok(Self::Resolve),
            "chargeback" => Ok(Self::Chargeback),
            _ => Err(serde::de::Error::custom(format!("Unknown transaction type: {}", s))),
        }
    }
}


#[derive(Debug, Deserialize)]
struct Record {
    #[serde(rename = "type", deserialize_with = "TxType::from_deserializer")]
    kind: TxType,
    client: u16,
    tx: u32,
    amount: Decimal
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let file = File::open("input.csv").await?.compat();
    let mut csv_reader = AsyncReaderBuilder::new()
        .has_headers(true)
        .trim(csv_async::Trim::All)
        .create_deserializer(file);

    let mut input_stream = csv_reader.deserialize::<Record>();

        while let Some(record) = input_stream.next().await {
            let tx = record?;
            dbg!(&tx);
        }    
    Ok(())
}


// Overflow checks
// Incorrect, garbage input
// Maximum number of clients handled at the same time