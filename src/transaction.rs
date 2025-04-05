use rust_decimal::Decimal;
use serde::Deserialize;

#[derive(Debug, Copy, Clone)]
pub(super) enum TxType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

impl TxType {
    pub(super) fn from_deserializer<'de, D>(deserializer: D) -> Result<Self, D::Error>
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
pub(super) struct TxPayload {
    kind: TxType,
    tx: u32,
    amount: Option<Decimal>,
}

impl TxPayload {
    pub(super) fn new(kind: TxType, tx: u32, amount: Option<Decimal>) -> Self {
        Self { kind, tx, amount }
    }

    pub(super) fn amount(&self) -> Option<Decimal> {
        self.amount
    }

    pub(super) fn tx(&self) -> u32 {
        self.tx
    }

    pub(super) fn kind(&self) -> TxType {
        self.kind
    }
}
