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
pub(super) struct TxPayload<MonetaryValue>
where
    MonetaryValue: Copy,
{
    kind: TxType,
    tx: u32,
    amount: Option<MonetaryValue>,
}

impl<MonetaryValue> TxPayload<MonetaryValue>
where
    MonetaryValue: Copy,
{
    pub(super) fn new(kind: TxType, tx: u32, amount: Option<MonetaryValue>) -> Self {
        Self { kind, tx, amount }
    }

    pub(super) fn amount(&self) -> Option<MonetaryValue> {
        self.amount
    }

    pub(super) fn tx(&self) -> u32 {
        self.tx
    }

    pub(super) fn kind(&self) -> TxType {
        self.kind
    }
}
