use serde::Deserialize;

pub struct Deposit;
pub struct Withdrawal;
pub struct Dispute;
pub struct Resolve;
pub struct Chargeback;

pub(super) enum Tx<MonetaryValue> {
    Deposit(Transaction<Deposit, MonetaryValue>),
    Withdrawal(Transaction<Withdrawal, MonetaryValue>),
    Dispute(Transaction<Dispute, MonetaryValue>),
    Resolve(Transaction<Resolve, MonetaryValue>),
    Chargeback(Transaction<Chargeback, MonetaryValue>),
}

impl<MonetaryValue> Tx<MonetaryValue> {
    pub(super) fn client(&self) -> u16 {
        match self {
            Self::Deposit(tx) => tx.client(),
            Self::Withdrawal(tx) => tx.client(),
            Self::Dispute(tx) => tx.client(),
            Self::Resolve(tx) => tx.client(),
            Self::Chargeback(tx) => tx.client(),
        }
    }
}

pub(super) struct Transaction<Kind, MonetaryValue> {
    client: u16,
    tx: u32,
    amount: Option<MonetaryValue>,
    phantom: std::marker::PhantomData<Kind>,
}

impl<Kind, MonetaryValue> Transaction<Kind, MonetaryValue> {
    pub(super) fn client(&self) -> u16 {
        self.client
    }

    pub(super) fn tx(&self) -> u32 {
        self.tx
    }
}

impl<MonetaryValue> Transaction<Deposit, MonetaryValue> {
    pub(super) fn new(client: u16, tx: u32, amount: MonetaryValue) -> Self {
        Self {
            tx,
            client,
            amount: Some(amount),
            phantom: std::marker::PhantomData,
        }
    }

    pub(super) fn amount(&self) -> &MonetaryValue {
        self.amount
            .as_ref()
            .expect("amount guaranteed to be present")
    }
}

impl<MonetaryValue> Transaction<Withdrawal, MonetaryValue> {
    pub(super) fn new(client: u16, tx: u32, amount: MonetaryValue) -> Self {
        Self {
            tx,
            client,
            amount: Some(amount),
            phantom: std::marker::PhantomData,
        }
    }

    pub(super) fn amount(&self) -> &MonetaryValue {
        self.amount
            .as_ref()
            .expect("amount guaranteed to be present")
    }
}

impl<MonetaryValue> Transaction<Dispute, MonetaryValue> {
    pub(super) fn new(client: u16, tx: u32) -> Self {
        Self {
            tx,
            client,
            amount: None,
            phantom: std::marker::PhantomData,
        }
    }
}

impl<MonetaryValue> Transaction<Resolve, MonetaryValue> {
    pub(super) fn new(client: u16, tx: u32) -> Self {
        Self {
            tx,
            client,
            amount: None,
            phantom: std::marker::PhantomData,
        }
    }
}

impl<MonetaryValue> Transaction<Chargeback, MonetaryValue> {
    pub(super) fn new(client: u16, tx: u32) -> Self {
        Self {
            tx,
            client,
            amount: None,
            phantom: std::marker::PhantomData,
        }
    }
}

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
