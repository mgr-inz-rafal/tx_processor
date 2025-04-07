use serde::{Deserialize, Serialize};

use crate::{stream_processor::Error, BalanceUpdater, ClientState};

pub struct Deposit;
pub struct Withdrawal;
pub struct Dispute;
pub struct Resolve;
pub struct Chargeback;

pub(super) enum Transaction<MonetaryValue> {
    Deposit(TransactionPayload<Deposit, MonetaryValue>),
    Withdrawal(TransactionPayload<Withdrawal, MonetaryValue>),
    Dispute(TransactionPayload<Dispute, MonetaryValue>),
    Resolve(TransactionPayload<Resolve, MonetaryValue>),
    Chargeback(TransactionPayload<Chargeback, MonetaryValue>),
}

impl<MonetaryValue> Transaction<MonetaryValue> {
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

pub(super) struct TransactionPayload<Kind, MonetaryValue> {
    client: u16,
    tx: u32,
    amount: Option<MonetaryValue>,
    phantom: std::marker::PhantomData<Kind>,
}

impl<Kind, MonetaryValue> TransactionPayload<Kind, MonetaryValue> {
    pub(super) fn client(&self) -> u16 {
        self.client
    }

    pub(super) fn tx(&self) -> u32 {
        self.tx
    }
}

impl<MonetaryValue> TransactionPayload<Deposit, MonetaryValue> {
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

impl<MonetaryValue> TransactionPayload<Withdrawal, MonetaryValue> {
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

impl<MonetaryValue> TransactionPayload<Dispute, MonetaryValue> {
    pub(super) fn new(client: u16, tx: u32) -> Self {
        Self {
            tx,
            client,
            amount: None,
            phantom: std::marker::PhantomData,
        }
    }
}

impl<MonetaryValue> TransactionPayload<Resolve, MonetaryValue> {
    pub(super) fn new(client: u16, tx: u32) -> Self {
        Self {
            tx,
            client,
            amount: None,
            phantom: std::marker::PhantomData,
        }
    }
}

impl<MonetaryValue> TransactionPayload<Chargeback, MonetaryValue> {
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
pub(super) enum TransactionCsvType {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

impl TransactionCsvType {
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

#[derive(Clone, Debug, Deserialize)]
pub(super) struct InputCsvTransaction<MonetaryValue> {
    #[serde(rename = "type", deserialize_with = "TransactionCsvType::from_deserializer")]
    kind: TransactionCsvType,
    client: u16,
    tx: u32,
    amount: Option<MonetaryValue>,
}

#[derive(Debug, Serialize)]
pub(super) struct OutputCsvTransaction<MonetaryValue> {
    client: u16,
    available: MonetaryValue,
    held: MonetaryValue,
    total: MonetaryValue,
    locked: bool,
}

impl<MonetaryValue> TryFrom<ClientState<MonetaryValue>> for OutputCsvTransaction<MonetaryValue>
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

impl<MonetaryValue> TryFrom<InputCsvTransaction<MonetaryValue>> for Transaction<MonetaryValue>
where
    MonetaryValue: Copy,
{
    type Error = Error<MonetaryValue>;

    fn try_from(value: InputCsvTransaction<MonetaryValue>) -> Result<Self, Self::Error> {
        match value.kind {
            crate::TransactionCsvType::Deposit => {
                let amount = value.amount.ok_or(Error::DepositMustHaveAmount)?;
                Ok(Transaction::Deposit(TransactionPayload::<
                    Deposit,
                    MonetaryValue,
                >::new(
                    value.client, value.tx, amount
                )))
            }
            crate::TransactionCsvType::Withdrawal => {
                let amount = value.amount.ok_or(Error::WithdrawalMustHaveAmount)?;
                Ok(Transaction::Withdrawal(TransactionPayload::<
                    Withdrawal,
                    MonetaryValue,
                >::new(
                    value.client, value.tx, amount
                )))
            }
            crate::TransactionCsvType::Dispute => Ok(Transaction::Dispute(TransactionPayload::<
                Dispute,
                MonetaryValue,
            >::new(
                value.client,
                value.tx,
            ))),
            crate::TransactionCsvType::Resolve => Ok(Transaction::Resolve(TransactionPayload::<
                Resolve,
                MonetaryValue,
            >::new(
                value.client,
                value.tx,
            ))),
            crate::TransactionCsvType::Chargeback => {
                Ok(Transaction::Chargeback(TransactionPayload::<
                    Chargeback,
                    MonetaryValue,
                >::new(
                    value.client, value.tx
                )))
            }
        }
    }
}
