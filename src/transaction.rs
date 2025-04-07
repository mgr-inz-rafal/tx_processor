//! A module consisting of types and functions to handle transactions.

use rust_decimal::Decimal;
use serde::Deserialize;

use crate::{NonZero, stream_processor::Error};

pub struct Deposit;
pub struct Withdrawal;
pub struct Dispute;
pub struct Resolve;
pub struct Chargeback;

// The main transaction type.
pub(super) enum Transaction {
    Deposit(TransactionPayload<Deposit>),
    Withdrawal(TransactionPayload<Withdrawal>),
    Dispute(TransactionPayload<Dispute>),
    Resolve(TransactionPayload<Resolve>),
    Chargeback(TransactionPayload<Chargeback>),
}

impl Transaction {
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

// Payload (metadata) of the transaction.
pub(super) struct TransactionPayload<Kind> {
    client: u16,
    tx: u32,
    // Option, since not all types of transactions have an amount.
    // The `Kind` type parameter ensures that this is correctly handled.
    amount: Option<NonZero>,
    phantom: std::marker::PhantomData<Kind>,
}

impl<Kind> TransactionPayload<Kind> {
    pub(super) fn client(&self) -> u16 {
        self.client
    }

    pub(super) fn tx(&self) -> u32 {
        self.tx
    }
}

impl TransactionPayload<Deposit> {
    pub(super) fn new(client: u16, tx: u32, amount: NonZero) -> Self {
        Self {
            tx,
            client,
            amount: Some(amount),
            phantom: std::marker::PhantomData,
        }
    }

    pub(super) fn amount(&self) -> &NonZero {
        self.amount
            .as_ref()
            .expect("amount guaranteed to be present")
    }
}

impl TransactionPayload<Withdrawal> {
    pub(super) fn new(client: u16, tx: u32, amount: NonZero) -> Self {
        Self {
            tx,
            client,
            amount: Some(amount),
            phantom: std::marker::PhantomData,
        }
    }

    pub(super) fn amount(&self) -> &NonZero {
        self.amount
            .as_ref()
            .expect("amount guaranteed to be present")
    }
}

impl TransactionPayload<Dispute> {
    pub(super) fn new(client: u16, tx: u32) -> Self {
        Self {
            tx,
            client,
            amount: None,
            phantom: std::marker::PhantomData,
        }
    }
}

impl TransactionPayload<Resolve> {
    pub(super) fn new(client: u16, tx: u32) -> Self {
        Self {
            tx,
            client,
            amount: None,
            phantom: std::marker::PhantomData,
        }
    }
}

impl TransactionPayload<Chargeback> {
    pub(super) fn new(client: u16, tx: u32) -> Self {
        Self {
            tx,
            client,
            amount: None,
            phantom: std::marker::PhantomData,
        }
    }
}

// Helper struct that deserializes the CSV input into the correct transaction type.
// It helps to avoid carrying around the `String` instance with every transaction.
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

// Transaction as created from the CSV input. This metadata is converted
// to a correct transaction before being processed.
// TODO: Reorg code and move this to a common place with `OutputClientData`
#[derive(Clone, Debug, Deserialize)]
pub(super) struct InputCsvTransaction<MonetaryValue> {
    #[serde(
        rename = "type",
        deserialize_with = "TransactionCsvType::from_deserializer"
    )]
    kind: TransactionCsvType,
    client: u16,
    tx: u32,
    amount: Option<MonetaryValue>,
}

impl TryFrom<InputCsvTransaction<Decimal>> for Transaction {
    type Error = Error;

    fn try_from(value: InputCsvTransaction<Decimal>) -> Result<Self, Self::Error> {
        match value.kind {
            crate::TransactionCsvType::Deposit => {
                let amount = value.amount.ok_or(Error::DepositMustHaveAmount)?;
                Ok(Transaction::Deposit(TransactionPayload::<Deposit>::new(
                    value.client,
                    value.tx,
                    amount
                        .try_into()
                        .map_err(|_| Error::DepositMustHaveNonZeroAmount)?,
                )))
            }
            crate::TransactionCsvType::Withdrawal => {
                let amount = value.amount.ok_or(Error::WithdrawalMustHaveAmount)?;
                Ok(Transaction::Withdrawal(
                    TransactionPayload::<Withdrawal>::new(
                        value.client,
                        value.tx,
                        amount
                            .try_into()
                            .map_err(|_| Error::WithdrawalMustHaveNonZeroAmount)?,
                    ),
                ))
            }
            crate::TransactionCsvType::Dispute => {
                Ok(Transaction::Dispute(TransactionPayload::<Dispute>::new(
                    value.client,
                    value.tx,
                )))
            }
            crate::TransactionCsvType::Resolve => {
                Ok(Transaction::Resolve(TransactionPayload::<Resolve>::new(
                    value.client,
                    value.tx,
                )))
            }
            crate::TransactionCsvType::Chargeback => Ok(Transaction::Chargeback(
                TransactionPayload::<Chargeback>::new(value.client, value.tx),
            )),
        }
    }
}
