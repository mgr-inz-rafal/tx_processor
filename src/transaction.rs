//! A module consisting of types and functions to handle transactions.

use crate::NonZero;

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

// Payload (data) of the transaction.
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
