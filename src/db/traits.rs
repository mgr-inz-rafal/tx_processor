//! Traits for the database module.

use crate::transaction::{Deposit, TransactionPayload};

/// A trait for caching deposit values in the database. It won't work
/// with transactions other than deposit.
pub trait DepositValueCache<ValueType> {
    type Error;

    fn get(&self, id: &u32) -> Option<&ValueType>;

    fn insert(&mut self, id: u32, tx: TransactionPayload<Deposit>) -> Result<(), Self::Error>;

    #[allow(dead_code)]
    // To could be helpful when pruning is implemented.
    fn remove(&mut self, id: u32) -> Option<ValueType>;
}
