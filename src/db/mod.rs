//! The database module for the transaction processor.
//!
//! Database is needed to store the deposit values which are needed when dispute is created.

pub(super) mod in_mem;
mod traits;

pub(super) use traits::DepositValueCache;
