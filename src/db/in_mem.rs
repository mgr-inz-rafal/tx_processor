//! In-memory database implementation for the `DepositValueCache` trait.
//!
//! Provides a simple in-memory cache for storing deposit values associated with transaction IDs.
//! It is not production ready since it has no overflow protection implemented.

use std::collections::HashMap;

use crate::{
    NonZero,
    transaction::{Deposit, TransactionPayload},
};

use super::DepositValueCache;

pub(crate) enum Error {
    AlreadyExists,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub(super) enum PruningStrategy {
    Ttl { duration: std::time::Duration },
    Size { max_size: usize },
}

#[derive(Debug, Clone)]
pub(crate) struct AmountCache {
    txs: HashMap<u32, NonZero>,
    pruning_strategy: Option<PruningStrategy>,
}

impl AmountCache {
    pub(crate) fn new() -> Self {
        Self {
            txs: HashMap::new(),
            pruning_strategy: None,
        }
    }
}

impl DepositValueCache<NonZero> for AmountCache {
    type Error = Error;

    fn get(&self, id: &u32) -> Option<&NonZero> {
        self.txs.get(id)
    }

    fn insert(&mut self, id: u32, tx: TransactionPayload<Deposit>) -> Result<(), Self::Error> {
        if let Some(strategy) = &self.pruning_strategy {
            match strategy {
                PruningStrategy::Size { max_size: _ } | PruningStrategy::Ttl { duration: _ } => {
                    // TODO: Implement proper pruning strategy
                }
            }
        }

        let amount = tx.amount();
        match self.txs.insert(id, *amount) {
            Some(_) => Err(Error::AlreadyExists),
            None => Ok(()),
        }
    }

    #[allow(dead_code)]
    // To could be helpful when pruning is implemented.
    fn remove(&mut self, id: u32) -> Option<NonZero> {
        self.txs.remove(&id)
    }
}
