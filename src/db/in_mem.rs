use std::collections::HashMap;

use crate::transaction::{Deposit, Transaction};

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
pub(crate) struct AmountCache<MonetaryValue> {
    txs: HashMap<u32, MonetaryValue>,
    pruning_strategy: Option<PruningStrategy>,
}

impl<MonetaryValue> AmountCache<MonetaryValue> {
    pub(crate) fn new() -> Self {
        Self {
            txs: HashMap::new(),
            pruning_strategy: None,
        }
    }
}

impl<MonetaryValue> DepositValueCache<MonetaryValue> for AmountCache<MonetaryValue>
where
    MonetaryValue: Copy,
{
    type Error = Error;

    fn get(&self, id: &u32) -> Option<&MonetaryValue> {
        self.txs.get(id)
    }

    fn insert(
        &mut self,
        id: u32,
        tx: Transaction<Deposit, MonetaryValue>,
    ) -> Result<(), Self::Error>
    where
        MonetaryValue: Copy,
    {
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
    fn remove(&mut self, id: u32) -> Option<MonetaryValue> {
        self.txs.remove(&id)
    }
}
