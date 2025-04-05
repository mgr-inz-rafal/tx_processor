use std::collections::HashMap;

use rust_decimal::Decimal;

use super::ValueCache;

#[derive(Debug, Clone)]
pub(crate) struct AmountCache {
    txs: HashMap<u32, Decimal>,
}

impl AmountCache {
    pub(crate) fn new() -> Self {
        Self {
            txs: HashMap::new(),
        }
    }
}

impl ValueCache for AmountCache {
    fn get(&self, id: &u32) -> Option<&Decimal> {
        self.txs.get(id)
    }

    // TODO: Duplicated ID should yield an error.
    fn insert(&mut self, id: u32, amount: Decimal) {
        self.txs.insert(id, amount);
    }

    fn remove(&mut self, id: u32) -> Option<Decimal> {
        self.txs.remove(&id)
    }
}
