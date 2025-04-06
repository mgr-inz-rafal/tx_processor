use std::collections::HashMap;

use super::ValueCache;

#[derive(Debug, Clone)]
pub(crate) struct AmountCache<MonetaryValue> {
    txs: HashMap<u32, MonetaryValue>,
}

impl<MonetaryValue> AmountCache<MonetaryValue> {
    pub(crate) fn new() -> Self {
        Self {
            txs: HashMap::new(),
        }
    }
}

impl<MonetaryValue> ValueCache<MonetaryValue> for AmountCache<MonetaryValue> {
    fn get(&self, id: &u32) -> Option<&MonetaryValue> {
        self.txs.get(id)
    }

    // TODO: Duplicated ID should yield an error.
    fn insert(&mut self, id: u32, amount: MonetaryValue) {
        self.txs.insert(id, amount);
    }

    fn remove(&mut self, id: u32) -> Option<MonetaryValue> {
        self.txs.remove(&id)
    }
}
