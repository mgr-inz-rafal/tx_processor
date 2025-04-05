use std::collections::HashMap;

use super::ValueCache;

#[derive(Debug, Clone)]
pub(crate) struct AmountCache<V> {
    txs: HashMap<u32, V>,
}

impl<V> AmountCache<V> {
    pub(crate) fn new() -> Self {
        Self {
            txs: HashMap::new(),
        }
    }
}

impl<V> ValueCache<V> for AmountCache<V> {
    fn get(&self, id: &u32) -> Option<&V> {
        self.txs.get(id)
    }

    // TODO: Duplicated ID should yield an error.
    fn insert(&mut self, id: u32, amount: V) {
        self.txs.insert(id, amount);
    }

    fn remove(&mut self, id: u32) -> Option<V> {
        self.txs.remove(&id)
    }
}
