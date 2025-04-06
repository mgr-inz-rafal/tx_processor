use std::collections::HashMap;

use super::ValueCache;

pub(crate) enum Error {
    AlreadyExists,
}

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
    type Error = Error;

    fn get(&self, id: &u32) -> Option<&MonetaryValue> {
        self.txs.get(id)
    }

    fn insert(&mut self, id: u32, amount: MonetaryValue) -> Result<(), Self::Error> {
        match self.txs.insert(id, amount) {
            Some(_) => Err(Error::AlreadyExists),
            None => Ok(()),
        }
    }

    fn remove(&mut self, id: u32) -> Option<MonetaryValue> {
        self.txs.remove(&id)
    }
}
