use crate::transaction::{Deposit, Transaction};

pub trait ValueCache<MonetaryValue>
where
    MonetaryValue: Copy,
{
    type Error;

    fn get(&self, id: &u32) -> Option<&MonetaryValue>;
    fn insert(
        &mut self,
        id: u32,
        tx: Transaction<Deposit, MonetaryValue>,
    ) -> Result<(), Self::Error>;

    #[allow(dead_code)]
    // To could be helpful when pruning is implemented.
    fn remove(&mut self, id: u32) -> Option<MonetaryValue>;
}
