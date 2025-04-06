// Maybe not all txs type must be stored, definitely "Deposit" has to be here.
pub trait ValueCache<MonetaryValue> {
    type Error;

    fn get(&self, id: &u32) -> Option<&MonetaryValue>;
    fn insert(&mut self, id: u32, amount: MonetaryValue) -> Result<(), Self::Error>;
    fn remove(&mut self, id: u32) -> Option<MonetaryValue>;
}
