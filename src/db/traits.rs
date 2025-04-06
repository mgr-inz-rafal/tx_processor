// Maybe not all txs type must be stored, definitely "Deposit" has to be here.
pub trait ValueCache<MonetaryValue> {
    fn get(&self, id: &u32) -> Option<&MonetaryValue>;
    fn insert(&mut self, id: u32, amount: MonetaryValue);
    fn remove(&mut self, id: u32) -> Option<MonetaryValue>;
}
