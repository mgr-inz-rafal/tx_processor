// Maybe not all txs type must be stored, definitely "Deposit" has to be here.
pub trait ValueCache<V> {
    fn get(&self, id: &u32) -> Option<&V>;
    fn insert(&mut self, id: u32, amount: V);
    fn remove(&mut self, id: u32) -> Option<V>;
}
