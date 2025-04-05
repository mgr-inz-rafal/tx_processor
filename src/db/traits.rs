use rust_decimal::Decimal;

// Maybe not all txs type must be stored, definitely "Deposit" has to be here.
pub trait ValueCache {
    fn get(&self, id: &u32) -> Option<&Decimal>;
    fn insert(&mut self, id: u32, amount: Decimal);
    fn remove(&mut self, id: u32) -> Option<Decimal>;
}
