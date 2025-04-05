use rust_decimal::Decimal;

#[derive(Debug, Clone)]
pub(super) struct Balances {
    available: Decimal,
    held: Decimal,
    total: Decimal,
    locked: bool,
}

impl Balances {
    pub(super) fn new() -> Self {
        Self {
            available: Decimal::ZERO,
            held: Decimal::ZERO,
            total: Decimal::ZERO,
            locked: false,
        }
    }

    // TODO: Saturating or checked operations. Hmm, rather checked with proper error handling.
    pub(super) fn deposit(&mut self, amount: Decimal) {
        self.available += amount;
        self.total += amount;
    }

    pub(super) fn withdrawal(&mut self, amount: Decimal) {
        self.available -= amount;
        self.total -= amount;
    }

    pub(super) fn dispute(&mut self, amount: Decimal) {
        self.held += amount;
        self.available -= amount;
    }

    pub(super) fn resolve(&mut self, amount: Decimal) {
        self.held -= amount;
        self.available += amount;
    }

    pub(super) fn chargeback(&mut self, amount: Decimal) {
        self.held -= amount;
        self.total -= amount;
        self.locked = true;
    }
}
