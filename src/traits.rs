use rust_decimal::Decimal;

pub(super) trait BalanceUpdater
where
    Self: Sized,
{
    fn new() -> Self;
    fn checked_add(self, other: Self) -> Option<Self>;
    fn checked_sub(self, other: Self) -> Option<Self>;
}

impl BalanceUpdater for Decimal {
    fn new() -> Self {
        Decimal::ZERO
    }

    fn checked_add(self, other: Self) -> Option<Self> {
        self.checked_add(other)
    }

    fn checked_sub(self, other: Self) -> Option<Self> {
        self.checked_sub(other)
    }
}
