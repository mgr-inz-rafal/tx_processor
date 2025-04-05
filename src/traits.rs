use rust_decimal::Decimal;

pub(super) trait BalanceUpdater
where
    Self: Sized,
{
    fn new() -> Self;
    fn add(self, other: Self) -> Option<Self>;
    fn sub(self, other: Self) -> Option<Self>;
}

impl BalanceUpdater for Decimal {
    fn new() -> Self {
        Decimal::ZERO
    }

    fn add(self, other: Self) -> Option<Self> {
        self.checked_add(other)
    }

    fn sub(self, other: Self) -> Option<Self> {
        let new_value = self.checked_sub(other);
        new_value.and_then(|v| v.is_sign_positive().then_some(v))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_balance_updater() {
        let a = Decimal::new(10, 0);
        let b = Decimal::new(5, 0);

        assert_eq!(a.add(b), Some(Decimal::new(15, 0)));
        assert_eq!(a.sub(b), Some(Decimal::new(5, 0)));
        assert_eq!(b.sub(a), None);
    }
}
