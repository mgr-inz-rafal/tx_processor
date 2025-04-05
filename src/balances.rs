use thiserror::Error;

use crate::traits::BalanceUpdater;

#[derive(Error, Debug)]
pub(super) enum Error {
    #[error("Arithmetic overflow when updating balances")]
    ArithmeticOverflow,
}

#[derive(Debug, Clone)]
pub(super) struct Balances<V>
where
    V: BalanceUpdater + Copy,
{
    available: V,
    held: V,
    total: V,
}

impl<V> Balances<V>
where
    V: BalanceUpdater + Copy,
{
    pub(super) fn new() -> Self {
        Self {
            available: V::new(),
            held: V::new(),
            total: V::new(),
        }
    }

    #[cfg(test)]
    fn new_with_values(available: V, held: V, total: V) -> Self {
        Self {
            available,
            held,
            total,
        }
    }

    // TODO: Saturating or checked operations. Hmm, rather checked with proper error handling.
    pub(super) fn deposit(&mut self, amount: V) -> Result<(), Error> {
        self.available = self
            .available
            .checked_add(amount)
            .ok_or(Error::ArithmeticOverflow)?;
        self.total = self
            .total
            .checked_add(amount)
            .ok_or(Error::ArithmeticOverflow)?;
        Ok(())
    }

    pub(super) fn withdrawal(&mut self, amount: V) -> Result<(), Error> {
        self.available = self
            .available
            .checked_sub(amount)
            .ok_or(Error::ArithmeticOverflow)?;
        self.total = self
            .total
            .checked_sub(amount)
            .ok_or(Error::ArithmeticOverflow)?;
        Ok(())
    }

    pub(super) fn dispute(&mut self, amount: V) -> Result<(), Error> {
        self.held = self
            .held
            .checked_add(amount)
            .ok_or(Error::ArithmeticOverflow)?;
        self.available = self
            .available
            .checked_sub(amount)
            .ok_or(Error::ArithmeticOverflow)?;
        Ok(())
    }

    pub(super) fn resolve(&mut self, amount: V) -> Result<(), Error> {
        self.held = self
            .held
            .checked_sub(amount)
            .ok_or(Error::ArithmeticOverflow)?;
        self.available = self
            .available
            .checked_add(amount)
            .ok_or(Error::ArithmeticOverflow)?;
        Ok(())
    }

    pub(super) fn chargeback(&mut self, amount: V) -> Result<(), Error> {
        self.held = self
            .held
            .checked_sub(amount)
            .ok_or(Error::ArithmeticOverflow)?;
        self.total = self
            .total
            .checked_sub(amount)
            .ok_or(Error::ArithmeticOverflow)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use crate::Balances;

    fn new_balance(available: Decimal, held: Decimal, total: Decimal) -> Balances<Decimal> {
        Balances::new_with_values(available, held, total)
    }

    mod update {
        use crate::balances::tests::new_balance;

        #[test]
        fn deposit() {
            let mut balance = new_balance(100.into(), 200.into(), 300.into());

            assert!(balance.deposit(1.into()).is_ok());

            assert_eq!(balance.available, 101.into());
            assert_eq!(balance.total, 301.into());
        }

        #[test]
        fn withdrawal() {
            let mut balance = new_balance(100.into(), 200.into(), 300.into());

            assert!(balance.withdrawal(1.into()).is_ok());

            assert_eq!(balance.available, 99.into());
            assert_eq!(balance.total, 299.into());
        }

        #[test]
        fn dispute() {
            let mut balance = new_balance(100.into(), 200.into(), 300.into());

            assert!(balance.dispute(1.into()).is_ok());

            assert_eq!(balance.held, 201.into());
            assert_eq!(balance.available, 99.into());
        }

        #[test]
        fn resolve() {
            let mut balance = new_balance(100.into(), 200.into(), 300.into());

            assert!(balance.resolve(1.into()).is_ok());

            assert_eq!(balance.held, 199.into());
            assert_eq!(balance.available, 101.into());
        }

        #[test]
        fn chargeback() {
            let mut balance = new_balance(100.into(), 200.into(), 300.into());

            assert!(balance.chargeback(1.into()).is_ok());

            assert_eq!(balance.held, 199.into());
            assert_eq!(balance.total, 299.into());
        }
    }

    mod overflow {
        use rust_decimal::Decimal;

        use crate::balances::{tests::new_balance, Error};

        #[test]
        fn deposit() {
            let mut balance = new_balance(Decimal::MAX, 100.into(), 100.into());
            assert!(matches!(
                balance.deposit(1.into()),
                Err(Error::ArithmeticOverflow)
            ));

            let mut balance = new_balance(100.into(), 100.into(), Decimal::MAX);
            assert!(matches!(
                balance.deposit(1.into()),
                Err(Error::ArithmeticOverflow)
            ));
        }

        #[test]
        fn withdrawal() {
            let mut balance = new_balance(Decimal::MIN, 100.into(), 100.into());
            assert!(matches!(
                balance.withdrawal(1.into()),
                Err(Error::ArithmeticOverflow)
            ));

            let mut balance = new_balance(100.into(), 100.into(), Decimal::MIN);
            assert!(matches!(
                balance.withdrawal(1.into()),
                Err(Error::ArithmeticOverflow)
            ));
        }

        #[test]
        fn dispute() {
            let mut balance = new_balance(100.into(), Decimal::MAX, 100.into());
            assert!(matches!(
                balance.dispute(1.into()),
                Err(Error::ArithmeticOverflow)
            ));

            let mut balance = new_balance(Decimal::MIN, 100.into(), 100.into());
            assert!(matches!(
                balance.dispute(1.into()),
                Err(Error::ArithmeticOverflow)
            ));
        }

        #[test]
        fn resolve() {
            let mut balance = new_balance(100.into(), Decimal::MIN, 100.into());
            assert!(matches!(
                balance.resolve(1.into()),
                Err(Error::ArithmeticOverflow)
            ));

            let mut balance = new_balance(Decimal::MAX, 100.into(), 100.into());
            assert!(matches!(
                balance.resolve(1.into()),
                Err(Error::ArithmeticOverflow)
            ));
        }

        #[test]
        fn chargeback() {
            let mut balance = new_balance(100.into(), Decimal::MIN, 100.into());
            assert!(matches!(
                balance.chargeback(1.into()),
                Err(Error::ArithmeticOverflow)
            ));

            let mut balance = new_balance(100.into(), 100.into(), Decimal::MIN);
            assert!(matches!(
                balance.chargeback(1.into()),
                Err(Error::ArithmeticOverflow)
            ));
        }
    }
}
