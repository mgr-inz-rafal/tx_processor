use thiserror::Error;

use crate::traits::BalanceUpdater;

#[derive(Error, Debug)]
pub(super) enum Error {
    #[error("Arithmetic overflow when updating balances")]
    ArithmeticOverflow,
}

#[derive(Debug, Clone)]
pub(super) struct Balances<MonetaryValue>
where
    MonetaryValue: BalanceUpdater + Copy,
{
    available: MonetaryValue,
    held: MonetaryValue,
}

impl<MonetaryValue> Balances<MonetaryValue>
where
    MonetaryValue: BalanceUpdater + Copy,
{
    pub(super) fn new() -> Self {
        Self {
            available: MonetaryValue::new(),
            held: MonetaryValue::new(),
        }
    }

    #[cfg(test)]
    fn new_with_values(available: MonetaryValue, held: MonetaryValue) -> Self {
        Self { available, held }
    }

    fn transfer(
        from: MonetaryValue,
        to: MonetaryValue,
        amount: MonetaryValue,
    ) -> Option<(MonetaryValue, MonetaryValue)> {
        let new_from = from.sub(amount)?;
        let new_to = to.add(amount)?;
        Some((new_from, new_to))
    }

    pub(super) fn deposit(&mut self, amount: MonetaryValue) -> Result<(), Error> {
        self.available = self
            .available
            .add(amount)
            .ok_or(Error::ArithmeticOverflow)?;
        Ok(())
    }

    pub(super) fn withdrawal(&mut self, amount: MonetaryValue) -> Result<(), Error> {
        self.available = self
            .available
            .sub(amount)
            .ok_or(Error::ArithmeticOverflow)?;
        Ok(())
    }

    pub(super) fn dispute(&mut self, amount: MonetaryValue) -> Result<(), Error> {
        let (new_available, new_held) =
            Self::transfer(self.available, self.held, amount).ok_or(Error::ArithmeticOverflow)?;

        self.held = new_held;
        self.available = new_available;
        Ok(())
    }

    pub(super) fn resolve(&mut self, amount: MonetaryValue) -> Result<(), Error> {
        let (new_held, new_available) =
            Self::transfer(self.held, self.available, amount).ok_or(Error::ArithmeticOverflow)?;

        self.held = new_held;
        self.available = new_available;
        Ok(())
    }

    pub(super) fn chargeback(&mut self, amount: MonetaryValue) -> Result<(), Error> {
        self.held = self.held.sub(amount).ok_or(Error::ArithmeticOverflow)?;
        Ok(())
    }

    pub(super) fn available(&self) -> MonetaryValue {
        self.available
    }

    pub(super) fn held(&self) -> MonetaryValue {
        self.held
    }
}

#[cfg(test)]
mod tests {
    use rust_decimal::Decimal;

    use crate::Balances;

    fn new_balance(available: Decimal, held: Decimal) -> Balances<Decimal> {
        Balances::new_with_values(available, held)
    }

    mod update {
        use crate::balances::tests::new_balance;

        #[test]
        fn deposit() {
            let mut balance = new_balance(100.into(), 200.into());

            assert!(balance.deposit(1.into()).is_ok());

            assert_eq!(balance.available, 101.into());
        }

        #[test]
        fn withdrawal() {
            let mut balance = new_balance(100.into(), 200.into());

            assert!(balance.withdrawal(1.into()).is_ok());

            assert_eq!(balance.available, 99.into());
        }

        #[test]
        fn dispute() {
            let mut balance = new_balance(100.into(), 200.into());

            assert!(balance.dispute(1.into()).is_ok());

            assert_eq!(balance.held, 201.into());
            assert_eq!(balance.available, 99.into());
        }

        #[test]
        fn resolve() {
            let mut balance = new_balance(100.into(), 200.into());

            assert!(balance.resolve(1.into()).is_ok());

            assert_eq!(balance.held, 199.into());
            assert_eq!(balance.available, 101.into());
        }

        #[test]
        fn chargeback() {
            let mut balance = new_balance(100.into(), 200.into());

            assert!(balance.chargeback(1.into()).is_ok());

            assert_eq!(balance.held, 199.into());
        }
    }

    mod overflow {
        use rust_decimal::Decimal;

        use crate::balances::{Error, tests::new_balance};

        #[test]
        fn deposit() {
            let mut balance = new_balance(Decimal::MAX, 100.into());
            assert!(matches!(
                balance.deposit(1.into()),
                Err(Error::ArithmeticOverflow)
            ));
        }

        #[test]
        fn withdrawal() {
            let mut balance = new_balance(Decimal::MIN, 100.into());
            assert!(matches!(
                balance.withdrawal(1.into()),
                Err(Error::ArithmeticOverflow)
            ));
        }

        #[test]
        fn dispute() {
            let mut balance = new_balance(100.into(), Decimal::MAX);
            assert!(matches!(
                balance.dispute(1.into()),
                Err(Error::ArithmeticOverflow)
            ));

            let mut balance = new_balance(Decimal::MIN, 100.into());
            assert!(matches!(
                balance.dispute(1.into()),
                Err(Error::ArithmeticOverflow)
            ));
        }

        #[test]
        fn resolve() {
            let mut balance = new_balance(100.into(), Decimal::MIN);
            assert!(matches!(
                balance.resolve(1.into()),
                Err(Error::ArithmeticOverflow)
            ));

            let mut balance = new_balance(Decimal::MAX, 100.into());
            assert!(matches!(
                balance.resolve(1.into()),
                Err(Error::ArithmeticOverflow)
            ));
        }

        #[test]
        fn chargeback() {
            let mut balance = new_balance(100.into(), Decimal::MIN);
            assert!(matches!(
                balance.chargeback(1.into()),
                Err(Error::ArithmeticOverflow)
            ));
        }
    }
}
