//! Balances module helps to track and update all balances.
//!
//! It works with any value that implements the `BalanceUpdater` trait. It does not store
//! the `total` balance as it can always be derived from `held` and `available`.

use thiserror::Error;

use crate::NonNegative;

#[derive(Error, Debug)]
pub(super) enum Error {
    #[error("Arithmetic overflow when updating balances")]
    ArithmeticOverflow,
}

pub(super) trait BalanceUpdater
where
    Self: Sized,
{
    fn new() -> Self;
    fn add(self, other: Self) -> Option<Self>;
    fn sub(self, other: Self) -> Option<Self>;
}

#[derive(Debug, Clone)]
pub(super) struct Balances {
    available: NonNegative,
    held: NonNegative,
}

impl Balances {
    pub(super) fn new() -> Self {
        Self {
            available: NonNegative::new(),
            held: NonNegative::new(),
        }
    }

    #[cfg(test)]
    fn new_with_values(available: NonNegative, held: NonNegative) -> Self {
        Self { available, held }
    }

    fn transfer(
        from: NonNegative,
        to: NonNegative,
        amount: NonNegative,
    ) -> Option<(NonNegative, NonNegative)> {
        let new_from = from.sub(amount)?;
        let new_to = to.add(amount)?;
        Some((new_from, new_to))
    }

    pub(super) fn deposit(&mut self, amount: NonNegative) -> Result<(), Error> {
        self.available = self
            .available
            .add(amount)
            .ok_or(Error::ArithmeticOverflow)?;
        Ok(())
    }

    pub(super) fn withdrawal(&mut self, amount: NonNegative) -> Result<(), Error> {
        self.available = self
            .available
            .sub(amount)
            .ok_or(Error::ArithmeticOverflow)?;
        Ok(())
    }

    pub(super) fn dispute(&mut self, amount: NonNegative) -> Result<(), Error> {
        let (new_available, new_held) =
            Self::transfer(self.available, self.held, amount).ok_or(Error::ArithmeticOverflow)?;

        self.held = new_held;
        self.available = new_available;
        Ok(())
    }

    pub(super) fn resolve(&mut self, amount: NonNegative) -> Result<(), Error> {
        let (new_held, new_available) =
            Self::transfer(self.held, self.available, amount).ok_or(Error::ArithmeticOverflow)?;

        self.held = new_held;
        self.available = new_available;
        Ok(())
    }

    pub(super) fn chargeback(&mut self, amount: NonNegative) -> Result<(), Error> {
        self.held = self.held.sub(amount).ok_or(Error::ArithmeticOverflow)?;
        Ok(())
    }

    pub(super) fn available(&self) -> NonNegative {
        self.available
    }

    pub(super) fn held(&self) -> NonNegative {
        self.held
    }
}

#[cfg(test)]
mod tests {
    use crate::{Balances, NonNegative};

    fn new_balance(available: NonNegative, held: NonNegative) -> Balances {
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

    mod overflow_with_non_negative_type {
        use crate::{
            NonNegative,
            balances::{Error, tests::new_balance},
        };

        #[test]
        fn deposit() {
            let mut balance = new_balance(NonNegative::MAX, 100.into());
            assert!(matches!(
                balance.deposit(1.into()),
                Err(Error::ArithmeticOverflow)
            ));
        }

        #[test]
        fn withdrawal() {
            let mut balance = new_balance(NonNegative::MIN, 100.into());
            assert!(matches!(
                balance.withdrawal(1.into()),
                Err(Error::ArithmeticOverflow)
            ));
        }

        #[test]
        fn dispute() {
            let mut balance = new_balance(100.into(), NonNegative::MAX);
            assert!(matches!(
                balance.dispute(1.into()),
                Err(Error::ArithmeticOverflow)
            ));

            let mut balance = new_balance(NonNegative::MIN, 100.into());
            assert!(matches!(
                balance.dispute(1.into()),
                Err(Error::ArithmeticOverflow)
            ));
        }

        #[test]
        fn resolve() {
            let mut balance = new_balance(100.into(), NonNegative::MIN);
            assert!(matches!(
                balance.resolve(1.into()),
                Err(Error::ArithmeticOverflow)
            ));

            let mut balance = new_balance(NonNegative::MAX, 100.into());
            assert!(matches!(
                balance.resolve(1.into()),
                Err(Error::ArithmeticOverflow)
            ));
        }

        #[test]
        fn chargeback() {
            let mut balance = new_balance(100.into(), NonNegative::MIN);
            assert!(matches!(
                balance.chargeback(1.into()),
                Err(Error::ArithmeticOverflow)
            ));
        }
    }
}
