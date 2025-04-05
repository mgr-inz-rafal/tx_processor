use thiserror::Error;

use crate::traits::BalanceUpdater;

#[derive(Error, Debug)]
pub(super) enum Error {
    #[error("Arithmetic overflow when updating balances")]
    Overflow,
    #[error("Account is locked")]
    Locked,
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
        self.available = self.available.checked_add(amount).ok_or(Error::Overflow)?;
        self.total = self.total.checked_add(amount).ok_or(Error::Overflow)?;
        Ok(())
    }

    pub(super) fn withdrawal(&mut self, amount: V) -> Result<(), Error> {
        self.available = self.available.checked_sub(amount).ok_or(Error::Overflow)?;
        self.total = self.total.checked_sub(amount).ok_or(Error::Overflow)?;
        Ok(())
    }

    pub(super) fn dispute(&mut self, amount: V) -> Result<(), Error> {
        self.held = self.held.checked_add(amount).ok_or(Error::Overflow)?;
        self.available = self.available.checked_sub(amount).ok_or(Error::Overflow)?;
        Ok(())
    }

    pub(super) fn resolve(&mut self, amount: V) -> Result<(), Error> {
        self.held = self.held.checked_sub(amount).ok_or(Error::Overflow)?;
        self.available = self.available.checked_add(amount).ok_or(Error::Overflow)?;
        Ok(())
    }

    pub(super) fn chargeback(&mut self, amount: V) -> Result<(), Error> {
        self.held = self.held.checked_sub(amount).ok_or(Error::Overflow)?;
        self.total = self.total.checked_sub(amount).ok_or(Error::Overflow)?;
        Ok(())
    }
}

/*
#[cfg(test)]
mod tests {
    mod locking {
        use rust_decimal::Decimal;

        use crate::{balances::Error, Balances};

        fn new_locked_balance() -> Balances<Decimal> {
            Balances::new_with_values(0.into(), 0.into(), 0.into(), true)
        }

        #[test]
        fn deposit() {
            let mut balance = new_locked_balance();
            assert!(matches!(balance.deposit(1.into()), Err(Error::Locked)));
        }

        #[test]
        fn withdrawal() {
            let mut balance = new_locked_balance();
            assert!(matches!(balance.withdrawal(1.into()), Err(Error::Locked)));
        }

        #[test]
        fn dispute() {
            let mut balance = new_locked_balance();
            assert!(matches!(balance.dispute(1.into()), Err(Error::Locked)));
        }

        #[test]
        fn resolve() {
            let mut balance = new_locked_balance();
            assert!(matches!(balance.resolve(1.into()), Err(Error::Locked)));
        }

        #[test]
        fn chargeback() {
            let mut balance = new_locked_balance();
            assert!(matches!(balance.chargeback(1.into()), Err(Error::Locked)));
        }
    }
}
*/
