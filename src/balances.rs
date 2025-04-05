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
    locked: bool,
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
            locked: false,
        }
    }

    // TODO: Only process when not locked.

    // TODO: Saturating or checked operations. Hmm, rather checked with proper error handling.
    pub(super) fn deposit(&mut self, amount: V) -> Result<(), Error> {
        if self.locked {
            return Err(Error::Locked);
        }
        self.available = self.available.checked_add(amount).ok_or(Error::Overflow)?;
        self.total = self.total.checked_add(amount).ok_or(Error::Overflow)?;
        Ok(())
    }

    pub(super) fn withdrawal(&mut self, amount: V) -> Result<(), Error> {
        if self.locked {
            return Err(Error::Locked);
        }
        self.available = self.available.checked_sub(amount).ok_or(Error::Overflow)?;
        self.total = self.total.checked_sub(amount).ok_or(Error::Overflow)?;
        Ok(())
    }

    pub(super) fn dispute(&mut self, amount: V) -> Result<(), Error> {
        if self.locked {
            return Err(Error::Locked);
        }
        self.held = self.held.checked_add(amount).ok_or(Error::Overflow)?;
        self.available = self.available.checked_sub(amount).ok_or(Error::Overflow)?;
        Ok(())
    }

    pub(super) fn resolve(&mut self, amount: V) -> Result<(), Error> {
        if self.locked {
            return Err(Error::Locked);
        }
        self.held = self.held.checked_sub(amount).ok_or(Error::Overflow)?;
        self.available = self.available.checked_add(amount).ok_or(Error::Overflow)?;
        Ok(())
    }

    pub(super) fn chargeback(&mut self, amount: V) -> Result<(), Error> {
        self.held = self.held.checked_sub(amount).ok_or(Error::Overflow)?;
        self.total = self.total.checked_sub(amount).ok_or(Error::Overflow)?;
        self.locked = true;
        Ok(())
    }
}
