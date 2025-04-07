//! A helper wrappers around `rust_decimal::Decimal` that ensures various properties
//! like non-zero or non-negative values.

use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::balances::BalanceUpdater;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Eq, PartialEq)]
pub(super) struct NonZero(Decimal);

impl TryFrom<Decimal> for NonZero {
    type Error = ();

    fn try_from(value: Decimal) -> Result<Self, Self::Error> {
        if value > Decimal::ZERO {
            Ok(Self(value))
        } else {
            Err(())
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Eq, PartialEq)]
pub(super) struct NonNegative(Decimal);

#[cfg(test)]
impl NonNegative {
    pub(super) const MIN: Self = Self(Decimal::ZERO);
    pub(super) const MAX: Self = Self(Decimal::MAX);
}

impl From<NonZero> for NonNegative {
    fn from(value: NonZero) -> Self {
        Self(value.0)
    }
}

impl From<&NonZero> for NonNegative {
    fn from(value: &NonZero) -> Self {
        Self(value.0)
    }
}

impl BalanceUpdater for NonNegative {
    fn new() -> Self {
        Self(Decimal::ZERO)
    }

    fn add(self, other: Self) -> Option<Self> {
        self.0.checked_add(other.0).map(Self)
    }

    fn sub(self, other: Self) -> Option<Self> {
        let new_value = self.0.checked_sub(other.0);
        new_value.and_then(|v| v.is_sign_positive().then_some(Self(v)))
    }
}

impl std::convert::From<u32> for NonNegative {
    fn from(value: u32) -> Self {
        Self(Decimal::from(value))
    }
}

#[cfg(test)]
mod tests {
    mod non_zero {
        use rust_decimal::Decimal;

        use crate::NonZero;

        #[test]
        fn can_not_be_zero() {
            let zero = Decimal::ZERO;
            let non_zero = NonZero::try_from(zero);
            assert!(non_zero.is_err());
        }

        #[test]
        fn can_not_be_negative() {
            let negative = Decimal::MIN;
            let non_zero = NonZero::try_from(negative);
            assert!(non_zero.is_err());
        }
    }

    mod non_negative {
        use test_case::test_case;

        use crate::{BalanceUpdater, NonNegative};

        #[test_case(10.into(), 5.into() => Some(15.into()))]
        #[test_case(0.into(), 0.into() => Some(0.into()))]
        #[test_case(NonNegative::MAX, 0.into() => Some(NonNegative::MAX))]
        #[test_case(NonNegative::MAX, 1.into() => None)]
        fn addition(a: NonNegative, b: NonNegative) -> Option<NonNegative> {
            a.add(b)
        }

        #[test_case(10.into(), 5.into() => Some(5.into()))]
        #[test_case(0.into(), 0.into() => Some(0.into()))]
        #[test_case(0.into(), 1.into() => None)]
        fn subtraction(a: NonNegative, b: NonNegative) -> Option<NonNegative> {
            a.sub(b)
        }

        #[test]
        fn min() {
            assert_eq!(NonNegative::MIN, 0.into());
        }
    }
}
