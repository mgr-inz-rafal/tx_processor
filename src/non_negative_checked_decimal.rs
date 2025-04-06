use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::BalanceUpdater;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Eq, PartialEq)]
pub(super) struct NonNegativeCheckedDecimal(Decimal);

#[cfg(test)]
impl NonNegativeCheckedDecimal {
    pub(super) const MIN: Self = Self(Decimal::ZERO);
    pub(super) const MAX: Self = Self(Decimal::MAX);
}

impl BalanceUpdater for NonNegativeCheckedDecimal {
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

impl std::convert::From<u32> for NonNegativeCheckedDecimal {
    fn from(value: u32) -> Self {
        Self(Decimal::from(value))
    }
}

#[cfg(test)]
mod tests {
    use test_case::test_case;

    use crate::BalanceUpdater;

    use super::NonNegativeCheckedDecimal;

    #[test_case(10.into(), 5.into() => Some(15.into()))]
    #[test_case(0.into(), 0.into() => Some(0.into()))]
    #[test_case(NonNegativeCheckedDecimal::MAX, 0.into() => Some(NonNegativeCheckedDecimal::MAX))]
    #[test_case(NonNegativeCheckedDecimal::MAX, 1.into() => None)]
    fn addition(
        a: NonNegativeCheckedDecimal,
        b: NonNegativeCheckedDecimal,
    ) -> Option<NonNegativeCheckedDecimal> {
        a.add(b)
    }

    #[test_case(10.into(), 5.into() => Some(5.into()))]
    #[test_case(0.into(), 0.into() => Some(0.into()))]
    #[test_case(0.into(), 1.into() => None)]
    fn subtraction(
        a: NonNegativeCheckedDecimal,
        b: NonNegativeCheckedDecimal,
    ) -> Option<NonNegativeCheckedDecimal> {
        a.sub(b)
    }

    #[test]
    fn min() {
        assert_eq!(NonNegativeCheckedDecimal::MIN, 0.into());
    }
}
