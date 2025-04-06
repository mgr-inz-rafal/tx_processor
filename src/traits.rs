pub(super) trait BalanceUpdater
where
    Self: Sized,
{
    fn new() -> Self;
    fn add(self, other: Self) -> Option<Self>;
    fn sub(self, other: Self) -> Option<Self>;
}
