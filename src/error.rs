// TODO: More granular errors for different kind of failures
pub(super) enum Error {
    InvalidTransaction { id: u32 },
}
