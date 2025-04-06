use thiserror::Error;

use crate::balances;

#[derive(Error, Debug)]
pub(super) enum Error {
    #[error("Invalid transaction: {id}")]
    InvalidTransaction { id: u32 },
    #[error("Duplicated transaction: {id}")]
    DuplicatedTransaction { id: u32 },
    #[error(transparent)]
    Balances(#[from] balances::Error),
}
