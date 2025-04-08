use serde::{Deserialize, Serialize};

use crate::{
    client_processor::ClientState,
    transaction::{
        Chargeback, Deposit, Dispute, Resolve, Transaction, TransactionPayload, Withdrawal,
    },
    BalanceUpdater, NonNegative, NonZero,
};

#[derive(Debug, thiserror::Error)]
pub(super) enum Error {
    #[error("deposit must have an amount")]
    DepositMustHaveAmount,
    #[error("deposit must have a non-zero amount")]
    DepositMustHaveNonZeroAmount,
    #[error("withdrawal must have an amount")]
    WithdrawalMustHaveAmount,
    #[error("withdrawal must have a non-zero amount")]
    WithdrawalMustHaveNonZeroAmount,
}

// Transaction as created from the CSV input. This metadata is converted
// to a correct transaction before being processed.
#[derive(Clone, Debug, Deserialize)]
pub(super) struct InputRecord<MonetaryValue> {
    #[serde(rename = "type", deserialize_with = "Kind::from_deserializer")]
    kind: Kind,
    client: u16,
    tx: u32,
    amount: Option<MonetaryValue>,
}

impl<MonetaryValue> TryFrom<InputRecord<MonetaryValue>> for Transaction
where
    MonetaryValue: TryInto<NonZero>,
{
    type Error = Error;

    fn try_from(value: InputRecord<MonetaryValue>) -> Result<Self, Self::Error> {
        match value.kind {
            Kind::Deposit => {
                let amount = value.amount.ok_or(Error::DepositMustHaveAmount)?;
                Ok(Transaction::Deposit(TransactionPayload::<Deposit>::new(
                    value.client,
                    value.tx,
                    amount
                        .try_into()
                        .map_err(|_| Error::DepositMustHaveNonZeroAmount)?,
                )))
            }
            Kind::Withdrawal => {
                let amount = value.amount.ok_or(Error::WithdrawalMustHaveAmount)?;
                Ok(Transaction::Withdrawal(
                    TransactionPayload::<Withdrawal>::new(
                        value.client,
                        value.tx,
                        amount
                            .try_into()
                            .map_err(|_| Error::WithdrawalMustHaveNonZeroAmount)?,
                    ),
                ))
            }
            Kind::Dispute => Ok(Transaction::Dispute(TransactionPayload::<Dispute>::new(
                value.client,
                value.tx,
            ))),
            Kind::Resolve => Ok(Transaction::Resolve(TransactionPayload::<Resolve>::new(
                value.client,
                value.tx,
            ))),
            Kind::Chargeback => Ok(Transaction::Chargeback(
                TransactionPayload::<Chargeback>::new(value.client, value.tx),
            )),
        }
    }
}

// This struct is used to serialize the results of processing.
#[derive(Debug, Serialize)]
pub(super) struct OutputRecord {
    client: u16,
    available: NonNegative,
    held: NonNegative,
    total: NonNegative,
    locked: bool,
}

impl TryFrom<ClientState> for OutputRecord {
    type Error = anyhow::Error;

    fn try_from(client_state: ClientState) -> Result<Self, Self::Error> {
        let balances = client_state.balances();
        let total = balances.available().add(balances.held());
        let Some(total) = total else {
            return Err(anyhow::anyhow!("total balance overflow"));
        };
        Ok(Self {
            client: client_state.client(),
            available: balances.available(),
            held: balances.held(),
            total,
            locked: client_state.locked(),
        })
    }
}

// Helper struct that deserializes the CSV input into the correct transaction type.
// It helps to avoid carrying around the `String` instance with every transaction.
#[derive(Debug, Copy, Clone)]
pub(super) enum Kind {
    Deposit,
    Withdrawal,
    Dispute,
    Resolve,
    Chargeback,
}

impl Kind {
    pub(super) fn from_deserializer<'de, D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        // TODO: Maybe we can avoid the String allocation here. Serde internals could give us &str?
        // Idea to be explored.
        let s = String::deserialize(deserializer)?.to_lowercase();
        match s.as_str() {
            "deposit" => Ok(Self::Deposit),
            "withdrawal" => Ok(Self::Withdrawal),
            "dispute" => Ok(Self::Dispute),
            "resolve" => Ok(Self::Resolve),
            "chargeback" => Ok(Self::Chargeback),
            _ => Err(serde::de::Error::custom(format!(
                "Unknown transaction type: {}",
                s
            ))),
        }
    }
}
