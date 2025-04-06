use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use futures_util::{Stream, StreamExt, stream};
use thiserror::Error;
use tokio::sync::{mpsc, oneshot};

use crate::{
    BalanceUpdater, ClientProcessor, InputRecord,
    client_processor::ClientState,
    in_mem,
    transaction::{Chargeback, Deposit, Dispute, Resolve, Transaction, Tx, Withdrawal},
};

// TODO: This could potentially be a config option to adjust the backpressure
// for a specific scenario.
const TX_CHANNEL_SIZE: usize = 1000;

pub(super) type ClientResult<MonetaryValue> =
    Result<ClientState<MonetaryValue>, Error<MonetaryValue>>;

#[derive(Debug, Error)]
pub(super) enum Error<MonetaryValue>
where
    MonetaryValue: Copy,
{
    #[error(transparent)]
    Csv(#[from] csv_async::Error),
    #[error(transparent)]
    Tokio(#[from] tokio::sync::mpsc::error::SendError<Tx<MonetaryValue>>),
    #[error("could not receive results for client {client}: {reason}")]
    CouldNotReceiveResults { client: u16, reason: String },
    #[error("deposit must have an amount")]
    DepositMustHaveAmount,
    #[error("withdrawal must have an amount")]
    WithdrawalMustHaveAmount,
}

// The `Decimal` type, while being convenient for financial calculations,
// consists of 4 u32 values. This is why `StreamProcessor` abstracts over it
// so we can build a smaller type based on a single u64 and then
// easily use it with the `StreamProcessor`.
pub(super) struct StreamProcessor<MonetaryValue>
where
    MonetaryValue: BalanceUpdater + Copy + Send,
{
    // Each client is handled by a separate processor.
    // TODO: When there are millions of clients the current approach could be
    // problematic as we spawn a new task for each client. Potential solutions
    // to explore if this proves to be a problem:
    // - Use a batched processing, where there is a limited number of
    //   processors and each processor handles multiple clients.
    // - Use LRU cache - keep only N processors alive and reuse them.
    //   Persist a state of the processor when it is not used and
    //   restore when it is needed again.
    client_processors: HashMap<u16, mpsc::Sender<Tx<MonetaryValue>>>,

    result_receivers: HashMap<u16, oneshot::Receiver<ClientState<MonetaryValue>>>,
}

fn try_tx_from_record<MonetaryValue>(
    record: &InputRecord<MonetaryValue>,
) -> Result<Tx<MonetaryValue>, Error<MonetaryValue>>
where
    MonetaryValue: Copy,
{
    match record.kind {
        crate::TxType::Deposit => {
            let amount = record.amount.ok_or(Error::DepositMustHaveAmount)?;
            Ok(Tx::Deposit(Transaction::<Deposit, MonetaryValue>::new(
                record.client,
                record.tx,
                amount,
            )))
        }
        crate::TxType::Withdrawal => {
            let amount = record.amount.ok_or(Error::WithdrawalMustHaveAmount)?;
            Ok(Tx::Withdrawal(
                Transaction::<Withdrawal, MonetaryValue>::new(record.client, record.tx, amount),
            ))
        }
        crate::TxType::Dispute => Ok(Tx::Dispute(Transaction::<Dispute, MonetaryValue>::new(
            record.client,
            record.tx,
        ))),
        crate::TxType::Resolve => Ok(Tx::Resolve(Transaction::<Resolve, MonetaryValue>::new(
            record.client,
            record.tx,
        ))),
        crate::TxType::Chargeback => Ok(Tx::Chargeback(
            Transaction::<Chargeback, MonetaryValue>::new(record.client, record.tx),
        )),
    }
}

impl<MonetaryValue> StreamProcessor<MonetaryValue>
where
    MonetaryValue: BalanceUpdater + Copy + Send + 'static,
{
    pub(super) fn new() -> Self {
        Self {
            client_processors: HashMap::new(),
            result_receivers: HashMap::new(),
        }
    }

    pub(super) async fn process<S>(
        &mut self,
        mut stream: S,
    ) -> impl Stream<Item = ClientResult<MonetaryValue>>
    where
        S: Stream<Item = Result<InputRecord<MonetaryValue>, csv_async::Error>> + Unpin,
    {
        // Use usize so that we can basically ignore potential overflows. If there
        // are more than usize::MAX transactions in flight, we have bigger problems
        // already.
        let active_transactions = Arc::new(AtomicUsize::new(0));

        while let Some(record) = stream.next().await {
            let Ok(record) = record else {
                //tracing::error!("csv record error");
                continue;
            };
            let Ok(tx) = try_tx_from_record(&record) else {
                //tracing::error!("invalid transaction in csv");
                continue;
            };

            let active_transactions = Arc::clone(&active_transactions);

            let client_processor = self.client_processors.get(&tx.client());
            match client_processor {
                Some(tx_sender) => {
                    send_and_register(tx, Arc::clone(&active_transactions), tx_sender).await;
                }
                None => {
                    let (tx_sender, tx_receiver) = mpsc::channel(TX_CHANNEL_SIZE);
                    let (result_sender, result_receiver) = oneshot::channel();
                    let client_db = in_mem::AmountCache::new();
                    let mut client_processor =
                        ClientProcessor::new(tx.client(), client_db, tx_receiver, result_sender);
                    self.client_processors
                        .insert(tx.client(), tx_sender.clone());
                    self.result_receivers.insert(tx.client(), result_receiver);
                    tokio::spawn({
                        let active_transactions = Arc::clone(&active_transactions);
                        async move {
                            if let Err(_err) = client_processor
                                .crank(Arc::clone(&active_transactions))
                                .await
                            {
                                //tracing::error!(%_err);
                            }
                        }
                    });
                    send_and_register(tx, Arc::clone(&active_transactions), &tx_sender).await;
                }
            }
        }

        // TODO: Busy waiting at the end to make sure all txs are processed.
        // This could potentially be improved with a more elegant solution.
        while active_transactions.load(Ordering::SeqCst) > 0 {
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        }

        // We only drop senders after all transactions are processed.
        self.client_processors = HashMap::new();

        // Read all results from the receivers.
        stream::iter(self.result_receivers.iter_mut())
            .then(|(client, receiver)| async move {
                receiver.await.map_err(|err| Error::CouldNotReceiveResults {
                    client: *client,
                    reason: err.to_string(),
                })
            })
            .boxed()
    }
}

async fn send_and_register<MonetaryValue>(
    tx: Tx<MonetaryValue>,
    active_tx_clone: Arc<AtomicUsize>,
    sender: &mpsc::Sender<Tx<MonetaryValue>>,
) where
    MonetaryValue: BalanceUpdater + Copy + Send + 'static,
{
    active_tx_clone.fetch_add(1, Ordering::SeqCst);
    if let Err(_err) = sender.send(tx).await {
        //tracing::error!(%_err);
    };
}
