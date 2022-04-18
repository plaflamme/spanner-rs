use std::time::Duration;
use std::time::SystemTime;

use google_api_proto::google::spanner::v1 as proto;

/// Specifies the bounds withing wich to make reads in Spanner.
///
/// See [the Spanner Documentation](https://cloud.google.com/spanner/docs/reference/rpc/google.spanner.v1#google.spanner.v1.TransactionOptions.ReadOnly)
#[derive(Clone, Debug)]
pub enum TimestampBound {
    /// Read at a timestamp where all previously committed transactions are visible.
    ///
    /// Strong reads are guaranteed to see the effects of all transactions that have committed before the start of the read.
    /// Furthermore, all rows yielded by a single read are consistent with each other -- if any part of the read observes a transaction, all parts of the read see the transaction.
    Strong,

    /// Executes all reads at the given timestamp.
    ///
    /// Unlike other modes, reads at a specific timestamp are repeatable; the same read at the same timestamp always returns the same data.
    /// If the timestamp is in the future, the read will block until the specified timestamp, modulo the read's deadline.
    ///
    /// Useful for large scale consistent reads such as mapreduces, or for coordinating many reads against a consistent snapshot of the data.
    ReadTimestamp(SystemTime),

    /// Executes all reads at a timestamp >= the provided timestamp.
    ///
    /// This is useful for requesting fresher data than some previous read,
    /// or data that is fresh enough to observe the effects of some previously committed transaction whose timestamp is known.
    MinReadTimestamp(SystemTime),

    /// Executes all reads at a timestamp that is `ExactStaleness` old. The timestamp is chosen soon after the read is started.
    ///
    /// Guarantees that all writes that have committed more than the specified number of seconds ago are visible.
    ExactStaleness(Duration),

    /// Read data at a timestamp `>= now() - MaxStaleness` seconds.
    ///
    /// Guarantees that all writes that have committed more than the specified number of seconds ago are visible.
    MaxStaleness(Duration),
}

impl From<TimestampBound> for proto::transaction_options::read_only::TimestampBound {
    fn from(value: TimestampBound) -> Self {
        match value {
            TimestampBound::Strong => {
                proto::transaction_options::read_only::TimestampBound::Strong(true)
            }
            TimestampBound::ReadTimestamp(timestamp) => {
                proto::transaction_options::read_only::TimestampBound::ReadTimestamp(
                    timestamp.into(),
                )
            }
            TimestampBound::MinReadTimestamp(timestamp) => {
                proto::transaction_options::read_only::TimestampBound::MinReadTimestamp(
                    timestamp.into(),
                )
            }
            TimestampBound::MaxStaleness(duration) => {
                proto::transaction_options::read_only::TimestampBound::MaxStaleness(duration.into())
            }
            TimestampBound::ExactStaleness(duration) => {
                proto::transaction_options::read_only::TimestampBound::ExactStaleness(
                    duration.into(),
                )
            }
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) enum TransactionSelector {
    SingleUse(Option<TimestampBound>),
    Id(Transaction),
    Begin,
}

impl From<TransactionSelector> for proto::TransactionSelector {
    fn from(value: TransactionSelector) -> Self {
        match value {
            TransactionSelector::SingleUse(bound) => proto::TransactionSelector {
                selector: Some(proto::transaction_selector::Selector::SingleUse(
                    proto::TransactionOptions {
                        mode: Some(proto::transaction_options::Mode::ReadOnly(
                            proto::transaction_options::ReadOnly {
                                return_read_timestamp: false,
                                timestamp_bound: bound.map(Into::into),
                            },
                        )),
                    },
                )),
            },
            TransactionSelector::Id(tx) => proto::TransactionSelector {
                selector: Some(proto::transaction_selector::Selector::Id(tx.spanner_tx.id)),
            },
            TransactionSelector::Begin => proto::TransactionSelector {
                selector: Some(proto::transaction_selector::Selector::Begin(
                    proto::TransactionOptions {
                        mode: Some(proto::transaction_options::Mode::ReadWrite(
                            proto::transaction_options::ReadWrite {},
                        )),
                    },
                )),
            },
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Transaction {
    spanner_tx: proto::Transaction,
}

impl Transaction {
    pub(crate) fn id(&self) -> &prost::bytes::Bytes {
        &self.spanner_tx.id
    }
}

impl From<proto::Transaction> for Transaction {
    fn from(spanner_tx: proto::Transaction) -> Self {
        Transaction { spanner_tx }
    }
}

impl From<Transaction> for proto::Transaction {
    fn from(tx: Transaction) -> Self {
        tx.spanner_tx
    }
}
