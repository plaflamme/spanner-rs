use std::time::Duration;
use std::time::SystemTime;

use crate::proto::google::spanner::v1 as proto;

#[derive(Clone, Debug)]
pub enum TimestampBound {
    Strong,
    ReadTimestamp(SystemTime),
    MinReadTimestamp(SystemTime),
    ExactStaleness(Duration),
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
    pub(crate) fn id(&self) -> &Vec<u8> {
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
