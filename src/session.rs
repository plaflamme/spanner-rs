use crate::proto::google::spanner::v1::transaction_options::Mode;
use crate::proto::google::spanner::v1::transaction_options::ReadOnly;
use crate::proto::google::spanner::v1::transaction_selector::Selector;
use crate::proto::google::spanner::v1::Session as SpannerSession;
use crate::proto::google::spanner::v1::TransactionOptions;
use crate::proto::google::spanner::v1::TransactionSelector as SpannerTransactionSelector;
pub struct Session(String);

impl Session {
    pub fn name(&self) -> &str {
        &self.0
    }
}

impl From<SpannerSession> for Session {
    fn from(value: SpannerSession) -> Self {
        Self(value.name)
    }
}

pub enum TransactionSelector {
    SingleUse,
}

impl From<TransactionSelector> for SpannerTransactionSelector {
    fn from(value: TransactionSelector) -> Self {
        match value {
            TransactionSelector::SingleUse => SpannerTransactionSelector {
                selector: Some(Selector::SingleUse(TransactionOptions {
                    mode: Some(Mode::ReadOnly(ReadOnly {
                        return_read_timestamp: false,
                        timestamp_bound: None,
                    })),
                })),
            },
        }
    }
}
