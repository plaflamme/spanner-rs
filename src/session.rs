use bb8::ManageConnection;
use bb8::PooledConnection;
use tokio::sync::Mutex;

use crate::connection::GrpcConnection;
use crate::proto::google::spanner::v1::transaction_options::Mode;
use crate::proto::google::spanner::v1::transaction_options::ReadOnly;
use crate::proto::google::spanner::v1::transaction_selector::Selector;
use crate::proto::google::spanner::v1::Session as SpannerSession;
use crate::proto::google::spanner::v1::TransactionOptions;
use crate::proto::google::spanner::v1::TransactionSelector as SpannerTransactionSelector;
use crate::Connection;
use crate::Error;
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

pub(crate) struct SessionManager {
    connection: Mutex<GrpcConnection>,
}

impl SessionManager {
    pub(crate) fn new(connection: GrpcConnection) -> Self {
        Self {
            connection: Mutex::new(connection),
        }
    }
}

#[async_trait::async_trait]
impl ManageConnection for SessionManager {
    type Connection = Session;
    type Error = Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        self.connection.lock().await.create_session().await
    }

    async fn is_valid(&self, _conn: &mut PooledConnection<'_, Self>) -> Result<(), Self::Error> {
        Ok(())
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        false
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
