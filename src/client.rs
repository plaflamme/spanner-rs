use std::future::Future;
use std::pin::Pin;

use bb8::{Pool, PooledConnection};

use crate::connection::GrpcConnection;
use crate::result_set::ResultSet;
use crate::TimestampBound;
use crate::{session::SessionManager, Config, Connection, Error, TransactionSelector};

pub struct Client {
    connection: GrpcConnection,
    session_pool: Pool<SessionManager>,
}

impl Client {
    pub fn config() -> Config {
        Config::default()
    }
}

impl Client {
    pub(crate) fn connect(connection: GrpcConnection, session_pool: Pool<SessionManager>) -> Self {
        Self {
            connection,
            session_pool,
        }
    }

    pub async fn single_use(&'_ mut self) -> Result<impl ReadContext + '_, Error> {
        let session = self.session_pool.get().await?;
        Ok(SingleUse {
            connection: self.connection.clone(),
            bound: None,
            session: Some(session),
        })
    }

    pub async fn single_use_with_bound(
        &'_ mut self,
        bound: TimestampBound,
    ) -> Result<impl ReadContext + '_, Error> {
        let session = self.session_pool.get().await?;
        Ok(SingleUse {
            connection: self.connection.clone(),
            bound: Some(bound),
            session: Some(session),
        })
    }

    pub fn read_write(&mut self) -> TxRunner {
        TxRunner {
            connection: self.connection.clone(),
            session_pool: self.session_pool.clone(),
        }
    }
}

#[async_trait::async_trait]
pub trait ReadContext {
    async fn execute_sql(&mut self, statement: &str) -> Result<ResultSet, Error>;
}

struct SingleUse<'a> {
    connection: GrpcConnection,
    bound: Option<TimestampBound>,
    session: Option<PooledConnection<'a, SessionManager>>,
}

#[async_trait::async_trait]
impl<'a> ReadContext for SingleUse<'a> {
    async fn execute_sql(&mut self, statement: &str) -> Result<ResultSet, Error> {
        if let Some(session) = self.session.take() {
            let result = self
                .connection
                .execute_sql(
                    &session,
                    &TransactionSelector::SingleUse(self.bound.clone()),
                    statement,
                )
                .await?;

            Ok(result)
        } else {
            Err(Error::Client(
                "single_use can only be used for doing one read".to_string(),
            ))
        }
    }
}
pub struct TransactionContext<'a> {
    connection: GrpcConnection,
    session: &'a PooledConnection<'a, SessionManager>,
    selector: TransactionSelector,
}

#[async_trait::async_trait]
impl<'a> ReadContext for TransactionContext<'a> {
    async fn execute_sql(&mut self, statement: &str) -> Result<ResultSet, Error> {
        let result_set = self
            .connection
            .execute_sql(&self.session, &self.selector, statement)
            .await?;

        if let TransactionSelector::Begin = self.selector {
            if let Some(tx) = result_set.transaction.as_ref() {
                self.selector = TransactionSelector::Id(tx.clone());
            }
        }

        Ok(result_set)
    }
}

pub struct TxRunner {
    connection: GrpcConnection,
    session_pool: Pool<SessionManager>,
}

impl TxRunner {
    pub async fn run<O, F>(&mut self, mut work: F) -> Result<O, Error>
    where
        F: for<'a> FnMut(
            &'a mut TransactionContext,
        ) -> Pin<Box<dyn Future<Output = Result<O, Error>> + 'a>>,
        F: Send,
    {
        let session = self.session_pool.get().await?;
        let mut ctx = TransactionContext {
            connection: self.connection.clone(),
            session: &session,
            selector: TransactionSelector::Begin,
        };
        let result = (work)(&mut ctx).await;
        if result.is_ok() {
            if let TransactionSelector::Id(tx) = ctx.selector {
                self.connection.commit(&session, tx).await?;
            }
        } else {
            todo!("rollback")
        }
        result
    }
}
