use std::future::Future;
use std::pin::Pin;

use bb8::{Pool, PooledConnection};
use tonic::Code;

use crate::result_set::ResultSet;
use crate::{session::SessionManager, ConfigBuilder, Connection, Error, TransactionSelector};
use crate::{TimestampBound, Value};

pub struct Client {
    connection: Box<dyn Connection>,
    session_pool: Pool<SessionManager>,
}

impl Client {
    pub fn config() -> ConfigBuilder {
        ConfigBuilder::default()
    }
}

impl Client {
    pub(crate) fn connect(
        connection: Box<dyn Connection>,
        session_pool: Pool<SessionManager>,
    ) -> Self {
        Self {
            connection,
            session_pool,
        }
    }

    pub fn read_only(&self) -> impl ReadContext {
        ReadOnly {
            connection: self.connection.clone(),
            bound: None,
            session_pool: self.session_pool.clone(),
        }
    }

    pub fn read_only_with_bound(&self, bound: TimestampBound) -> impl ReadContext {
        ReadOnly {
            connection: self.connection.clone(),
            bound: Some(bound),
            session_pool: self.session_pool.clone(),
        }
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
    async fn execute_sql(
        &mut self,
        statement: &str,
        parameters: Vec<(String, Value)>,
    ) -> Result<ResultSet, Error>;
}

struct ReadOnly {
    connection: Box<dyn Connection>,
    bound: Option<TimestampBound>,
    session_pool: Pool<SessionManager>,
}

#[async_trait::async_trait]
impl ReadContext for ReadOnly {
    async fn execute_sql(
        &mut self,
        statement: &str,
        parameters: Vec<(String, Value)>,
    ) -> Result<ResultSet, Error> {
        let session = self.session_pool.get().await?;
        let result = self
            .connection
            .execute_sql(
                &session,
                &TransactionSelector::SingleUse(self.bound.clone()),
                statement,
                parameters,
            )
            .await?;

        Ok(result)
    }
}

#[async_trait::async_trait]
pub trait TransactionContext: ReadContext {
    async fn execute_update(
        &mut self,
        statement: &str,
        parameters: Vec<(String, Value)>,
    ) -> Result<i64, Error>;
}

pub struct Tx<'a> {
    connection: Box<dyn Connection>,
    session: PooledConnection<'a, SessionManager>,
    selector: TransactionSelector,
}

#[async_trait::async_trait]
impl<'a> ReadContext for Tx<'a> {
    async fn execute_sql(
        &mut self,
        statement: &str,
        parameters: Vec<(String, Value)>,
    ) -> Result<ResultSet, Error> {
        let result_set = self
            .connection
            .execute_sql(&self.session, &self.selector, statement, parameters)
            .await?;

        // TODO: this is brittle, if we forget to do this in some other method, then we risk not committing.
        if let TransactionSelector::Begin = self.selector {
            if let Some(tx) = result_set.transaction.as_ref() {
                self.selector = TransactionSelector::Id(tx.clone());
            }
        }

        Ok(result_set)
    }
}

#[async_trait::async_trait]
impl<'a> TransactionContext for Tx<'a> {
    async fn execute_update(
        &mut self,
        statement: &str,
        parameters: Vec<(String, Value)>,
    ) -> Result<i64, Error> {
        let result_set = self.execute_sql(statement, parameters).await?;
        result_set
            .stats
            .row_count
            .ok_or_else(|| Error::Client("no row count available. This may be the result of using execute_update on a statement that did not contain DML.".to_string()))
    }
}

pub struct TxRunner {
    connection: Box<dyn Connection>,
    session_pool: Pool<SessionManager>,
}

impl TxRunner {
    pub async fn run<'b, O, F>(&'b mut self, mut work: F) -> Result<O, Error>
    where
        F: for<'a> FnMut(&'a mut Tx<'b>) -> Pin<Box<dyn Future<Output = Result<O, Error>> + 'a>>,
    {
        let session = self.session_pool.get().await?;
        let mut ctx = Tx {
            connection: self.connection.clone(),
            session,
            selector: TransactionSelector::Begin,
        };

        loop {
            ctx.selector = TransactionSelector::Begin;
            let result = (work)(&mut ctx).await;

            let commit_result = if let TransactionSelector::Id(tx) = ctx.selector {
                if result.is_ok() {
                    self.connection.commit(&ctx.session, tx).await
                } else {
                    self.connection.rollback(&ctx.session, tx).await
                }
            } else {
                Ok(())
            };

            match commit_result {
                Err(Error::Status(status)) if status.code() == Code::Aborted => continue,
                Err(err) => break Err(err),
                _ => break result,
            }
        }
    }
}
