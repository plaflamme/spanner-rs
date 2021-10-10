use std::future::Future;
use std::pin::Pin;

use bb8::{Pool, PooledConnection};
use tonic::Code;

use crate::result_set::ResultSet;
use crate::TimestampBound;
use crate::ToSpanner;
use crate::{session::SessionManager, ConfigBuilder, Connection, Error, TransactionSelector};

/// An asynchronous Cloud Spanner client.
pub struct Client {
    connection: Box<dyn Connection>,
    session_pool: Pool<SessionManager>,
}

impl Client {
    /// Returns a new [ConfigBuilder] which can be used to configure how to connect to a Cloud Spanner instance and database.
    pub fn configure() -> ConfigBuilder {
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

    /// Returns a [ReadContext] that can be used to read data out of Cloud Spanner.
    /// The returned context uses [TimestampBound::Strong] consistency for each individual read.
    pub fn read_only(&self) -> impl ReadContext {
        ReadOnly {
            connection: self.connection.clone(),
            bound: None,
            session_pool: self.session_pool.clone(),
        }
    }

    /// Returns a [ReadContext] that can be used to read data out of Cloud Spanner.
    /// The returned context uses the specified bounded consistency for each individual read.
    pub fn read_only_with_bound(&self, bound: TimestampBound) -> impl ReadContext {
        ReadOnly {
            connection: self.connection.clone(),
            bound: Some(bound),
            session_pool: self.session_pool.clone(),
        }
    }

    /// Returns a [TransactionContext] that can be used to both read and write data from/into Cloud Spanner.
    pub fn read_write(&mut self) -> TxRunner {
        TxRunner {
            connection: self.connection.clone(),
            session_pool: self.session_pool.clone(),
        }
    }
}

/// Defines the interface to read data out of Cloud Spanner.
#[async_trait::async_trait]
pub trait ReadContext {
    /// Execute a read-only SQL statement and returns a [ResultSet].
    ///
    /// # Parameters
    ///
    /// As per the [Cloud Spanner documentation](https://cloud.google.com/spanner/docs/sql-best-practices#query-parameters), the statement may contain named parameters, e.g.: `@param_name`.
    /// When such parameters are present in the SQL query, their value must be provided in the second argument to this function.
    ///
    /// See [ToSpanner] to determine how Rust values can be mapped to Cloud Spanner values.
    ///
    /// If the parameter values do not line up with parameters in the statement, an [Error] is returned.
    ///
    /// # Example
    ///
    ///  ```no_run
    /// # use spanner_rs::{Client, Error, ReadContext};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Error> {
    /// # let mut client = Client::configure().connect().await?;
    /// let my_id = 42;
    /// let rs = client.read_only().execute_sql(
    ///     "SELECT id FROM person WHERE id > @my_id",
    ///     &[("my_id", &my_id)],
    /// ).await?;
    /// for row in rs.iter() {
    ///     let id: u32 = row.get("id")?;
    ///     println!("id: {}", id);
    /// }
    /// # Ok(()) }
    ///  ```
    async fn execute_sql(
        &mut self,
        statement: &str,
        parameters: &[(&str, &(dyn ToSpanner + Sync))],
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
        parameters: &[(&str, &(dyn ToSpanner + Sync))],
    ) -> Result<ResultSet, Error> {
        let session = self.session_pool.get().await?;
        let result = self
            .connection
            .execute_sql(
                &session,
                &TransactionSelector::SingleUse(self.bound.clone()),
                statement,
                parameters,
                None,
            )
            .await?;

        Ok(result)
    }
}

/// Defines the interface to read from and write into Cloud Spanner.
///
/// This extends [ReadContext] to provide additional write functionalities.
#[async_trait::async_trait]
pub trait TransactionContext: ReadContext {
    /// Execute a DML SQL statement and returns the number of affected rows.
    ///
    /// # Parameters
    ///
    /// Like its [ReadContext::execute_sql] counterpart, this function also supports query parameters.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use spanner_rs::{Client, Error, TransactionContext};
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Error> {
    /// # let mut client = Client::configure().connect().await?;
    /// let id = 42;
    /// let name = "ferris";
    /// let rows = client
    ///     .read_write()
    ///     .run(|tx| {
    ///         Box::pin(async move {
    ///             tx.execute_update(
    ///                 "INSERT INTO person(id, name) VALUES (@id, @name)",
    ///                 &[("id", &id), ("name", &name)],
    ///             )
    ///             .await
    ///         })
    ///     })
    ///     .await?;
    ///
    /// println!("Inserted {} row", rows);
    /// # Ok(()) }
    /// ```
    async fn execute_update(
        &mut self,
        statement: &str,
        parameters: &[(&str, &(dyn ToSpanner + Sync))],
    ) -> Result<i64, Error>;
}

pub struct Tx<'a> {
    connection: Box<dyn Connection>,
    session: PooledConnection<'a, SessionManager>,
    selector: TransactionSelector,
    seqno: i64,
}

#[async_trait::async_trait]
impl<'a> ReadContext for Tx<'a> {
    async fn execute_sql(
        &mut self,
        statement: &str,
        parameters: &[(&str, &(dyn ToSpanner + Sync))],
    ) -> Result<ResultSet, Error> {
        // seqno is required on DML queries and ignored otherwise. Specifying it on every query is fine.
        self.seqno += 1;
        let result_set = self
            .connection
            .execute_sql(
                &self.session,
                &self.selector,
                statement,
                parameters,
                Some(self.seqno),
            )
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
        parameters: &[(&str, &(dyn ToSpanner + Sync))],
    ) -> Result<i64, Error> {
        self.execute_sql(statement, parameters).await?
            .stats
            .row_count
            .ok_or_else(|| Error::Client("no row count available. This may be the result of using execute_update on a statement that did not contain DML.".to_string()))
    }
}

/// Allows running read/write transactions against Cloud Spanner.
pub struct TxRunner {
    connection: Box<dyn Connection>,
    session_pool: Pool<SessionManager>,
}

impl TxRunner {
    /// Runs abitrary read / write operations against Cloud Spanner.
    ///
    /// This function encapsulates the read/write transaction management concerns, allowing the application to minimize boilerplate.
    ///
    /// # Begin
    ///
    /// The underlying transaction is only lazily created. If the provided closure does no work against Cloud Spanner,
    /// then no transaction is created.
    ///
    /// # Commit / Rollback
    ///
    /// The underlying transaction will be committed if the provided closure returns `Ok`.
    /// Conversely, any `Err` returned will initiate a rollback.
    ///
    /// If the commit or rollback operation returns an unexpected error, then this function will return that error.
    ///
    /// # Retries
    ///
    /// When committing, Cloud Spanner may reject the transaction due to conflicts with another transaction.
    /// In these situations, Cloud Spanner allows retrying the transaction which will have a higher priority and potentially successfully commit.
    ///
    /// **NOTE:** the consequence of retyring is that the provided closure may be invoked multiple times.
    /// It is important to avoid doing any additional side effects within this closure as they will also potentially occur more than once.
    ///
    /// # Example
    ///
    /// ```no_run
    /// # use spanner_rs::{Client, Error, ReadContext, TransactionContext};
    /// async fn bump_version(id: u32) -> Result<u32, Error> {
    /// # let mut client = Client::configure().connect().await?;
    ///     client
    ///         .read_write()
    ///         .run(|tx| {
    ///             Box::pin(async move {
    ///                 let rs = tx
    ///                     .execute_sql(
    ///                         "SELECT MAX(version) FROM versions WHERE id = @id",
    ///                         &[("id", &id)],
    ///                     )
    ///                     .await?;
    ///                 let latest_version: u32 = rs.iter().next().unwrap().get(0)?;
    ///                 let next_version = latest_version + 1;
    ///                 tx.execute_update(
    ///                     "INSERT INTO versions(id, version) VALUES(@id, @next_version)",
    ///                     &[("id", &id), ("next_version", &next_version)],
    ///                 )
    ///                 .await?;
    ///                 Ok(next_version)
    ///             })
    ///         })
    ///         .await
    /// }
    /// # #[tokio::main]
    /// # async fn main() -> Result<(), Error> {
    /// # bump_version(42).await?;
    /// # Ok(()) }
    /// ```
    pub async fn run<'b, O, F>(&'b mut self, mut work: F) -> Result<O, Error>
    where
        F: for<'a> FnMut(&'a mut Tx<'b>) -> Pin<Box<dyn Future<Output = Result<O, Error>> + 'a>>,
    {
        let session = self.session_pool.get().await?;
        let mut ctx = Tx {
            connection: self.connection.clone(),
            session,
            selector: TransactionSelector::Begin,
            seqno: 0,
        };

        loop {
            ctx.selector = TransactionSelector::Begin;
            ctx.seqno = 0;
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
