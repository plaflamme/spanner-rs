use bb8::{Pool, PooledConnection};

use crate::connection::GrpcConnection;
use crate::result_set::ResultSet;
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
            session: Some(session),
        })
    }
}

#[async_trait::async_trait]
pub trait ReadContext {
    async fn execute_sql(&mut self, statement: &str) -> Result<ResultSet, Error>;
}

struct SingleUse<'a> {
    connection: GrpcConnection,
    session: Option<PooledConnection<'a, SessionManager>>,
}

#[async_trait::async_trait]
impl<'a> ReadContext for SingleUse<'a> {
    async fn execute_sql(&mut self, statement: &str) -> Result<ResultSet, Error> {
        if let Some(session) = self.session.take() {
            let result = self
                .connection
                .execute_sql(&session, TransactionSelector::SingleUse, statement)
                .await?;

            Ok(result)
        } else {
            Err(Error::Client(
                "single_use can only be used for doing one read".to_string(),
            ))
        }
    }
}
