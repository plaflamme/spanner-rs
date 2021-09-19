use crate::connection::GrpcConnection;
use crate::result_set::ResultSet;
use crate::{Config, Connection, Error, Session, TransactionSelector};

pub struct Client {
    connection: GrpcConnection,
}

impl Client {
    pub fn config() -> Config {
        Config::default()
    }
}

impl Client {
    pub(crate) fn connect(connection: GrpcConnection) -> Self {
        Self { connection }
    }

    pub async fn single_use(&mut self) -> Result<impl ReadContext, Error> {
        let session = self.connection.create_session().await?;
        Ok(SingleUse {
            connection: self.connection.clone(),
            session,
        })
    }
}

#[async_trait::async_trait]
pub trait ReadContext {
    async fn execute_sql(&mut self, statement: &str) -> Result<ResultSet, Error>;
}

struct SingleUse {
    connection: GrpcConnection,
    session: Session,
}

#[async_trait::async_trait]
impl ReadContext for SingleUse {
    async fn execute_sql(&mut self, statement: &str) -> Result<ResultSet, Error> {
        let result = self
            .connection
            .execute_sql(&self.session, TransactionSelector::SingleUse, statement)
            .await?;
        self.connection.delete_session(&self.session).await?; // TODO: we should do something like self.session.take()
        Ok(result)
    }
}
