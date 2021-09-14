use crate::keys::KeySet;
use crate::result_set::ResultSet;
use crate::{Config, Connection, Error};

pub struct Client {
    connection: Box<dyn Connection>,
}

impl Client {
    pub fn connect(connection: impl Connection + 'static) -> Self {
        Self {
            connection: Box::new(connection),
        }
    }

    pub fn config() -> Config {
        Config::default()
    }

    pub async fn read(
        &mut self,
        table: &str,
        key_set: KeySet,
        columns: Vec<String>,
    ) -> Result<ResultSet, Error> {
        self.connection.read(table, key_set, columns).await
    }

    pub async fn execute_sql(&mut self, statement: &str) -> Result<ResultSet, Error> {
        self.connection.execute_sql(statement).await
    }
}
