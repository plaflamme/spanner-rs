use crate::{connection::GrpcConnection, Client, DatabaseId, Error};

#[derive(Default, PartialEq, Clone)]
pub struct Config {
    pub(crate) endpoint: Option<String>,
    pub(crate) database: Option<DatabaseId>,
}

impl Config {
    pub fn database(mut self, database: DatabaseId) -> Self {
        self.database = Some(database);
        self
    }

    pub fn endpoint(mut self, endpoint: String) -> Self {
        self.endpoint = Some(endpoint);
        self
    }

    pub async fn connect(self) -> Result<Client, Error> {
        let connection = GrpcConnection::connect(self).await?;
        Ok(Client::connect(connection))
    }
}
