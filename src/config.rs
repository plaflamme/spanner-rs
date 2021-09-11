use crate::{Client, DatabaseId, Error};

#[derive(Default, PartialEq, Clone)]
pub struct Config {
    pub(crate) endpoint: Option<String>,
    pub(crate) port: Option<u16>,
    pub(crate) database: Option<DatabaseId>,
}

impl Config {
    pub fn database(mut self, database: DatabaseId) -> Self {
        self.database = Some(database);
        self
    }

    pub fn endpoint(mut self, endpoint: &str) -> Self {
        self.endpoint = Some(endpoint.to_string());
        self
    }

    pub fn port(mut self, port: u16) -> Self {
        self.port = Some(port);
        self
    }

    pub async fn connect(self) -> Result<Client, Error> {
        Client::connect(self).await
    }
}
