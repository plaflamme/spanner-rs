use crate::{Client, Error};

#[derive(Default, PartialEq, Clone)]
pub struct Config {
    pub(crate) endpoint: Option<String>,
    pub(crate) port: Option<u16>,
}

impl Config {
    pub fn endpoint(mut self, endpoint: &str) -> Self {
        self.endpoint = Some(endpoint.to_string());
        self
    }

    pub fn port(mut self, port: u16) -> Self {
        self.port = Some(port);
        self
    }

    pub async fn connect(&self) -> Result<Client, Error> {
        Client::connect(self).await
    }
}
