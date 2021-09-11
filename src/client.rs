use tonic::transport::Channel;
use tonic::Request;

use crate::proto::google::spanner::v1::{
    spanner_client::SpannerClient, CreateSessionRequest, Session,
};
use crate::{Config, DatabaseId, Error, SpannerResource};

pub struct Client {
    client: SpannerClient<Channel>,
}

impl Client {
    pub fn config() -> Config {
        Config::default()
    }

    pub async fn connect(config: &Config) -> Result<Self, Error> {
        let channel = Channel::from_shared(format!(
            "http://{}:{}",
            config.endpoint.as_ref().unwrap(),
            config.port.unwrap()
        ))
        .unwrap()
        .connect()
        .await?;
        Ok(Self {
            client: SpannerClient::new(channel),
        })
    }

    pub async fn create_session(&mut self, database_id: DatabaseId) -> Result<Session, Error> {
        let response = self
            .client
            .create_session(Request::new(CreateSessionRequest {
                database: database_id.id(),
                session: None,
            }))
            .await?;
        Ok(response.into_inner())
    }
}
