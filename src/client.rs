use std::convert::TryInto;

use crate::keys::KeySet;
use crate::proto::google::spanner::v1::{
    spanner_client::SpannerClient, CreateSessionRequest, ReadRequest, Session,
};
use crate::result_set::ResultSet;
use crate::{Config, DatabaseId, Error, SpannerResource};
use tonic::transport::Channel;
use tonic::Request;

pub struct Client {
    client: SpannerClient<Channel>,
    database: DatabaseId,
}

impl Client {
    pub fn config() -> Config {
        Config::default()
    }

    pub async fn connect(config: Config) -> Result<Self, Error> {
        let channel = Channel::from_shared(
            config
                .endpoint
                .ok_or_else(|| Error::Config("unspecified endpoint".to_string()))?,
        )
        .map_err(|invalid_uri| Error::Config(format!("invalid endpoint: {}", invalid_uri)))?
        .connect()
        .await?;

        Ok(Self {
            client: SpannerClient::new(channel),
            database: config.database.unwrap(),
        })
    }

    pub async fn read(
        &mut self,
        table: &str,
        key_set: KeySet,
        columns: Vec<String>,
    ) -> Result<ResultSet, Error> {
        let session = self.create_session().await?;
        let result_set = self
            .client
            .read(Request::new(ReadRequest {
                session: session.name,
                transaction: None,
                table: table.to_string(),
                index: "".to_string(),
                columns,
                key_set: Some(key_set.into()),
                limit: 0,
                resume_token: vec![],
                partition_token: vec![],
                request_options: None,
            }))
            .await?
            .into_inner();

        result_set.try_into()
    }

    pub async fn create_session(&mut self) -> Result<Session, Error> {
        let response = self
            .client
            .create_session(Request::new(CreateSessionRequest {
                database: self.database.id(),
                session: None,
            }))
            .await?;
        Ok(response.into_inner())
    }
}
