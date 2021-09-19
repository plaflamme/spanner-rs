use std::collections::HashMap;
use std::convert::TryInto;

use crate::proto::google::spanner::v1::execute_sql_request::QueryMode;
use crate::proto::google::spanner::v1::{
    spanner_client::SpannerClient, CreateSessionRequest, DeleteSessionRequest, ExecuteSqlRequest,
    ReadRequest,
};
use crate::{
    Config, DatabaseId, Error, KeySet, ResultSet, Session, SpannerResource, TransactionSelector,
};
use async_trait::async_trait;
use tonic::transport::Channel;
use tonic::Request;

#[async_trait]
pub trait Connection: Clone {
    async fn create_session(&mut self) -> Result<Session, Error>;
    async fn delete_session(&mut self, session: Session) -> Result<(), Error>;
    async fn read(
        &mut self,
        table: &str,
        key_set: KeySet,
        columns: Vec<String>,
    ) -> Result<ResultSet, Error>;

    async fn execute_sql(
        &mut self,
        session: &Session,
        selector: TransactionSelector,
        statement: &str,
    ) -> Result<ResultSet, Error>;
}

#[derive(Clone)]
pub(crate) struct GrpcConnection {
    database: DatabaseId,
    spanner: SpannerClient<Channel>,
}

impl GrpcConnection {
    pub(crate) async fn connect(config: Config) -> Result<Self, Error> {
        let channel = Channel::from_shared(
            config
                .endpoint
                .ok_or_else(|| Error::Config("unspecified endpoint".to_string()))?,
        )
        .map_err(|invalid_uri| Error::Config(format!("invalid endpoint: {}", invalid_uri)))?
        .connect()
        .await?;

        Ok(Self {
            database: config.database.unwrap(),
            spanner: SpannerClient::new(channel),
        })
    }
}

#[async_trait]
impl Connection for GrpcConnection {
    async fn create_session(&mut self) -> Result<Session, Error> {
        let response = self
            .spanner
            .create_session(Request::new(CreateSessionRequest {
                database: self.database.id(),
                session: None,
            }))
            .await?;
        Ok(response.into_inner().into())
    }
    async fn delete_session(&mut self, session: Session) -> Result<(), Error> {
        self.spanner
            .delete_session(Request::new(DeleteSessionRequest {
                name: session.name().to_string(),
            }))
            .await?;
        Ok(())
    }
    async fn read(
        &mut self,
        table: &str,
        key_set: KeySet,
        columns: Vec<String>,
    ) -> Result<ResultSet, Error> {
        let session = self.create_session().await?;
        let result_set = self
            .spanner
            .read(Request::new(ReadRequest {
                session: session.name().to_string(),
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

    async fn execute_sql(
        &mut self,
        session: &Session,
        selector: TransactionSelector,
        statement: &str,
    ) -> Result<ResultSet, Error> {
        self.spanner
            .execute_sql(Request::new(ExecuteSqlRequest {
                session: session.name().to_string(),
                transaction: Some(selector.into()),
                sql: statement.to_string(),
                params: None,                // TODO: statement parameters
                param_types: HashMap::new(), // TODO: Struct for both values and types
                resume_token: vec![],
                query_mode: QueryMode::Normal as i32,
                partition_token: vec![],
                seqno: 0, // ignored for queries
                query_options: None,
                request_options: None,
            }))
            .await?
            .into_inner()
            .try_into()
    }
}
