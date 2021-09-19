use std::collections::HashMap;
use std::convert::TryInto;

use crate::proto::google::spanner::v1::commit_request;
use crate::proto::google::spanner::v1::execute_sql_request::QueryMode;
use crate::proto::google::spanner::v1::transaction_options::{Mode, ReadWrite};
use crate::proto::google::spanner::v1::transaction_selector::Selector;
use crate::proto::google::spanner::v1::{
    spanner_client::SpannerClient, CommitRequest, CreateSessionRequest, ExecuteSqlRequest,
    ReadRequest, Session, TransactionOptions, TransactionSelector,
};
use crate::{Config, DatabaseId, Error, KeySet, ResultSet, SpannerResource};
use async_trait::async_trait;
use tonic::transport::Channel;
use tonic::Request;

#[async_trait]
pub trait Connection {
    async fn read(
        &mut self,
        table: &str,
        key_set: KeySet,
        columns: Vec<String>,
    ) -> Result<ResultSet, Error>;

    async fn execute_sql(&mut self, statement: &str) -> Result<ResultSet, Error>;
}

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

    async fn create_session(&mut self) -> Result<Session, Error> {
        let response = self
            .spanner
            .create_session(Request::new(CreateSessionRequest {
                database: self.database.id(),
                session: None,
            }))
            .await?;
        Ok(response.into_inner())
    }
}

#[async_trait]
impl Connection for GrpcConnection {
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

    async fn execute_sql(&mut self, statement: &str) -> Result<ResultSet, Error> {
        let session = self.create_session().await?;
        let result = self
            .spanner
            .execute_sql(Request::new(ExecuteSqlRequest {
                session: session.name.clone(),
                transaction: Some(TransactionSelector {
                    selector: Some(Selector::Begin(TransactionOptions {
                        mode: Some(Mode::ReadWrite(ReadWrite {})),
                    })),
                }),
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
            .into_inner();
        self.spanner
            .commit(Request::new(CommitRequest {
                session: session.name.clone(),
                mutations: vec![],
                return_commit_stats: false,
                request_options: None,
                transaction: Some(commit_request::Transaction::TransactionId(
                    result
                        .metadata
                        .as_ref()
                        .and_then(|rs| rs.transaction.as_ref())
                        .map(|tx| tx.id.clone())
                        .unwrap(),
                )),
            }))
            .await?;
        result.try_into()
    }
}
