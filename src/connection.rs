use std::convert::TryInto;

use crate::proto::google::spanner::v1 as proto;
use crate::{
    DatabaseId, Error, KeySet, ResultSet, Session, SpannerResource, Transaction,
    TransactionSelector, Value,
};
use async_trait::async_trait;
use proto::{
    execute_sql_request::QueryMode, spanner_client::SpannerClient, CommitRequest,
    CreateSessionRequest, DeleteSessionRequest, ExecuteSqlRequest, ReadRequest, RollbackRequest,
};
use tonic::transport::Channel;
use tonic::Request;

#[async_trait]
pub(crate) trait Connection: Clone {
    async fn create_session(&mut self) -> Result<Session, Error>;
    async fn delete_session(&mut self, session: Session) -> Result<(), Error>;
    async fn commit(&mut self, session: &Session, transaction: Transaction) -> Result<(), Error>;
    async fn rollback(&mut self, session: &Session, transaction: Transaction) -> Result<(), Error>;

    async fn read(
        &mut self,
        table: &str,
        key_set: KeySet,
        columns: Vec<String>,
    ) -> Result<ResultSet, Error>;

    async fn execute_sql(
        &mut self,
        session: &Session,
        selector: &TransactionSelector,
        statement: &str,
        parameters: Vec<(String, Value)>,
    ) -> Result<ResultSet, Error>;
}

#[derive(Clone)]
pub(crate) struct GrpcConnection {
    database: DatabaseId,
    spanner: SpannerClient<Channel>,
}

impl GrpcConnection {
    pub(crate) async fn connect(endpoint: String, database: DatabaseId) -> Result<Self, Error> {
        let channel = Channel::from_shared(endpoint)
            .map_err(|invalid_uri| Error::Config(format!("invalid endpoint: {}", invalid_uri)))?
            .connect()
            .await?;

        Ok(Self {
            database,
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

    async fn commit(&mut self, session: &Session, tx: Transaction) -> Result<(), Error> {
        self.spanner
            .commit(Request::new(CommitRequest {
                session: session.name().to_string(),
                mutations: vec![],
                return_commit_stats: false,
                transaction: Some(proto::commit_request::Transaction::TransactionId(
                    tx.id().clone(),
                )),
                request_options: None,
            }))
            .await?;
        Ok(())
    }

    async fn rollback(&mut self, session: &Session, tx: Transaction) -> Result<(), Error> {
        self.spanner
            .rollback(Request::new(RollbackRequest {
                session: session.name().to_string(),
                transaction_id: tx.id().clone(),
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
        selector: &TransactionSelector,
        statement: &str,
        parameters: Vec<(String, Value)>,
    ) -> Result<ResultSet, Error> {
        let params = Some(prost_types::Struct {
            fields: parameters
                .clone()
                .into_iter()
                .map(|(name, value)| (name, value.into()))
                .collect(),
        });
        let param_types = parameters
            .clone()
            .into_iter()
            .map(|(name, value)| (name, value.r#type().into()))
            .collect();

        self.spanner
            .execute_sql(Request::new(ExecuteSqlRequest {
                session: session.name().to_string(),
                transaction: Some(selector.clone().into()),
                sql: statement.to_string(),
                params,
                param_types,
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
