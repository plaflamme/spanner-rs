use super::Connection;
use crate::auth::AuthFilter;
use crate::proto::google::spanner::v1 as proto;
use crate::{
    DatabaseId, Error, KeySet, ResultSet, Session, SpannerResource, ToSpanner, Transaction,
    TransactionSelector,
};
use async_trait::async_trait;
use gcp_auth::AuthenticationManager;
use proto::{
    execute_sql_request::QueryMode, spanner_client::SpannerClient, CommitRequest,
    CreateSessionRequest, DeleteSessionRequest, ExecuteSqlRequest, ReadRequest, RollbackRequest,
};
use tonic::transport::{Channel, ClientTlsConfig};
use tonic::Request;
use tower::filter::{AsyncFilter, AsyncFilterLayer};
use tower::util::Either;
use tower::ServiceBuilder;

#[derive(Clone)]
struct GrpcConnection {
    database: DatabaseId,
    // TODO: abstract over Service
    spanner: SpannerClient<Either<AsyncFilter<Channel, AuthFilter>, Channel>>,
}

pub(crate) async fn connect(
    endpoint: Option<String>,
    tls_config: Option<ClientTlsConfig>,
    auth: Option<AuthenticationManager>,
    database: DatabaseId,
) -> Result<Box<dyn Connection>, Error> {
    let channel = match endpoint {
        None => Channel::from_static("https://spanner.googleapis.com")
            .tls_config(tls_config.ok_or_else(|| Error::Config("TLS is required".into()))?)?,
        Some(hostname) => {
            let channel = Channel::from_shared(hostname).map_err(|invalid_uri| {
                Error::Config(format!("invalid endpoint: {}", invalid_uri))
            })?;
            if let Some(tls_config) = tls_config {
                channel.tls_config(tls_config)?
            } else {
                channel
            }
        }
    };

    let channel = channel.connect().await?;

    let auth_layer = auth
        .map(|auth| AsyncFilterLayer::new(AuthFilter::new(auth, crate::auth::Scopes::Database)));

    let channel = ServiceBuilder::new()
        .option_layer(auth_layer)
        .service(channel);

    Ok(Box::new(GrpcConnection {
        database,
        spanner: SpannerClient::new(channel),
    }))
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
        parameters: &[(&str, &(dyn ToSpanner + Sync))],
    ) -> Result<ResultSet, Error> {
        let mut params = std::collections::BTreeMap::new();
        let mut param_types = std::collections::HashMap::new();

        for (name, value) in parameters {
            let value = value.to_spanner()?;
            param_types.insert(name.to_string(), value.r#type().into());
            params.insert(name.to_string(), value.into());
        }

        self.spanner
            .execute_sql(Request::new(ExecuteSqlRequest {
                session: session.name().to_string(),
                transaction: Some(selector.clone().into()),
                sql: statement.to_string(),
                params: Some(prost_types::Struct { fields: params }),
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
