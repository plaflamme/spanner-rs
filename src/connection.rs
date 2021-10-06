use crate::{Error, ResultSet, Session, ToSpanner, Transaction, TransactionSelector};
use async_trait::async_trait;
use dyn_clone::DynClone;

#[async_trait]
pub(crate) trait Connection
where
    Self: DynClone + Send,
{
    async fn create_session(&mut self) -> Result<Session, Error>;
    async fn delete_session(&mut self, session: Session) -> Result<(), Error>;
    async fn commit(&mut self, session: &Session, transaction: Transaction) -> Result<(), Error>;
    async fn rollback(&mut self, session: &Session, transaction: Transaction) -> Result<(), Error>;
    async fn execute_sql(
        &mut self,
        session: &Session,
        selector: &TransactionSelector,
        statement: &str,
        parameters: &[(&str, &(dyn ToSpanner + Sync))],
        seqno: Option<i64>,
    ) -> Result<ResultSet, Error>;
}

dyn_clone::clone_trait_object!(Connection);

pub(crate) mod grpc;
