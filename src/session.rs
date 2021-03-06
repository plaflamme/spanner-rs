use bb8::ManageConnection;
use tokio::sync::Mutex;

use crate::Connection;
use crate::Error;
use google_api_proto::google::spanner::v1 as proto;
pub(crate) struct Session(String);

impl Session {
    pub fn name(&self) -> &str {
        &self.0
    }
}

impl From<proto::Session> for Session {
    fn from(value: proto::Session) -> Self {
        Self(value.name)
    }
}

pub(crate) struct SessionManager {
    connection: Mutex<Box<dyn Connection>>,
}

impl SessionManager {
    pub(crate) fn new(connection: Box<dyn Connection>) -> Self {
        Self {
            connection: Mutex::new(connection),
        }
    }
}

#[async_trait::async_trait]
impl ManageConnection for SessionManager {
    type Connection = Session;
    type Error = Error;

    async fn connect(&self) -> Result<Self::Connection, Self::Error> {
        self.connection.lock().await.create_session().await
    }

    async fn is_valid(&self, _conn: &mut Self::Connection) -> Result<(), Self::Error> {
        Ok(())
    }

    fn has_broken(&self, _conn: &mut Self::Connection) -> bool {
        false
    }
}
