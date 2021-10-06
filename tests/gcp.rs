use std::ops::{Deref, DerefMut};

use spanner_rs::{Client, DatabaseId, Error, InstanceId};

pub(crate) struct ClientWrapper(Client);
impl Deref for ClientWrapper {
    type Target = Client;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for ClientWrapper {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

#[allow(dead_code)]
pub(crate) async fn new_client() -> Result<ClientWrapper, Error> {
    let _ = env_logger::builder().is_test(true).try_init();
    dotenv::dotenv().ok();

    let project_id = std::env::var("SPANNER_RS_PROJECT_ID")
        .expect("missing SPANNER_RS_PROJECT_ID environment variable");
    let instance = std::env::var("SPANNER_RS_INSTANCE")
        .expect("missing SPANNER_RS_INSTANCE environment variable");
    let database = std::env::var("SPANNER_RS_DATABASE")
        .expect("missing SPANNER_RS_DATABASE environment variable");

    let client = Client::configure()
        .database(DatabaseId::new(
            InstanceId::new(&project_id, &instance),
            &database,
        ))
        .connect()
        .await?;

    Ok(ClientWrapper(client))
}
