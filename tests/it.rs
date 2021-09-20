use std::ops::{Deref, DerefMut};

use spanner_rs::{Client, DatabaseId, Error, InstanceId, ReadContext, Value};
use testcontainers::{clients, Container, Docker};

mod spanner_emulator;

use ctor::ctor;
use spanner_emulator::{SpannerContainer, SpannerEmulator};

// Holds on to Container so it is dropped with Client.
// This is necessary to keep the container running for the duration of the test.
struct ClientFixture<'a> {
    _container: Container<'a, clients::Cli, SpannerEmulator>,
    client: Client,
}

impl<'a> Deref for ClientFixture<'a> {
    type Target = Client;

    fn deref(&self) -> &Self::Target {
        &self.client
    }
}

impl<'a> DerefMut for ClientFixture<'a> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.client
    }
}

#[ctor]
static DOCKER: clients::Cli = clients::Cli::default();

async fn new_client<'a>() -> Result<ClientFixture<'a>, Error> {
    let instance_id = InstanceId::new("test-project", "test-instance");
    let database_id = DatabaseId::new(&instance_id, "test-database");
    let container = DOCKER.run(SpannerEmulator);
    container
        .with_instance(&instance_id)
        .await
        .with_database(
            &database_id,
            vec!["CREATE TABLE my_table(a INT64, b STRING(MAX)) PRIMARY KEY(a)"],
        )
        .await;

    let client = Client::config()
        .endpoint(format!("http://localhost:{}", container.grpc_port()))
        .database(database_id)
        .connect()
        .await?;

    Ok(ClientFixture {
        _container: container,
        client,
    })
}

#[tokio::test]
async fn test_read_only() -> Result<(), Error> {
    let client = new_client().await?;
    let mut read_only = client.read_only();

    let result_set = read_only.execute_sql("SELECT * FROM my_table").await?;
    let row = result_set.iter().next();
    assert!(row.is_none());

    let result_set = read_only.execute_sql("SELECT * FROM my_table").await?;
    let row = result_set.iter().next();
    assert!(row.is_none());
    Ok(())
}

#[tokio::test]
async fn test_read_write() -> Result<(), Error> {
    let mut client = new_client().await?;
    let row_count = client
        .read_write()
        .run(|ctx| ctx.execute_update("INSERT INTO my_table(a,b) VALUES(1,\"one\")"))
        .await?;

    assert_eq!(row_count, 1);

    let result_set = client
        .read_only()
        .execute_sql("SELECT * FROM my_table")
        .await?;
    let row = result_set.iter().next();
    assert!(row.is_some());
    let row = row.unwrap();
    assert_eq!(row.try_get_by_name("a")?, Value::Int64(1));
    assert_eq!(row.try_get_by_name("b")?, Value::String("one".to_string()));

    Ok(())
}
