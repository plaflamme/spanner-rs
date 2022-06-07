use spanner_rs::{Client, DatabaseId, Error, InstanceId, ProjectId, SpannerResource};

use ctor::ctor;
use std::ops::{Deref, DerefMut};
use testcontainers::{clients, core::WaitFor, Container, Image};

#[derive(Default, Debug, Clone)]
pub struct SpannerEmulator;
impl Image for SpannerEmulator {
    type Args = ();

    fn name(&self) -> String {
        "gcr.io/cloud-spanner-emulator/emulator".to_owned()
    }

    fn tag(&self) -> String {
        "latest".to_owned()
    }

    fn ready_conditions(&self) -> Vec<WaitFor> {
        vec![WaitFor::message_on_stderr("gRPC server listening")]
    }
}

struct SpannerContainer<'a> {
    container: Container<'a, SpannerEmulator>,
}

impl<'a> SpannerContainer<'a> {
    fn http_port(&self) -> u16 {
        self.container.get_host_port_ipv4(9020)
    }

    fn grpc_port(&self) -> u16 {
        self.container.get_host_port_ipv4(9010)
    }

    async fn post(&self, path: String, body: String) {
        let response = reqwest::Client::new()
            .post(format!("http://localhost:{}/v1/{}", self.http_port(), path))
            .body(body)
            .send()
            .await
            .unwrap();

        assert!(response.status().is_success(), "{:?}", response);
    }

    async fn with_instance(&'a self, instance: &InstanceId) {
        self.post(
            instance.resources_path(),
            format!(r#"{{"instanceId": "{}"}}"#, instance.name()),
        )
        .await;
    }

    async fn with_database(&self, database: &DatabaseId, extra_statements: Vec<&str>) {
        let json_statements = extra_statements
            .into_iter()
            .map(|s| format!(r#""{}""#, s))
            .collect::<Vec<String>>()
            .join(",");

        self.post(
            database.resources_path(),
            format!(
                r#"{{"createStatement":"CREATE DATABASE `{}`", "extraStatements":[{}]}}"#,
                database.name(),
                json_statements,
            ),
        )
        .await;
    }
}

// Holds on to Container so it is dropped with Client.
// This is necessary to keep the container running for the duration of the test.
pub(crate) struct ClientFixture<'a> {
    _container: SpannerContainer<'a>,
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
static DOCKER: clients::Cli = {
    let _ = env_logger::builder().is_test(true).try_init();
    clients::Cli::default()
};

#[allow(dead_code)]
pub(crate) async fn new_client<'a>() -> Result<ClientFixture<'a>, Error> {
    let _ = env_logger::builder().is_test(true).try_init();
    let instance_id = InstanceId::new(ProjectId::new("test-project"), "test-instance");
    let database_id = DatabaseId::new(instance_id.clone(), "test-database");
    let container = DOCKER.run(SpannerEmulator);
    let container = SpannerContainer { container };
    container.with_instance(&instance_id).await;
    container.with_database(
            &database_id,
            vec![
                "CREATE TABLE my_table(a INT64, b STRING(MAX)) PRIMARY KEY(a)",
                "CREATE TABLE person(id INT64, name STRING(MAX) NOT NULL, data BYTES(MAX)) PRIMARY KEY(id)",
            ],
        )
        .await;

    let client = Client::configure()
        .with_emulator_grpc_port(container.grpc_port())
        .project("test-project")
        .instance("test-instance")
        .database("test-database")
        .connect()
        .await?;

    Ok(ClientFixture {
        _container: container,
        client,
    })
}
