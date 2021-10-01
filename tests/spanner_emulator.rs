use async_trait::async_trait;
use spanner_rs::{Client, DatabaseId, Error, InstanceId, SpannerResource};

use ctor::ctor;
use std::{
    collections::HashMap,
    ops::{Deref, DerefMut},
};
use testcontainers::{clients, Container, Docker, Image, WaitForMessage};

#[derive(Default, Debug, Clone)]
pub struct SpannerEmulator;
impl Image for SpannerEmulator {
    type Args = Vec<String>;
    type EnvVars = HashMap<String, String>;
    type Volumes = HashMap<String, String>;
    type EntryPoint = std::convert::Infallible;

    fn descriptor(&self) -> String {
        "gcr.io/cloud-spanner-emulator/emulator".to_string()
    }

    fn wait_until_ready<D: Docker>(&self, container: &testcontainers::Container<'_, D, Self>) {
        container
            .logs()
            .stderr
            .wait_for_message("gRPC server listening")
            .unwrap()
    }

    fn args(&self) -> Self::Args {
        Vec::new()
    }

    fn env_vars(&self) -> Self::EnvVars {
        HashMap::new()
    }

    fn volumes(&self) -> Self::Volumes {
        HashMap::new()
    }

    fn with_args(self, _arguments: Self::Args) -> Self {
        self
    }

    fn with_entrypoint(self, _entryppoint: &Self::EntryPoint) -> Self {
        self
    }

    fn entrypoint(&self) -> Option<String> {
        None
    }
}

#[async_trait]
pub trait SpannerContainer {
    fn http_port(&self) -> u16;

    fn grpc_port(&self) -> u16;

    async fn post(&self, path: String, body: String) {
        let response = reqwest::Client::new()
            .post(format!("http://localhost:{}/v1/{}", self.http_port(), path))
            .body(body)
            .send()
            .await
            .unwrap();

        assert!(response.status().is_success(), "{:?}", response);
    }

    async fn with_instance(&self, instance: &InstanceId) -> &Self {
        self.post(
            instance.resources_id(),
            format!(r#"{{"instanceId": "{}"}}"#, instance.name()),
        )
        .await;
        self
    }

    async fn with_database(&self, database: &DatabaseId, extra_statements: Vec<&str>) -> &Self {
        let json_statements = extra_statements
            .into_iter()
            .map(|s| format!(r#""{}""#, s))
            .collect::<Vec<String>>()
            .join(",");

        self.post(
            database.resources_id(),
            format!(
                r#"{{"createStatement":"CREATE DATABASE `{}`", "extraStatements":[{}]}}"#,
                database.name(),
                json_statements,
            ),
        )
        .await;
        self
    }
}

impl<'a, D: Docker> SpannerContainer for Container<'a, D, SpannerEmulator> {
    fn http_port(&self) -> u16 {
        self.get_host_port(9020).unwrap()
    }
    fn grpc_port(&self) -> u16 {
        self.get_host_port(9010).unwrap()
    }
}

// Holds on to Container so it is dropped with Client.
// This is necessary to keep the container running for the duration of the test.
pub(crate) struct ClientFixture<'a> {
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
static DOCKER: clients::Cli = {
    let _ = env_logger::builder().is_test(true).try_init();
    clients::Cli::default()
};

#[allow(dead_code)]
pub(crate) async fn new_client<'a>() -> Result<ClientFixture<'a>, Error> {
    let _ = env_logger::builder().is_test(true).try_init();
    let instance_id = InstanceId::new("test-project", "test-instance");
    let database_id = DatabaseId::new(instance_id.clone(), "test-database");
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
        .with_emulator_grpc_port(container.grpc_port())
        .database(database_id)
        .connect()
        .await?;

    Ok(ClientFixture {
        _container: container,
        client,
    })
}
