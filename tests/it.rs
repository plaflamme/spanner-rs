use async_trait::async_trait;
use spanner_rs::{Client, DatabaseId, InstanceId, Key, KeySet, SpannerResource, Value};
use std::collections::HashMap;
use testcontainers::{clients, Container, Docker, Image, WaitForMessage};

#[derive(Default, Debug, Clone)]
struct SpannerEmulator;
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
trait SpannerContainer {
    fn http_port(&self) -> u16;

    async fn post(&self, path: String, body: String) {
        reqwest::Client::new()
            .post(format!("http://localhost:{}/v1/{}", self.http_port(), path))
            .body(body)
            .send()
            .await
            .unwrap();
    }

    async fn with_instance(&self, instance: &InstanceId) -> &Self {
        self.post(
            instance.resources_id(),
            format!(r#"{{"instanceId": "{}"}}"#, instance.name()),
        )
        .await;
        self
    }

    async fn with_database(&self, database: &DatabaseId) -> &Self {
        self.post(
            database.resources_id(),
            format!(
                r#"{{"createStatement":"CREATE DATABASE `{}`"}}"#,
                database.name()
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
}

#[tokio::test]
async fn test_create_session() -> Result<(), spanner_rs::Error> {
    let docker = clients::Cli::default();
    let container = docker.run(SpannerEmulator);

    let instance_id = InstanceId::new("test-project", "test-instance");
    let database_id = DatabaseId::new(&instance_id, "test-database");

    container
        .with_instance(&instance_id)
        .await
        .with_database(&database_id)
        .await;

    let mut client = Client::config()
        .endpoint("localhost")
        .port(container.get_host_port(9010).unwrap())
        .database(database_id)
        .connect()
        .await?;

    let session = client.create_session().await?;

    assert_eq!(
        session.name,
        "projects/test-project/instances/test-instance/databases/test-database/sessions/0"
    );

    let read = client
        .read(
            "my_table",
            KeySet::from(vec![Key::from(Value::Int64(32))]),
            vec!["a".to_string(), "b".to_string()],
        )
        .await;

    assert!(read.is_err());

    Ok(())
}
