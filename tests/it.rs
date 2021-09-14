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
        .with_database(
            &database_id,
            vec!["CREATE TABLE my_table(a INT64, b INT64) PRIMARY KEY(a)"],
        )
        .await;

    let mut client = Client::config()
        .endpoint(format!(
            "http://localhost:{}",
            container.get_host_port(9010).unwrap()
        ))
        .database(database_id)
        .connect()
        .await?;

    let result_set = client
        .read(
            "my_table",
            KeySet::from(vec![Key::from(Value::Int64(32))]),
            vec!["a".to_string(), "b".to_string()],
        )
        .await?;

    assert!(result_set.iter().next().is_none());

    let result_set = client.execute_sql("SELECT * FROM my_table").await?;

    assert!(result_set.iter().next().is_none());

    Ok(())
}
