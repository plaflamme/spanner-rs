use async_trait::async_trait;
use spanner_rs::proto::google::spanner::v1::{spanner_client::SpannerClient, CreateSessionRequest};
use std::collections::HashMap;
use testcontainers::{clients, Container, Docker, Image, WaitForMessage};
use tonic::{transport::Channel, Request};

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
            .post(format!("http://localhost:{}{}", self.http_port(), path))
            .body(body)
            .send()
            .await
            .unwrap();
    }

    async fn create_instance(&self, name: &str) {
        self.post(
            "/v1/projects/test-project/instances".to_string(),
            format!(r#"{{"instanceId": "{}"}}"#, name),
        )
        .await
    }

    async fn create_database(&self, instance: &str, database: &str) {
        self.post(
            format!("/v1/projects/test-project/instances/{}/databases", instance),
            format!(r#"{{"createStatement":"CREATE DATABASE `{}`"}}"#, database),
        )
        .await
    }
}

impl<'a, D: Docker> SpannerContainer for Container<'a, D, SpannerEmulator> {
    fn http_port(&self) -> u16 {
        self.get_host_port(9020).unwrap()
    }
}

#[tokio::test]
async fn test_create_session() -> Result<(), Box<dyn std::error::Error>> {
    let docker = clients::Cli::default();
    let container = docker.run(SpannerEmulator);

    container.create_instance("test-instance").await;
    container
        .create_database("test-instance", "test-database")
        .await;

    let grpc_port = container.get_host_port(9010).unwrap();
    let channel = Channel::from_shared(format!("http://localhost:{}", grpc_port))
        .unwrap()
        .connect()
        .await?;

    let mut service = SpannerClient::new(channel);

    let response = service
        .create_session(Request::new(CreateSessionRequest {
            database: "projects/test-project/instances/test-instance/databases/test-database"
                .to_string(),
            session: None,
        }))
        .await?;

    assert_eq!(response.metadata().get("grpc-status").unwrap(), "0");

    Ok(())
}
