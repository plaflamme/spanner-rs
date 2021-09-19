use async_trait::async_trait;
use spanner_rs::{DatabaseId, InstanceId, SpannerResource};

use std::collections::HashMap;
use testcontainers::{Container, Docker, Image, WaitForMessage};

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
