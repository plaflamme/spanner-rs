#![feature(async_closure)]
use spanner_rs::proto::google::spanner::v1::{spanner_client::SpannerClient, CreateSessionRequest};
use testcontainers::{clients, images::generic::WaitFor, Docker};
use tonic::{transport::Channel, Request};

#[tokio::test]
async fn test_create_session() -> Result<(), Box<dyn std::error::Error>> {
    let docker = clients::Cli::default();
    let spanner_emulator = testcontainers::images::generic::GenericImage::new(
        "gcr.io/cloud-spanner-emulator/emulator",
    )
    .with_wait_for(WaitFor::message_on_stderr("gRPC server listening"));

    let container = docker.run(spanner_emulator);

    let http_port = container.get_host_port(9020).unwrap();

    let post = async move |path: &'static str, body: &'static str| {
        reqwest::Client::new()
            .post(format!("http://localhost:{}{}", http_port, path))
            .body(body)
            .send()
            .await
    };

    post(
        "/v1/projects/test-project/instances",
        r#"{"instanceId": "test-instance"}"#,
    )
    .await?;
    post(
        "/v1/projects/test-project/instances/test-instance/databases",
        r#"{"createStatement":"CREATE DATABASE `test-database`"}"#,
    )
    .await?;

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
