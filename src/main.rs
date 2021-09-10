use spanner_rs::{
    self,
    proto::google::spanner::v1::{spanner_client::SpannerClient, CreateSessionRequest, Session},
};

use tonic::{transport::Channel, Request};

const ENDPOINT: &str = "http://localhost:9010";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let channel = Channel::from_static(ENDPOINT).connect().await?;

    let mut service = SpannerClient::new(channel);

    let response = service
        .create_session(Request::new(CreateSessionRequest {
            database: "projects/test-project/instances/test-instance/databases/test-database"
                .to_string(),
            session: Some(Session::default()),
        }))
        .await?;

    println!("RESPONSE={:?}", response);

    Ok(())
}
