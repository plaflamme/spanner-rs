#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("configuration error: {0}")]
    Config(String),

    #[error("transport error: {0}")]
    TransportError(#[from] tonic::transport::Error),

    #[error("unexpected gRPC status: {0}")]
    Status(#[from] tonic::Status),
}
