#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("transport error: {0}")]
    TransportError(#[from] tonic::transport::Error),

    #[error("unexpected gRPC status: {0}")]
    Status(#[from] tonic::Status),
}
