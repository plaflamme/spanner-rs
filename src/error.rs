use bb8::RunError;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("spanner client error: {0}")]
    Client(String),
    #[error("configuration error: {0}")]
    Config(String),
    #[error("codec error: {0}")]
    Codec(String),

    #[error("transport error: {0}")]
    TransportError(#[from] tonic::transport::Error),

    #[error("unexpected gRPC status: {0}")]
    Status(#[from] tonic::Status),
}

impl From<RunError<Error>> for Error {
    fn from(value: RunError<Error>) -> Self {
        match value {
            RunError::User(error) => error,
            RunError::TimedOut => Error::Client("timeout while obtaining new session".to_string()),
        }
    }
}
