use std::num::TryFromIntError;

use bb8::RunError;
#[cfg(feature = "temporal")]
use chrono::ParseError;
use derive_builder::UninitializedFieldError;

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("spanner client error: {0}")]
    Client(String),
    #[error("configuration error: {0}")]
    Config(String),
    #[error("codec error: {0}")]
    Codec(String),
    #[error("authentication error: {0}")]
    Auth(#[from] gcp_auth::Error),

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

impl From<TryFromIntError> for Error {
    fn from(value: TryFromIntError) -> Self {
        Error::Codec(format!("{}", value))
    }
}

impl From<UninitializedFieldError> for Error {
    fn from(value: UninitializedFieldError) -> Self {
        Error::Config(format!("{}", value))
    }
}

#[cfg(feature = "temporal")]
impl From<ParseError> for Error {
    fn from(p: ParseError) -> Self {
        Error::Codec(format!("unexpected date or datetime format: {}", p))
    }
}

#[cfg(feature = "json")]
impl From<serde_json::Error> for Error {
    fn from(err: serde_json::Error) -> Self {
        Error::Codec(format!("unexpected json value: {}", err))
    }
}
