#[derive(Debug)]
pub enum Error {
    TransportError(tonic::transport::Error),
    Status(tonic::Status),
}

impl From<tonic::transport::Error> for Error {
    fn from(err: tonic::transport::Error) -> Self {
        Error::TransportError(err)
    }
}

impl From<tonic::Status> for Error {
    fn from(err: tonic::Status) -> Self {
        Error::Status(err)
    }
}
