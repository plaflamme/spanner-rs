use std::{future::Future, pin::Pin, sync::Arc};

use gcp_auth::AuthenticationManager;
use http::HeaderValue;
use tower::{filter::AsyncPredicate, BoxError};

const DATABASE_SCOPES: [&str; 2] = [
    "https://www.googleapis.com/auth/cloud-platform",
    "https://www.googleapis.com/auth/spanner.data",
];

const ADMIN_SCOPES: [&str; 3] = [
    "https://www.googleapis.com/auth/cloud-platform",
    "https://www.googleapis.com/auth/spanner.data",
    "https://www.googleapis.com/auth/spanner.admin",
];

#[derive(Clone)]
pub(crate) enum Scopes {
    Database,
    #[allow(dead_code)]
    Admin,
}

impl Scopes {
    fn as_slice(&self) -> &[&str] {
        match self {
            Scopes::Database => &DATABASE_SCOPES,
            Scopes::Admin => &ADMIN_SCOPES,
        }
    }
}

#[derive(Clone)]
pub(crate) struct AuthFilter {
    auth_manager: Arc<AuthenticationManager>,
    scopes: Scopes,
}

impl AuthFilter {
    pub(crate) fn new(auth_manager: AuthenticationManager, scopes: Scopes) -> Self {
        Self {
            auth_manager: Arc::new(auth_manager),
            scopes,
        }
    }
}

impl AsyncPredicate<http::Request<tonic::body::BoxBody>> for AuthFilter {
    type Future = Pin<Box<dyn Future<Output = Result<Self::Request, BoxError>> + Send>>;

    type Request = http::Request<tonic::body::BoxBody>;

    fn check(&mut self, request: http::Request<tonic::body::BoxBody>) -> Self::Future {
        let filter = self.clone();
        Box::pin(async move {
            let token = filter
                .auth_manager
                .get_token(filter.scopes.as_slice())
                .await?;

            let header = HeaderValue::try_from(format!("Bearer {}", token.as_str()))
                .map_err(|err| crate::Error::Client(format!("invalid auth token: {}", err)))?;

            let (mut parts, body) = request.into_parts();
            parts.headers.insert(http::header::AUTHORIZATION, header);
            Ok(http::Request::from_parts(parts, body))
        })
    }
}
