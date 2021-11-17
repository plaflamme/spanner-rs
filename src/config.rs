use bb8::{Builder as PoolBuilder, Pool};
use tonic::transport::ClientTlsConfig;

use crate::{Client, DatabaseId, Error, InstanceId, ProjectId, SessionManager};
use derive_builder::Builder;

/// Configuration for building a [`Client`].
///
/// # Example
///
/// ```no_run
/// use spanner_rs::Config;
/// #[tokio::main]
/// # async fn main() -> Result<(), spanner_rs::Error> {
/// let mut client = Config::builder()
///     .project("my-gcp-project")
///     .instance("my-spanner-instance")
///     .database("my-database")
///     .connect()
///     .await?;
/// # Ok(()) }
/// ```
#[derive(Builder, Debug)]
#[builder(pattern = "owned", build_fn(error = "crate::Error"))]
pub struct Config {
    /// Set the URI to use to reach the Spanner API. Leave unspecified to use Cloud Spanner.
    #[builder(setter(strip_option, into), default)]
    endpoint: Option<String>,

    /// Set custom client-side TLS settings.
    #[builder(setter(strip_option), default = "Some(ClientTlsConfig::default())")]
    tls_config: Option<ClientTlsConfig>,

    /// Specify the GCP project where the Cloud Spanner instance exists.
    ///
    /// This may be left unspecified, in which case, the project will be extracted
    /// from the credentials. Note that this only works when authenticating using [service accounts](https://cloud.google.com/docs/authentication/production).
    #[builder(setter(strip_option, into), default)]
    project: Option<String>,

    /// Set the Cloud Spanner instance ID.
    #[builder(setter(strip_option, into))]
    instance: String,

    /// Set the Cloud Spanner database name.
    #[builder(setter(strip_option, into))]
    database: String,

    /// Programatically specify the credentials file to use during authentication.
    ///
    /// When this is specified, it is used in favor of the `GOOGLE_APPLICATION_CREDENTIALS` environment variable.
    #[builder(setter(strip_option, into), default)]
    credentials_file: Option<String>,

    /// Configuration for the embedded session pool.
    #[builder(setter(strip_option), default)]
    session_pool_config: Option<SessionPoolConfig>,
}

impl Config {
    /// Returns a new [`ConfigBuilder`] for configuring a new client.
    pub fn builder() -> ConfigBuilder {
        ConfigBuilder::default()
    }

    /// Connect to Cloud Spanner and return a new [`Client`].
    ///
    /// # Example
    ///
    /// ```no_run
    /// use spanner_rs::Config;
    /// #[tokio::main]
    /// # async fn main() -> Result<(), spanner_rs::Error> {
    /// let mut client = Config::builder()
    ///     .project("my-gcp-project")
    ///     .instance("my-spanner-instance")
    ///     .database("my-database")
    ///     .connect()
    ///     .await?;
    /// # Ok(()) }
    /// ```
    ///
    /// # Authentication
    ///
    /// Authentication uses the [`gcp_auth`] crate which supports several authentication methods.
    /// In a typical production environment, nothing needs to be programatically provided during configuration as
    /// credentials are normally obtained from the environment (i.e.: `GOOGLE_APPLICATION_CREDENTIALS`).
    ///
    /// Similarly, for local development, authentication will transparently delegate to the `gcloud` command line tool.
    pub async fn connect(self) -> Result<Client, Error> {
        let auth = if self.tls_config.is_none() {
            None
        } else {
            match self.credentials_file {
                Some(file) => Some(gcp_auth::from_credentials_file(file).await?),
                None => Some(gcp_auth::init().await?),
            }
        };

        let project_id = match self.project {
            Some(project) => project,
            None => {
                if let Some(auth) = auth.as_ref() {
                    auth.project_id().await?
                } else {
                    return Err(Error::Config("missing project id".to_string()));
                }
            }
        };
        let database_id = DatabaseId::new(
            InstanceId::new(ProjectId::new(&project_id), &self.instance),
            &self.database,
        );

        let connection =
            crate::connection::grpc::connect(self.endpoint, self.tls_config, auth, database_id)
                .await?;

        let pool = self
            .session_pool_config
            .unwrap_or_default()
            .build()
            .build(SessionManager::new(connection.clone()))
            .await?;

        Ok(Client::connect(connection, pool))
    }
}

impl ConfigBuilder {
    /// Disable TLS when connecting to Spanner. This usually only makes sense when using the emulator.
    /// Note that this also disables authentication to prevent sending secrets in plain text.
    pub fn disable_tls(self) -> Self {
        Self {
            tls_config: Some(None),
            ..self
        }
    }

    /// Configure the client to connect to a Spanner emulator, e.g.: `http://localhost:9092`
    /// This disables TLS.
    pub fn with_emulator_host(self, endpoint: String) -> Self {
        self.endpoint(endpoint).disable_tls()
    }

    /// Configure the client to connect to a Spanner emulator running on localhost and using the specified port.
    /// This disables TLS.
    pub fn with_emulator_grpc_port(self, port: u16) -> Self {
        self.with_emulator_host(format!("http://localhost:{}", port))
    }

    /// See [Config::connect]
    pub async fn connect(self) -> Result<Client, Error> {
        self.build()?.connect().await
    }
}

/// Configuration for the internal Cloud Spanner session pool.
///
/// # Example
///
/// ```
/// use spanner_rs::{Config, SessionPoolConfig};
///
/// # fn main() -> Result<(), spanner_rs::Error> {
/// Config::builder().session_pool_config(SessionPoolConfig::builder().max_size(100).build()?);
/// # Ok(()) }
/// ```
#[derive(Builder, Default, Debug)]
#[builder(pattern = "owned", build_fn(error = "crate::Error"))]
pub struct SessionPoolConfig {
    /// Specify the maximum number of sessions that should be maintained in the pool.
    #[builder(setter(strip_option), default)]
    max_size: Option<u32>,

    /// Specify the minimum number of sessions that should be maintained in the pool.
    #[builder(setter(strip_option), default)]
    min_idle: Option<u32>,
}

impl SessionPoolConfig {
    pub fn builder() -> SessionPoolConfigBuilder {
        SessionPoolConfigBuilder::default()
    }

    fn build(self) -> PoolBuilder<SessionManager> {
        let mut builder = Pool::builder().test_on_check_out(false);
        if let Some(max_size) = self.max_size {
            builder = builder.max_size(max_size);
        }
        builder.min_idle(self.min_idle)
    }
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn test_config_database() {
        let cfg = Config::builder()
            .project("project")
            .instance("instance")
            .database("database")
            .build()
            .unwrap();

        assert_eq!(cfg.project, Some("project".to_string()));
        assert_eq!(cfg.instance, "instance".to_string());
        assert_eq!(cfg.database, "database".to_string());
    }

    #[test]
    fn test_config_endpoint() {
        let cfg = Config::builder().endpoint("endpoint");
        assert_eq!(cfg.endpoint, Some(Some("endpoint".to_string())))
    }

    #[test]
    fn test_session_pool_config() {
        let built = SessionPoolConfig::builder()
            .max_size(10)
            .min_idle(100)
            .build()
            .unwrap();

        assert_eq!(built.max_size, Some(10));
        assert_eq!(built.min_idle, Some(100));
    }
}
