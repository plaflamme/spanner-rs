use bb8::{Builder as PoolBuilder, Pool};
use tonic::transport::ClientTlsConfig;

use crate::{Client, DatabaseId, Error, SessionManager};
use derive_builder::Builder;

#[derive(Builder, Debug)]
#[builder(pattern = "owned")]
pub struct Config {
    /// Set the URI to use to reach the Spanner API. Leave unspecified to use Cloud Spanner.
    #[builder(setter(strip_option, into), default)]
    endpoint: Option<String>,

    /// Set custom client-side TLS settings.
    #[builder(setter(strip_option), default = "Some(ClientTlsConfig::default())")]
    tls_config: Option<ClientTlsConfig>,

    database: DatabaseId,
    #[builder(setter(into), default)]
    credentials_file: Option<String>,
    #[builder(setter(strip_option), default)]
    session_pool_config: Option<SessionPoolConfig>,
}

impl Config {
    pub fn builder() -> ConfigBuilder {
        ConfigBuilder::default()
    }

    pub async fn connect(self) -> Result<Client, Error> {
        let auth = if self.tls_config.is_none() {
            None
        } else {
            match self.credentials_file {
                Some(file) => Some(gcp_auth::from_credentials_file(file).await?),
                None => Some(gcp_auth::init().await?),
            }
        };

        let connection =
            crate::connection::grpc::connect(self.endpoint, self.tls_config, auth, self.database)
                .await?;
        let mgr = SessionManager::new(connection.clone());
        let pool = self
            .session_pool_config
            .unwrap_or_default()
            .build()
            .build(mgr)
            .await?;

        Ok(Client::connect(connection, pool))
    }
}

impl ConfigBuilder {
    /// Disable TLS when connecting to Spanner. This usually only makes sense when using the emulator.
    /// Note that this also disables authentication.
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

    pub async fn connect(self) -> Result<Client, Error> {
        self.build()?.connect().await
    }
}

#[derive(Builder, Default, Debug)]
#[builder(pattern = "owned")]
pub struct SessionPoolConfig {
    #[builder(setter(strip_option), default)]
    max_size: Option<u32>,
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
    use crate::{DatabaseId, InstanceId};

    #[test]
    fn test_config_database() {
        let cfg = Config::builder().database(DatabaseId::new(
            InstanceId::new("project", "instance"),
            "db",
        ));
        assert_eq!(
            cfg.database,
            Some(DatabaseId::new(
                InstanceId::new("project", "instance"),
                "db"
            ))
        )
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
