use bb8::{Builder as PoolBuilder, Pool};

use crate::{connection::GrpcConnection, Client, DatabaseId, Error, SessionManager};
use derive_builder::Builder;

#[derive(Builder, Debug)]
#[builder(pattern = "owned")]
pub struct Config {
    #[builder(setter(into))]
    endpoint: String,
    database: DatabaseId,
    #[builder(setter(strip_option), default)]
    session_pool_config: Option<SessionPoolConfig>,
}

impl Config {
    pub fn builder() -> ConfigBuilder {
        ConfigBuilder::default()
    }

    pub async fn connect(self) -> Result<Client, Error> {
        let connection = GrpcConnection::connect(self.endpoint, self.database).await?;
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
        let cfg = Config::builder().endpoint("endpoint".to_string());
        assert_eq!(cfg.endpoint, Some("endpoint".to_string()))
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
