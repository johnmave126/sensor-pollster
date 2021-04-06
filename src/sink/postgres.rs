use std::time::Duration;

use anyhow::Context;
use async_trait::async_trait;
use eui48::MacAddress;
use log::{debug, error, info};
use serde::{Deserialize, Serialize};
use tokio_postgres::{types::Type as SQLType, Client, NoTls, Statement};

use super::Sink;
use crate::message::UpdateMessage;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    host: String,
    port: Option<u16>,
    user: String,
    password: Option<String>,
    dbname: Option<String>,
    options: Option<String>,
    #[serde(with = "humantime_serde")]
    connect_timeout: Option<Duration>,
    keepalive: Option<bool>,
    #[serde(with = "humantime_serde")]
    keepalive_idle: Option<Duration>,

    tname: String,
}

pub struct PostgresSink {
    host: String,
    client: Client,
    insert_stmt: Statement,
}

impl PostgresSink {
    pub async fn new(config: Config) -> anyhow::Result<Self> {
        let builder = Self::build(&config);

        debug!("connecting to database `{}`", config.host);
        let (client, connection) = builder
            .connect(NoTls)
            .await
            .with_context(|| format!("fail to connect to database `{}`", config.host))?;
        info!("connected to database {}", config.host);
        tokio::spawn({
            let host = config.host.clone();
            async move {
                if let Err(e) = connection.await {
                    error!("connection to `{}` error: {}", host, e);
                }
            }
        });
        debug!("initializing database `{}`", config.host);
        Self::init_table(&client, &config).await?;
        info!("initialized database `{}`", config.host);

        let insert_sql = format!(
            "INSERT INTO {} (device, timestamp, temperature, battery, rssi) VALUES ($1, $2, $3, $4, $5)",
            config.tname
        );
        let insert_stmt = client
            .prepare_typed(
                &insert_sql,
                &[
                    SQLType::MACADDR,
                    SQLType::TIMESTAMPTZ,
                    SQLType::FLOAT4,
                    SQLType::INT2,
                    SQLType::INT2,
                ],
            )
            .await
            .with_context(|| {
                format!("fail to prepare SQL statement for table `{}`", config.tname)
            })?;

        Ok(Self {
            host: config.host,
            client,
            insert_stmt,
        })
    }

    fn build(config: &Config) -> tokio_postgres::Config {
        let mut builder = tokio_postgres::Config::new();
        builder.user(&config.user).host(&config.host);
        config.port.map(|port| builder.port(port));
        config
            .password
            .as_ref()
            .map(|password| builder.password(password));
        config.dbname.as_ref().map(|dbname| builder.dbname(dbname));
        config
            .options
            .as_ref()
            .map(|options| builder.options(options));
        config
            .connect_timeout
            .map(|connect_timeout| builder.connect_timeout(connect_timeout));
        config
            .keepalive
            .map(|keepalive| builder.keepalives(keepalive));
        config
            .keepalive_idle
            .map(|keepalive_idle| builder.keepalives_idle(keepalive_idle));
        builder
    }

    async fn init_table(client: &Client, config: &Config) -> anyhow::Result<()> {
        debug!(
            "initializing table `{}` for database `{}`",
            config.tname, config.host
        );
        let sql = format!(
            "
            CREATE TABLE IF NOT EXISTS {tname} (
                device      MACADDR NOT NULL,
                timestamp   TIMESTAMPTZ NOT NULL,
                temperature REAL NOT NULL,
                battery     SMALLINT NOT NULL,
                rssi        SMALLINT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS device_timestamp ON {tname} (device, timestamp);
            CREATE INDEX IF NOT EXISTS timestamp_device ON {tname} (timestamp, device);
        ",
            tname = config.tname
        );
        client.batch_execute(&sql).await.with_context(|| {
            format!(
                "fail to initialize table `{}` for database `{}`",
                config.tname, config.host
            )
        })?;
        debug!(
            "initialized table `{}` for database `{}`",
            config.tname, config.host
        );

        Ok(())
    }
}

#[async_trait]
impl Sink for PostgresSink {
    async fn update(&mut self, message: &UpdateMessage) -> anyhow::Result<()> {
        debug!(
            "submitting message {:?} into database `{}`",
            message, self.host
        );
        let mut raw_addr = message.device.address.clone();
        raw_addr.reverse();
        self.client
            .execute(
                &self.insert_stmt,
                &[
                    &MacAddress::new(raw_addr),
                    &message.timestamp,
                    &message.temperature,
                    &(message.battery as i16),
                    &message.rssi,
                ],
            )
            .await
            .with_context(|| {
                format!(
                    "failed to update device `{}` in database `{}`",
                    message.device, self.host
                )
            })?;

        Ok(())
    }
}
