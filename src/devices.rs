use std::{
    collections::HashSet,
    hash::{Hash, Hasher},
};

use anyhow::Context;
use btleplug::api::BDAddr;
use eui48::MacAddress;
use log::{error, info};
use serde::{Deserialize, Serialize};
use tokio_postgres::{Client, NoTls, Statement};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DeviceRepositoryConfig {
    pub host: String,
    pub port: Option<u16>,
    pub user: String,
    pub password: Option<String>,
    pub dbname: Option<String>,
    pub tname: String,
}

impl DeviceRepositoryConfig {
    pub async fn build(self) -> anyhow::Result<DeviceRepository> {
        let mut builder = tokio_postgres::Config::new();
        builder.user(&self.user).host(&self.host);
        self.port.map(|port| builder.port(port));
        self.password
            .as_ref()
            .map(|password| builder.password(password));
        self.dbname.as_ref().map(|dbname| builder.dbname(dbname));

        let (client, connection) = builder
            .connect(NoTls)
            .await
            .with_context(|| format!("fail to connect to database `{}`", self.host))?;

        info!("connected to database {}", self.host);
        tokio::spawn({
            let host = self.host.clone();
            async move {
                if let Err(e) = connection.await {
                    error!("connection to {} error: {}", host, e);
                }
            }
        });

        let select_sql = format!("SELECT name, address from {}", self.tname);
        let select_stmt = client
            .prepare(&select_sql)
            .await
            .with_context(|| format!("fail to prepare SQL statement for table `{}`", self.tname))?;

        Ok(DeviceRepository {
            host: self.host,
            client,
            select_stmt,
        })
    }
}

#[derive(Debug)]
pub struct DeviceConfig {
    pub name: String,
    pub address: BDAddr,
}

impl Hash for DeviceConfig {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.address.hash(state);
    }
}

impl PartialEq for DeviceConfig {
    fn eq(&self, other: &Self) -> bool {
        self.address.eq(&other.address)
    }
}

impl Eq for DeviceConfig {}

pub struct DeviceRepository {
    host: String,
    client: Client,
    select_stmt: Statement,
}

impl DeviceRepository {
    pub async fn retrieve(&mut self) -> anyhow::Result<HashSet<DeviceConfig>> {
        let rows = self
            .client
            .query(&self.select_stmt, &[])
            .await
            .with_context(|| format!("fail to perform query for `{}`", self.host))?;

        rows.into_iter()
            .map(|row| {
                let name: String = row
                    .try_get(0)
                    .context("fail to parse device name from repository")?;
                let address: MacAddress = row
                    .try_get(1)
                    .context("fail to parse MAC address from repository")?;
                let mut address = BDAddr {
                    address: address.to_array(),
                };
                address.address.reverse();

                Ok(DeviceConfig { name, address })
            })
            .collect()
    }
}
