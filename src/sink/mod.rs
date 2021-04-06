mod console;
mod file;
mod postgres;

use anyhow::Context;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::message::UpdateMessage;

#[async_trait]
pub trait Sink: Send + Sync {
    async fn update(&mut self, message: &UpdateMessage) -> anyhow::Result<()>;
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub enum SinkConfig {
    Console,
    Postgres(postgres::Config),
    File(file::Config),
}

impl SinkConfig {
    pub async fn into_sink(self) -> anyhow::Result<Box<dyn Sink>> {
        match self {
            SinkConfig::Console => Ok(Box::new(console::ConsoleSink)),
            SinkConfig::Postgres(config) => Ok(Box::new(
                postgres::PostgresSink::new(config)
                    .await
                    .context("failed to create postgres sink")?,
            )),
            SinkConfig::File(config) => Ok(Box::new(
                file::FileSink::new(config)
                    .await
                    .context("failed to create file sink")?,
            )),
        }
    }
}
