use anyhow::Context;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::{
    fs::{File, OpenOptions},
    io::AsyncWriteExt,
};

use super::Sink;
use crate::message::UpdateMessage;

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    path: String,
}

pub struct FileSink {
    path: String,
    file: File,
}

impl FileSink {
    pub async fn new(config: Config) -> anyhow::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&config.path)
            .await?;
        Ok(Self {
            file,
            path: config.path,
        })
    }
}

#[async_trait]
impl Sink for FileSink {
    async fn update(&mut self, message: &UpdateMessage) -> anyhow::Result<()> {
        let line = format!(
            "[{}] {}: temp {}C, battery {}%, rssi {}db\n",
            message.timestamp, message.device, message.temperature, message.battery, message.rssi
        );

        self.file
            .write(line.as_bytes())
            .await
            .with_context(|| format!("failed to append to file {}", self.path))?;
        self.file
            .flush()
            .await
            .with_context(|| format!("failed to flush file {}", self.path))?;
        Ok(())
    }
}
