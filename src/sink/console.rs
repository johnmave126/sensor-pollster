use async_trait::async_trait;

use super::Sink;
use crate::message::UpdateMessage;

pub struct ConsoleSink;

#[async_trait]
impl Sink for ConsoleSink {
    async fn update(&mut self, message: &UpdateMessage) -> anyhow::Result<()> {
        println!(
            "[update] {}: device {} reports temp {}C, battery {}%, rssi {}db",
            message.timestamp, message.device, message.temperature, message.battery, message.rssi
        );
        Ok(())
    }
}
