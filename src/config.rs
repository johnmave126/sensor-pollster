use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::{devices::DeviceRepositoryConfig, sink::SinkConfig};

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Config {
    pub device_repository: DeviceRepositoryConfig,
    pub sinks: Vec<SinkConfig>,
    #[serde(with = "humantime_serde")]
    pub interval: Duration,
    pub retry: usize,
}
