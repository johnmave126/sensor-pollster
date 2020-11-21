use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};

#[inline]
fn default_poll_period() -> u64 {
    300
}
#[inline]
fn default_retry() -> usize {
    1
}

#[serde_as]
#[derive(Debug, Clone, Deserialize, Serialize)]
pub(crate) struct Config {
    // The database connect string
    // required
    pub db_string: String,
    // Polling period for sensors in seconds
    // optional, default: 300 (5 min)
    #[serde(default = "default_poll_period")]
    pub poll_period: u64,
    // Number of retry when a connection fails
    // optional, default: 1
    #[serde(default = "default_retry")]
    pub retry: usize,
    // List of sensor MAC addresses to monitor
    #[serde_as(as = "HashMap<DisplayFromStr, _>")]
    pub devices: HashMap<btleplug::api::BDAddr, String>,
}
