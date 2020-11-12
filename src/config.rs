use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_with::{serde_as, DisplayFromStr};

#[inline]
fn default_db_tname() -> String {
    "sensor_pollster_reports".to_string()
}
#[inline]
fn default_poll_period() -> u64 {
    300
}
#[inline]
fn default_scan_period() -> u64 {
    300
}
#[inline]
fn default_scan_duration() -> u64 {
    60
}
#[inline]
fn default_retry_min() -> u16 {
    10
}
#[inline]
fn default_retry_max() -> u16 {
    600
}
#[inline]
fn default_retry_backoff() -> u16 {
    10
}

#[serde_as]
#[derive(Debug, Deserialize, Serialize)]
pub(crate) struct Config {
    // The database connect string
    // required
    pub db_string: String,
    // The database table name
    // optional, default: sensor_pollster_reports
    #[serde(default = "default_db_tname")]
    pub db_tname: String,
    // Polling period for sensors in seconds
    // optional, default: 300 (5 min)
    #[serde(default = "default_poll_period")]
    pub poll_period: u64,
    // Scaning period for device discovery in seconds
    // optional, default: 300 (5 min)
    #[serde(default = "default_scan_period")]
    pub scan_period: u64,
    // Scanning duration for each scanning in seconds
    // optional, default: 60 (1 min)
    #[serde(default = "default_scan_duration")]
    pub scan_duration: u64,
    // Smallest interval for reconnection in seconds
    // optional, default: 10
    #[serde(default = "default_retry_min")]
    pub retry_min: u16,
    // Largest interval for reconnection in seconds
    // optional, default: 600 (10 min)
    #[serde(default = "default_retry_max")]
    pub retry_max: u16,
    // Max number of retry before hitting maximum reconnection interval
    // The backoff multiplier will be automatically calculated
    // optional, default: 10
    #[serde(default = "default_retry_backoff")]
    pub retry_backoff: u16,
    // List of sensor MAC addresses to monitor
    #[serde_as(as = "HashMap<DisplayFromStr, _>")]
    pub devices: HashMap<btleplug::api::BDAddr, String>,
}
