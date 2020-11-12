mod ble;
mod config;
mod db;
mod message;
mod util;
use crate::config::Config;
use crate::util::AttachContext;

use std::{
    fs::File,
    io::BufReader,
    sync::{
        atomic::{AtomicBool, Ordering as AtomicOrdering},
        Arc,
    },
};

use anyhow::Context;
use clap::{App, Arg};
use futures::future::try_join;
use indoc::indoc;
use log::{error, info};
use thiserror::Error;
use tokio::{sync, time::Duration};

#[derive(Error, Debug)]
enum Error {
    #[error("{0}")]
    Io(#[from] std::io::Error),
    #[error("failed to parse config file: {0}")]
    ConfigParse(#[from] serde_yaml::Error),
    #[error("database error: {0}")]
    Db(#[from] tokio_postgres::Error),
    #[error("bluetooth error: {0}")]
    Ble(btleplug::Error),
    #[error("synchronization failed: {0}")]
    Channel(#[from] sync::broadcast::error::RecvError),
    #[error("cannot find bluetooth adapter")]
    NoAdapter,
    #[error("Device is not a HM device")]
    NotHMDevice,
}

impl From<btleplug::Error> for Error {
    fn from(err: btleplug::Error) -> Error {
        Error::Ble(err)
    }
}

#[derive(Error, Debug)]
enum VerifyConfigError {
    #[error("scan_duration cannot be 0")]
    ZeroScanDuration,
    #[error("retry_max ({0} provided) must be no smaller than retry_min ({1} provided)")]
    InverseRetryPair(u16, u16),
}

fn verify_config(config: &Config) -> Result<(), VerifyConfigError> {
    if config.scan_duration == 0 {
        Err(VerifyConfigError::ZeroScanDuration)
    } else if config.retry_max < config.retry_min {
        Err(VerifyConfigError::InverseRetryPair(
            config.retry_max,
            config.retry_min,
        ))
    } else {
        Ok(())
    }
}

fn main() -> anyhow::Result<()> {
    let cmd = App::new("sensor-pollster")
        .version("0.1")
        .author("Youmu")
        .about("Monitor temperatures of HM-11 sensors")
        .arg(
            Arg::with_name("config")
                .short("c")
                .long("config")
                .help("Sets custom config file location")
                .long_help(indoc!(
                    "Sets custom config file location, default to read config.yaml
                    The custom config must be a file of YAML 1.2 format.
                    The following configuration keys are supported:
                    db_string: required, the connection string of the database
                    poll_period: optional, default 300, polling period for sensors in seconds
                    scan_period: optional, default 300, scaning period for device discovery in seconds
                    scan_duration: optional, default 60, scanning duration for each scanning in seconds
                    retry_min: optional, default 10, smallest interval for reconnection in seconds
                    retry_max: optional, default 600, largest interval for reconnection in seconds
                    retry_backoff: optional, default 10, max number of retry before hitting maximum reconnection interval
                    devices: required, a list of MAC addresses for the devices
                    "
                )).value_name("FILE")
                .takes_value(true),
        )
        .get_matches();
    env_logger::init();

    let config_path = cmd.value_of("config").unwrap_or("config.yaml");
    info!("open and parse config file {}", config_path);
    let config_file =
        File::open(config_path).context(format!("failed to open file {}", config_path))?;
    let config_reader = BufReader::new(config_file);
    let config: Config = serde_yaml::from_reader(config_reader)
        .attach_context(format!("failed to parse config file {}", config_path))?;

    verify_config(&config)
        .attach_context(format!("failed to verify config file {}", config_path))?;

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let is_running = Arc::new(AtomicBool::new(true));

    let (ble_source, ble_handle) = ble::create_ble_source(&config, is_running.clone())?;
    let db_handle = db::create_db_sink(config.db_string.clone(), ble_source, is_running.clone());
    let task_handle = try_join(ble_handle, db_handle);
    ctrlc::set_handler(move || {
        info!("signal received, terminating...");
        is_running.store(false, AtomicOrdering::SeqCst);
    })
    .attach_context("failed to set up signal handlers")?;

    let result = runtime.block_on(async move { Ok(task_handle.await.map(|_| ())?) });
    runtime.shutdown_timeout(Duration::from_secs(10));
    result
}
