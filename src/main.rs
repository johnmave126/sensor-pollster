mod ble;
mod config;
mod db;
mod message;
mod util;
use crate::config::Config;
use crate::util::AttachContext;

use std::{fs::File, io::BufReader};

use anyhow::Context;
use clap::{App, Arg};
use futures::future::try_join;
use indoc::indoc;
use log::{error, info, warn};
use thiserror::Error;
use tokio::{
    sync,
    time::{sleep, Duration, Instant},
};

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
                    retry: optional, default 1, max number of retry when connection fails
                    devices: required, a list of MAC addresses for the devices
                    "
                ))
                .value_name("FILE")
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

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let (termination_sender, mut termination_receiver) = sync::broadcast::channel(1);

    let (ble_source, ble_handle) = ble::create_ble_source(&config, termination_sender.subscribe())?;
    let db_handle = db::create_db_sink(
        config.db_string.clone(),
        ble_source,
        termination_sender.subscribe(),
    );
    let task_handle = try_join(ble_handle, db_handle);
    ctrlc::set_handler(move || {
        info!("signal received, terminating...");
        let _ = termination_sender.send(());
    })
    .attach_context("failed to set up signal handlers")?;

    let result = runtime.block_on(async move {
        let mut deadline = sleep(Duration::from_secs(30));
        let mut terminated = false;
        tokio::pin!(task_handle);
        loop {
            tokio::select! {
                _ = termination_receiver.recv() => {
                    terminated = true;
                    deadline.reset(Instant::now() + Duration::from_secs(5));
                },
                _ = &mut deadline, if terminated => {
                    warn!("tasks didn't terminate in time, force exit in 1s");
                    return Ok(());
                },
                r = &mut task_handle => {
                    return Ok(r.map(|_| ())?);
                },
            }
        }
    });
    runtime.shutdown_timeout(Duration::from_secs(1));
    result
}
