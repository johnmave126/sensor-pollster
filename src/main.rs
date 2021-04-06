mod config;
mod devices;
mod message;
mod sensor_hub;
mod sink;

use std::{fs::File, io::BufReader};

use anyhow::Context;
use clap::{crate_name, crate_version, App, Arg};
use futures::stream::{self, StreamExt, TryStreamExt};
use indoc::indoc;
use log::{debug, error, info, warn};
use tokio::{
    sync::mpsc,
    time::{interval_at, sleep, Duration, Instant},
};

use crate::{config::Config, sensor_hub::Error as HubError, sink::SinkConfig};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cmd = App::new(crate_name!())
        .version(crate_version!())
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
        File::open(config_path).with_context(|| format!("fail to open file {}", config_path))?;
    let config_reader = BufReader::new(config_file);
    let config: Config = serde_yaml::from_reader(config_reader)
        .with_context(|| format!("fail to parse config file {}", config_path))?;

    let mut repository = config
        .device_repository
        .build()
        .await
        .context("fail to connect to device repository database")?;

    let (hub, backend) = sensor_hub::init().context("failed to initialize hub for sensors")?;
    let backend_handle = std::thread::spawn(move || {
        backend.serve();
    });

    let mut sinks: Vec<_> = stream::iter(config.sinks.into_iter())
        .then(SinkConfig::into_sink)
        .try_collect()
        .await
        .context("fail to initialize data sink")?;

    let mut interval = interval_at(Instant::now() + Duration::from_secs(5), config.interval);

    let (tx, mut rx) = mpsc::channel(8);
    ctrlc::set_handler(move || {
        if tx.blocking_send(()).is_err() {
            error!("fail to terminate gracefully");
            std::process::abort();
        }
    })
    .context("fail to set signal trap")?;

    'outer: loop {
        tokio::select! {
            _ = interval.tick() => {
                info!("start a polling round");
                let devices = repository
                    .retrieve()
                    .await
                    .context("fail to retrieve the list of devices")?;
                for device in devices.into_iter() {
                    debug!("poll device {} {}", device.name, device.address);
                    let mut retry = config.retry + 1;
                    let message = loop {
                        match hub.poll(&device.address).await {
                            Ok(message) => {
                                break Some(message);
                            }
                            Err(HubError::DeviceNotFound) => {
                                warn!("device {} {} not in range", device.name, device.address);
                                break None;
                            }
                            Err(HubError::NotSupported) => {
                                error!(
                                    "device {} {} is not a valid sensor",
                                    device.name, device.address
                                );
                                break None;
                            }
                            Err(HubError::BackendDead) => {
                                error!("bluetooth backend is dead");
                                break 'outer;
                            }
                            Err(e) if retry == 1 => {
                                error!(
                                    "fail to poll device `{} {}` after {} retries: {}",
                                    device.name, device.address, config.retry, e
                                );
                            }
                            _ => {
                                sleep(Duration::from_millis(1007)).await;
                            },
                        }
                        retry -= 1;
                        if retry == 0 {
                            break None;
                        }
                    };
                    if let Some(message) = message {
                        for sink in sinks.iter_mut() {
                            if let Err(e) = sink.update(&message).await.context("fail to write to sink") {
                                error!("{:?}", e);
                            }
                        }
                    }
                }
            }
            _ = rx.recv() => {
                break;
            }
        }
    }

    drop(hub);
    backend_handle.join().unwrap();
    Ok(())
}
