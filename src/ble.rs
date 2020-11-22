use crate::{
    config::Config,
    message::{Payload, UpdateMessage},
    Error,
};

use std::{
    collections::{hash_map, HashMap},
    fmt::Display,
};

use btleplug::api::{
    BDAddr, Central, CentralEvent, Characteristic, Peripheral, ValueNotification, UUID,
};
#[cfg(target_os = "linux")]
use btleplug::bluez::{adapter::ConnectedAdapter, manager::Manager};
#[cfg(target_os = "macos")]
use btleplug::corebluetooth::{adapter::Adapter, manager::Manager};
#[cfg(target_os = "windows")]
use btleplug::winrtble::{adapter::Adapter, manager::Manager};
use chrono::Local;
use futures::{executor::block_on, prelude::Future};
use log::{debug, error, info, warn};
use rand::Rng;
use tokio::{
    stream::StreamExt,
    sync::{broadcast, mpsc},
    task::{spawn, spawn_blocking},
    time::{interval_at, timeout, Duration, Instant},
};

#[derive(Debug)]
enum BusMessage {
    //Discovered(BDAddr),
    Connected(BDAddr),
    Poll,
    TemperatureUpdate(BDAddr, f32),
    BatteryUpdate(BDAddr, i8),
    RssiUpdate(BDAddr, i16),
    Terminate,
}

#[derive(Debug)]
enum DeviceMessage {
    Poll,
    Connected,
    Terminate,
}

#[derive(Debug, Clone)]
enum DeviceResponse {
    TemperatureSkipped,
    Temperature(f32),
    Battery(i8),
    Rssi(i16),
}

trait LogError {
    fn pipe_log<S: AsRef<str>, Formatter: FnOnce() -> S>(self, f: Formatter) -> Self;
}

impl<T, E: Display> LogError for Result<T, E> {
    fn pipe_log<S: AsRef<str>, Formatter: FnOnce() -> S>(self, f: Formatter) -> Self {
        if let Err(ref e) = self {
            error!("{}: {}", f().as_ref(), e);
        }
        self
    }
}

// 0000ffe1-0000-1000-8000-00805f9b34fb
#[cfg(any(target_os = "windows", target_os = "macos"))]
const UUID_NOTIFY: UUID = UUID::B128([
    0xfb, 0x34, 0x9b, 0x5f, 0x80, 0x00, 0x00, 0x80, 0x00, 0x10, 0x00, 0x00, 0xe1, 0xff, 0x00, 0x00,
]);
#[cfg(target_os = "linux")]
const UUID_NOTIFY: UUID = UUID::B16([0xe1, 0xff]);

#[cfg(any(target_os = "windows", target_os = "macos"))]
fn get_central(manager: &Manager) -> Result<Adapter, Error> {
    let adapters = manager.adapters()?;
    adapters
        .into_iter()
        .nth(0)
        .map_or(Err(Error::NoAdapter), |adapter| Ok(adapter))
}

#[cfg(target_os = "linux")]
fn get_central(manager: &Manager) -> Result<ConnectedAdapter, Error> {
    let adapters = manager.adapters()?;
    let adapter = adapters
        .into_iter()
        .nth(0)
        .map_or(Err(Error::NoAdapter), |adapter| Ok(adapter))?;
    Ok(adapter.connect()?)
}

fn setup_device<P: Peripheral + Display>(
    sender: broadcast::Sender<DeviceResponse>,
    device: &P,
) -> Result<Characteristic, Error> {
    debug!("setting up {}", device);
    let addr = device.address();
    let characteristics = device.discover_characteristics()?;
    let notify_service = characteristics
        .into_iter()
        .find(|c| c.uuid == UUID_NOTIFY)
        .ok_or(Error::NotHMDevice)?;
    device.subscribe(&notify_service)?;
    device.on_notification(Box::new(move |notification: ValueNotification| {
        debug!("received message from {}: {:x?}", addr, notification.value);
        match std::str::from_utf8(&notification.value) {
            Ok(s) => {
                let message = s.trim();
                if message.starts_with("OK+Get:") {
                    let payload = &s[7..];
                    if payload.starts_with('-') {
                        // RSSI
                        if let Ok(rssi) = payload.parse() {
                            let _ = sender.send(DeviceResponse::Rssi(rssi));
                        }
                    } else if payload.contains('.') {
                        // Temperature
                        if payload == "085.00" {
                            // Temperature sensor not initialized, skip polling
                            debug!(
                                "temperature sensor of device {} is not yet initialized",
                                addr
                            );
                            let _ = sender.send(DeviceResponse::TemperatureSkipped);
                        } else if let Ok(temperature) = payload.parse() {
                            let _ = sender.send(DeviceResponse::Temperature(temperature));
                        }
                    } else if let Ok(battery) = payload.parse() {
                        // Battery
                        let _ = sender.send(DeviceResponse::Battery(battery));
                    }
                }
            }
            Err(_) => {
                warn!(
                    "failed to decode message from {}: {:x?}",
                    addr, notification.value
                );
            }
        }
    }));
    debug!("finish setting up {}", device);
    Ok(notify_service)
}

fn poll_device<P: 'static + Peripheral + Display>(
    device: P,
    bus_sender: mpsc::Sender<BusMessage>,
) -> Result<(), Error> {
    async fn wait_command<P: 'static + Peripheral + Display>(
        device: P,
        notify_service: Characteristic,
        bus_sender: mpsc::Sender<BusMessage>,
        receiver: broadcast::Receiver<DeviceResponse>,
    ) -> Result<(), Error> {
        if !device.is_connected() {
            return Err(Error::PreemptDisconnect);
        }

        let address = device.address();

        let mut has_temp = false;
        let mut has_batt = false;
        let mut has_rssi = false;

        tokio::pin! {
            let stream = receiver.into_stream().take_while(Result::is_ok).map(Result::unwrap);
        }

        while let Some(message) = stream.next().await {
            match message {
                DeviceResponse::TemperatureSkipped => {
                    // Ask for temperature again
                    device.command(&notify_service, "AT+TEMP?".as_bytes())?;
                }
                DeviceResponse::Temperature(temperature) => {
                    info!("device {} temperature polled: {}", device, temperature);
                    has_temp = true;
                    let _ = bus_sender
                        .send(BusMessage::TemperatureUpdate(address, temperature))
                        .await;
                }
                DeviceResponse::Battery(battery) => {
                    info!("device {} battery polled: {}", device, battery);
                    has_batt = true;
                    let _ = bus_sender
                        .send(BusMessage::BatteryUpdate(address, battery))
                        .await;
                }
                DeviceResponse::Rssi(rssi) => {
                    info!("device {} rssi polled: {}", device, rssi);
                    has_rssi = true;
                    let _ = bus_sender.send(BusMessage::RssiUpdate(address, rssi)).await;
                }
            }
            if has_temp && has_batt && has_rssi {
                break;
            }
        }
        Ok(())
    }

    debug!("polling {}", device);

    let (value_sender, value_receiver) = broadcast::channel(16);
    let notify_service = setup_device(value_sender, &device)?;

    device.command(&notify_service, "AT+TEMP?".as_bytes())?;
    std::thread::sleep(Duration::from_millis(5));
    device.command(&notify_service, "AT+BATT?".as_bytes())?;
    std::thread::sleep(Duration::from_millis(5));
    device.command(&notify_service, "AT+RSSI?".as_bytes())?;
    // wait for all response, or timeout after 5s
    let timeout_task = timeout(
        Duration::from_secs(5),
        wait_command(device.clone(), notify_service, bus_sender, value_receiver),
    );
    if let Err(_) = block_on(timeout_task) {
        warn!("device {} doesn't finish polling in time", device);
    }
    info!("device {} polled", device);
    device.disconnect()?;

    Ok(())
}

fn device_connect<P: 'static + Peripheral + Display, C: Central<P>>(
    central: &C,
    addr: &BDAddr,
    retry: usize,
) {
    if let Some(device) = central.peripheral(*addr) {
        spawn_blocking(move || {
            debug!("connecting to device {}", device);
            for i in 0..retry + 1 {
                if let Ok(_) = device.connect() {
                    return;
                }
                debug!("{}-th try to connect to {} failed", i, device);
                std::thread::sleep(Duration::from_millis(rand::thread_rng().gen_range(10, 50)));
            }
            error!("failed to connect to device {}", device);
        });
    } else {
        debug!(
            "wanted to perform operation on device {}, not discovered yet",
            addr
        );
    }
}

async fn device_handler<P: 'static + Peripheral + Display, C: Central<P>>(
    central: C,
    addr: BDAddr,
    bus_sender: mpsc::Sender<BusMessage>,
    mut receiver: mpsc::Receiver<DeviceMessage>,
    retry: usize,
) {
    while let Some(message) = receiver.next().await {
        debug!("device {} receives request {:?}", addr, message);
        match message {
            DeviceMessage::Poll => {
                device_connect(&central, &addr, retry);
            }
            DeviceMessage::Connected => {
                if let Some(device) = central.peripheral(addr) {
                    let address = device.address();
                    let bus_sender_2 = bus_sender.clone();
                    spawn_blocking(move || {
                        std::thread::sleep(Duration::from_millis(10));
                        poll_device(device, bus_sender_2)
                            .pipe_log(|| format!("failed to poll device {}", address))
                    });
                } else {
                    warn!("device {} lost before performing operation", addr);
                }
            }
            DeviceMessage::Terminate => {
                break;
            }
        }
    }
}

async fn poll_ble_devices(
    config: &Config,
    sender: mpsc::Sender<UpdateMessage>,
    termination_receiver: broadcast::Receiver<()>,
) -> Result<(), Error> {
    let (bus_sender, bus_receiver) = mpsc::channel(64);
    tokio::pin! {
        let termination_receiver = termination_receiver.into_stream().take_while(Result::is_ok).map(|_| BusMessage::Terminate).take(1);
    }
    let mut bus_receiver = bus_receiver.merge(termination_receiver);

    let manager = Manager::new()?;
    let central = get_central(&manager)?;
    central.active(false);
    let ble_event_receiver = central.event_receiver().unwrap();

    let bus_sender_2 = bus_sender.clone();
    spawn_blocking(move || {
        while let Ok(event) = ble_event_receiver.recv() {
            match event {
                CentralEvent::DeviceDiscovered(addr) => {
                    debug!("device discovered: {}", addr);
                    //let _ = bus_sender_2.blocking_send(BusMessage::Discovered(addr));
                }
                CentralEvent::DeviceConnected(addr) => {
                    debug!("device connected: {}", addr);
                    let _ = bus_sender_2.blocking_send(BusMessage::Connected(addr));
                }
                CentralEvent::DeviceLost(addr) => {
                    debug!("device lost: {}", addr);
                }
                CentralEvent::DeviceDisconnected(addr) => {
                    debug!("device disconnected: {}", addr);
                }
                _ => (),
            }
        }
    });
    central.start_scan()?;

    let mut devices: HashMap<_, _> = config
        .devices
        .keys()
        .map(|addr| {
            let (sender, receiver) = mpsc::channel(16);
            let task = spawn(device_handler(
                central.clone(),
                *addr,
                bus_sender.clone(),
                receiver,
                config.retry,
            ));
            (*addr, (sender, task))
        })
        .collect();

    let bus_sender_2 = bus_sender.clone();
    let poll_period = config.poll_period;
    let _period_poll_task = spawn(async move {
        // Give it 10 seconds to start up scanning
        let mut interval = interval_at(
            Instant::now() + Duration::from_secs(10),
            Duration::from_secs(poll_period),
        );
        loop {
            interval.tick().await;
            let _ = bus_sender_2.send(BusMessage::Poll).await;
        }
    });

    while let Some(message) = bus_receiver.next().await {
        match message {
            BusMessage::Connected(addr) => {
                if let hash_map::Entry::Occupied(entry) = devices.entry(addr) {
                    let (device_sender, _) = entry.get();
                    let _ = device_sender.send(DeviceMessage::Connected).await;
                }
            }
            BusMessage::Poll => {
                // Prune polls that exits
                debug!("issuing poll request to all devices");
                devices.retain(|_, (sender, _)| !sender.is_closed());
                for (_, (device_sender, _)) in devices.iter() {
                    let _ = device_sender.send(DeviceMessage::Poll).await;
                }
            }
            BusMessage::TemperatureUpdate(device, temperature) => {
                let _ = sender
                    .send(UpdateMessage {
                        timestamp: Local::now(),
                        device,
                        value: Payload::Temperature(temperature),
                    })
                    .await;
            }
            BusMessage::BatteryUpdate(device, battery) => {
                let _ = sender
                    .send(UpdateMessage {
                        timestamp: Local::now(),
                        device,
                        value: Payload::Battery(battery),
                    })
                    .await;
            }
            BusMessage::RssiUpdate(device, rssi) => {
                let _ = sender
                    .send(UpdateMessage {
                        timestamp: Local::now(),
                        device,
                        value: Payload::Rssi(rssi),
                    })
                    .await;
            }
            BusMessage::Terminate => {
                debug!("terminate message received");
                // Prune polls that exits
                for (_, (device_sender, _)) in devices.iter() {
                    let _ = device_sender.send(DeviceMessage::Terminate).await;
                }
                break;
            }
        }
    }

    // Give 1s timeout before forcifully drop all the tasks
    if let Err(_) = timeout(
        Duration::from_secs(1),
        spawn(async {
            for (_, (_, task)) in devices {
                let _ = task.await;
            }
        }),
    )
    .await
    {
        warn!("device handlers didn't finish in time");
    }
    central.stop_scan()?;
    Ok(())
}

pub(crate) fn create_ble_source<'a>(
    config: &'a Config,
    termination_receiver: broadcast::Receiver<()>,
) -> Result<
    (
        mpsc::Receiver<UpdateMessage>,
        impl Future<Output = Result<(), Error>> + 'a,
    ),
    Error,
> {
    let (sender, receiver) = mpsc::channel(64);
    Ok((
        receiver,
        poll_ble_devices(config, sender, termination_receiver),
    ))
}
