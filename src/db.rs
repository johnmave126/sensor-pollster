use crate::message::{Payload, UpdateMessage};
use crate::Error;

use eui48::MacAddress;
use log::{debug, info};
use std::sync::{
    atomic::{AtomicBool, Ordering as AtomicOrdering},
    Arc,
};
use tokio::{spawn, stream::StreamExt, sync::mpsc};
use tokio_postgres::{connect, types::Type as SQLType, Client, NoTls, Statement};

const TEMPERATURE_TNAME: &str = "temperature";
const BATTERY_TNAME: &str = "battery";
const RSSI_TNAME: &str = "rssi";

async fn prepare_insert_sql(
    client: &Client,
    tname: &str,
    type_: SQLType,
) -> Result<Statement, tokio_postgres::Error> {
    let sql = format!(
        "INSERT INTO {} (device, timestamp, value) VALUES ($1, $2, $3)",
        tname
    );
    client
        .prepare_typed(&sql, &[SQLType::MACADDR, SQLType::TIMESTAMPTZ, type_])
        .await
}

async fn transfer_to_db(
    client: Client,
    mut source: mpsc::Receiver<UpdateMessage>,
    is_running: Arc<AtomicBool>,
) -> Result<(), Error> {
    let temperature_sql = prepare_insert_sql(&client, TEMPERATURE_TNAME, SQLType::FLOAT4).await?;
    let battery_sql = prepare_insert_sql(&client, BATTERY_TNAME, SQLType::INT2).await?;
    let rssi_sql = prepare_insert_sql(&client, RSSI_TNAME, SQLType::INT2).await?;
    while is_running.load(AtomicOrdering::SeqCst) {
        if let Some(message) = source.next().await {
            debug!("transferring data {:?} to database", message);
            let mut raw_addr = message.device.address.clone();
            raw_addr.reverse();
            match message.value {
                Payload::Temperature(v) => {
                    client
                        .execute(
                            &temperature_sql,
                            &[&MacAddress::new(raw_addr), &message.timestamp, &v],
                        )
                        .await?;
                }
                Payload::Battery(v) => {
                    client
                        .execute(
                            &battery_sql,
                            &[&MacAddress::new(raw_addr), &message.timestamp, &(v as i16)],
                        )
                        .await?;
                }
                Payload::Rssi(v) => {
                    client
                        .execute(
                            &rssi_sql,
                            &[&MacAddress::new(raw_addr), &message.timestamp, &v],
                        )
                        .await?;
                }
            }
        } else {
            break;
        }
    }
    Ok(())
}

fn init_table_sql(tname: &str, type_: &str) -> String {
    format!("
        CREATE TABLE IF NOT EXISTS {tname} (
            device      MACADDR NOT NULL,
            timestamp   TIMESTAMPTZ NOT NULL,
            value       {type} NOT NULL
        );
        CREATE INDEX IF NOT EXISTS device_timestamp ON {tname} (device, timestamp);
        CREATE INDEX IF NOT EXISTS timestamp_device ON {tname} (timestamp, device);
    ", tname = tname, type = type_)
}

pub(crate) async fn create_db_sink(
    db_str: String,
    source: mpsc::Receiver<UpdateMessage>,
    is_running: Arc<AtomicBool>,
) -> Result<(), Error> {
    debug!("connecting to database");
    let (client, connection) = connect(&db_str, NoTls).await?;
    let connection_handle = spawn(async move { connection.await });
    info!("connected to database");
    client
        .batch_execute(&init_table_sql(TEMPERATURE_TNAME, "REAL"))
        .await?;
    client
        .batch_execute(&init_table_sql(BATTERY_TNAME, "SMALLINT"))
        .await?;
    client
        .batch_execute(&init_table_sql(RSSI_TNAME, "SMALLINT"))
        .await?;
    info!("database initialized");

    tokio::select! {
        r = connection_handle => { Ok(r.unwrap()?) },
        r = transfer_to_db(client, source, is_running) => { r }
    }
}
