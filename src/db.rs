use crate::message::{Payload, UpdateMessage};
use crate::Error;

use eui48::MacAddress;
use log::{debug, info};

use tokio::{
    spawn,
    sync::{broadcast, mpsc},
};
use tokio_postgres::{connect, types::Type as SQLType, Client, NoTls, Statement};
use tokio_stream::StreamExt;

const TEMPERATURE_TNAME: &str = "temperature";
const BATTERY_TNAME: &str = "battery";
const RSSI_TNAME: &str = "rssi";

#[derive(Debug)]
enum Message {
    Update(UpdateMessage),
    Terminate,
}

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
    mut termination_receiver: broadcast::Receiver<()>,
) -> Result<(), Error> {
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

    let temperature_sql = prepare_insert_sql(&client, TEMPERATURE_TNAME, SQLType::FLOAT4).await?;
    let battery_sql = prepare_insert_sql(&client, BATTERY_TNAME, SQLType::INT2).await?;
    let rssi_sql = prepare_insert_sql(&client, RSSI_TNAME, SQLType::INT2).await?;

    let termination_receiver = async_stream::stream! {
        if let Ok(_) = termination_receiver.recv().await {
            yield Message::Terminate;
        }
    };
    let source = async_stream::stream! {
        while let Some(item) = source.recv().await {
            yield item;
        }
    };
    let source = source.map(Message::Update).merge(termination_receiver);
    tokio::pin!(source);
    while let Some(message) = source.next().await {
        match message {
            Message::Update(message) => {
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
            }
            Message::Terminate => {
                break;
            }
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
    termination_receiver: broadcast::Receiver<()>,
) -> Result<(), Error> {
    debug!("connecting to database");
    let (client, connection) = connect(&db_str, NoTls).await?;
    let connection_handle = spawn(async move { connection.await });
    info!("connected to database");

    tokio::select! {
        r = connection_handle => { Ok(r.unwrap()?) },
        r = transfer_to_db(client, source, termination_receiver) => { r }
    }
}
