use tokio_modbus::{client::rtu, prelude::*};
use tokio_serial::{SerialPortBuilderExt, Parity, StopBits, DataBits};
use tokio::net::TcpStream;
use tokio::io::AsyncWriteExt;
use chrono::Utc;
use serde::Serialize;
use influxdb2::{Client, models::DataPoint};
use std::error::Error;
use tokio_stream::iter;
use tokio::time::{sleep, Duration};
use std::path::Path;

#[derive(Serialize)]
struct SensorData {
    timestamp: String,
    sensor_id: String,
    location: String,
    process_stage: String,
    temperature_celsius: f32,
    humidity_percent: f32,
}

async fn read_sensor(slave: u8) -> Result<Vec<u16>, Box<dyn Error>> {
    let serial_path = "/dev/ttyUSB0";

    // ✅ Cek apakah device serial tersedia
    if !Path::new(serial_path).exists() {
        return Err(format!("Device {} tidak ditemukan", serial_path).into());
    }

    let builder = tokio_serial::new(serial_path, 9600)
        .parity(Parity::None)
        .stop_bits(StopBits::One)
        .data_bits(DataBits::Eight)
        .timeout(std::time::Duration::from_secs(1));

    let port = builder.open_native_async()?;
    let mut ctx = rtu::connect_slave(port, Slave(slave)).await?;
    let response = ctx.read_input_registers(1, 2).await?;

    Ok(response)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    println!("🚀 Program dimulai...");

    let org = "its";
    let bucket = "ISI_KOPI";
    let token = "U9HGTE1RSCjrPIHWOCyAev64B44duwZ63TaOpFe6Nw0KpAUCVnQ4yHfRVlyJTeVA38r2FRqNZPR5DbMhXkvbiw==";
    let influx_url = "http://localhost:8086";

    let client = Client::new(influx_url, org, token);

    loop {
        match read_sensor(1).await {
            Ok(response) if response.len() == 2 => {
                let temp = response[0] as f64 / 10.0;
                let rh = response[1] as f64 / 10.0;

                println!("📡 Temp: {:.1} °C | RH: {:.1} %", temp, rh);

                let data = SensorData {
                    timestamp: Utc::now().to_rfc3339(),
                    sensor_id: "SHT20-PascaPanen-001".into(),
                    location: "Gudang Fermentasi 1".into(),
                    process_stage: "Fermentasi".into(),
                    temperature_celsius: temp as f32,
                    humidity_percent: rh as f32,
                };

                // Kirim ke TCP server
                let json = serde_json::to_string(&data)?;
                match TcpStream::connect("127.0.0.1:9001").await {
                    Ok(mut stream) => {
                        stream.write_all(json.as_bytes()).await?;
                        stream.write_all(b"\n").await?;
                        println!("✅ Data dikirim ke TCP server");
                    }
                    Err(e) => {
                        println!("❌ Gagal konek ke TCP server: {}", e);
                    }
                }

                // Kirim ke InfluxDB v2
                let timestamp = Utc::now().timestamp_nanos_opt().unwrap_or_else(|| {
                    eprintln!("⚠️ Gagal mendapatkan timestamp, fallback ke 0");
                    0
                });

                let point = DataPoint::builder("fermentasi_sensor")
                    .tag("sensor_id", &data.sensor_id)
                    .tag("location", &data.location)
                    .tag("stage", &data.process_stage)
                    .field("temperature_celsius", data.temperature_celsius as f64)
                    .field("humidity_percent", data.humidity_percent as f64)
                    .timestamp(timestamp)
                    .build()?;

                let points = iter(vec![point]);

                match client.write(bucket, points).await {
                    Ok(_) => println!("✅ Data dikirim ke InfluxDB v2"),
                    Err(e) => println!("❌ Gagal kirim ke InfluxDB v2: {}", e),
                }
            }
            Ok(other) => println!("⚠️ Data tidak lengkap: {:?}", other),
            Err(e) => println!("❌ Gagal baca sensor: {}", e),
        }

        sleep(Duration::from_secs(2)).await;
    }
}

