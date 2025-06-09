use tokio::net::TcpListener;
use tokio::io::{AsyncBufReadExt, BufReader};
use serde::Deserialize;
use reqwest::Client;
use chrono::{Utc, DateTime};
use tokio::time::{sleep, Duration};
use std::error::Error;

#[derive(Deserialize, Debug)]
struct SensorData {
    timestamp: String,
    sensor_id: String,
    location: String,
    process_stage: String,
    temperature_celsius: f32,
    humidity_percent: f32,
}

#[tokio::main]
async fn main() {
    // Loop utama untuk restart otomatis jika terjadi error
    loop {
        if let Err(e) = run_server().await {
            eprintln!("❌ Server error: {} – akan restart dalam 5 detik", e);
            sleep(Duration::from_secs(5)).await;
        }
    }
}

async fn run_server() -> Result<(), Box<dyn Error>> {
    let listener = TcpListener::bind("0.0.0.0:9001").await?;
    let influx_url = "http://localhost:8086/api/v2/write?org=its&bucket=ISI_KOPI&precision=s";
    let token = "U9HGTE1RSCjrPIHWOCyAev64B44duwZ63TaOpFe6Nw0KpAUCVnQ4yHfRVlyJTeVA38r2FRqNZPR5DbMhXkvbiw==";
    let client = Client::new();

    println!("🚪 TCP Server listening on port 9001...");

    loop {
        let (socket, addr) = listener.accept().await?;
        println!("🔌 Koneksi masuk dari {}", addr);

        let client = client.clone();
        let influx_url = influx_url.to_string();
        let token = token.to_string();

        tokio::spawn(async move {
            let reader = BufReader::new(socket);
            let mut lines = reader.lines();

            while let Ok(Some(line)) = lines.next_line().await {
                match serde_json::from_str::<SensorData>(&line) {
                    Ok(data) => {
                        println!("📥 Data diterima: {:?}", data);

                        // Parse timestamp dengan aman
                        let ts = match DateTime::parse_from_rfc3339(&data.timestamp) {
                            Ok(dt) => dt.timestamp(),
                            Err(err) => {
                                eprintln!("⚠️ Gagal parse timestamp '{}': {}. Menggunakan Utc::now()", data.timestamp, err);
                                Utc::now().timestamp()
                            }
                        };

                        // Line Protocol format
                        let line_proto = format!(
                            "monitoring,sensor_id={},location={},stage={} temperature={},humidity={} {}",
                            data.sensor_id.replace(' ', "\\ "),
                            data.location.replace(' ', "\\ "),
                            data.process_stage.replace(' ', "\\ "),
                            data.temperature_celsius,
                            data.humidity_percent,
                            ts
                        );

                        // Kirim ke InfluxDB
                        match client.post(&influx_url)
                            .header("Authorization", format!("Token {}", token))
                            .header("Content-Type", "text/plain")
                            .body(line_proto)
                            .send()
                            .await
                        {
                            Ok(resp) if resp.status().is_success() => {
                                println!("✅ Data dikirim ke InfluxDB");
                            }
                            Ok(resp) => {
                                println!("⚠️ Gagal kirim ke InfluxDB: {}", resp.status());
                            }
                            Err(e) => {
                                println!("❌ HTTP Error: {}", e);
                            }
                        }
                    }
                    Err(e) => eprintln!("❌ Format JSON tidak valid: {}", e),
                }
            }
        });
    }
}
