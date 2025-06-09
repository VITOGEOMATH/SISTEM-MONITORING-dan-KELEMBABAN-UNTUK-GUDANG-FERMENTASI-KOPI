![alt text](https://github.com/VITOGEOMATH/SISTEM-MONITORING-dan-KELEMBABAN-UNTUK-GUDANG-FERMENTASI-KOPI/blob/main/Antarmuka%20QT.png?raw=true)
# Sistem Monitoring Suhu dan Kelembaban Untuk Gudang Fermentasi Kopi

## Nama Kelompok : 8

1. **Supervisor** : Ahmad Radhy S.Si. M.Si.  
2. Ahmad Rafli Al Adzani (2042231001)  
3. Muhammad Jidan Baraja (2042231009)  
4. Muhammad Rif'al Faiz Arivito (2042231067)

---

## 📦 Instalasi

### 1. Clone Repository

```bash
git clone https://github.com/VITOGEOMATH/SISTEM-MONITORING-dan-KELEMBABAN-UNTUK-GUDANG-FERMENTASI-KOPI
```

### 2. Install Python Dependencies

```bash
pip install tokio-modbus tokio-serial matplotlib requests chrono influxdb-client tkinter serde reqwest
```

### 3. Siapkan InfluxDB

- Jalankan atau instal **InfluxDB v2** di localhost.
- Buat konfigurasi berikut:
  - **Organization**
  - **Bucket**
  - **Token**
- Catat nama bucket dan token untuk konfigurasi aplikasi.

### 4. Konfigurasi Sensor Modbus RTU

- Hubungkan sensor melalui USB ke komputer (gunakan USB-to-RS485 converter).
- Pastikan:
  - Sensor terdeteksi sebagai port serial (misalnya `/dev/ttyUSB0` atau `COM3`)
  - Sensor dapat membaca data dari:
    - **Input Register 1** → Suhu (°C)
    - **Input Register 2** → Kelembaban (%RH)

### 5. Jalankan Server TCP (Rust)

```bash
cargo run --bin tcp_server
# atau
cargo build && ./target/debug/tcp_server
```

### 6. Jalankan Client Pembaca Sensor (Rust)

```bash
cargo run --bin modbus_client
```

---

## 🖥️ Penggunaan Aplikasi GUI (Python)

Jalankan antarmuka monitoring real-time berbasis **Tkinter**:

```bash
python gui.py
```

### 🔧 Fitur GUI:

- ✅ Menampilkan data suhu dan kelembaban secara **real-time** dari **InfluxDB**.
- 📈 Menampilkan **grafik historis** berdasarkan input waktu pengguna.
- ⏰ Input waktu menggunakan format **RFC3339**  
  Contoh: `2025-05-30T08:00:00Z`

---

## 📁 Struktur Proyek

```
proyek-monitoring-sensor/
├── tcp_server.rs         # Server TCP (menggunakan Tokio dan Reqwest)
├── modbus_client.rs      # Client pembaca sensor (Modbus RTU)
├── gui.py                # GUI Python (Tkinter + Matplotlib)
├── Cargo.toml            # Konfigurasi proyek Rust
└── README.md             # Dokumentasi proyek
```

---

## 🚀 Perbaikan & Pengembangan Selanjutnya

- ✅ Tambahkan fitur logging untuk menyimpan semua data lokal.
- ✅ Tambahkan validasi waktu input di GUI.
- 🔒 Tambahkan autentikasi dasar di TCP server.
- 📄 Tambahkan fitur export CSV dari data historis.
- 🌐 Tambahkan opsi konfigurasi melalui file `.env` atau GUI.

---

## 📃 Lisensi

Proyek ini hanya digunakan untuk kebutuhan tugas akhir/akademik.  
Kontak supervisor atau anggota kelompok untuk penggunaan lebih lanjut.
