import sys
import socket
import threading
import json
from PyQt5.QtWidgets import (
    QApplication, QWidget, QVBoxLayout, QHBoxLayout, QLabel,
    QTableWidget, QTableWidgetItem, QPushButton, QGroupBox
)
from PyQt5.QtCore import Qt, pyqtSlot, QMetaObject, Q_ARG
from PyQt5.QtGui import QFont, QColor, QPalette
import pyqtgraph as pg


class SensorMonitor(QWidget):
    def __init__(self):
        super().__init__()
        self.setWindowTitle("📊 Monitoring Sensor Fermentasi Kopi - Kelompok 8")
        self.setGeometry(100, 100, 1000, 600)

        self.counter = 0
        self.time_data = []
        self.temp_data = []
        self.hum_data = []
        self.tcp_thread = None
        self.server_running = False

        self.set_dark_theme()

        # --- Layout Utama ---
        main_layout = QVBoxLayout()

        # --- Header ---
        title = QLabel("📡 Live Monitoring Sensor Fermentasi Kopi")
        title.setStyleSheet("font-size: 18px; font-weight: bold; color: white;")
        subtitle = QLabel("👥 Kelompok 8: Ahmad Rafli Al Adzani, Muhammad Ri'fal Faiz Arivito, Muhammad Jidan Baraja")
        subtitle.setStyleSheet("color: white;")
        main_layout.addWidget(title)
        main_layout.addWidget(subtitle)

        # --- Tombol Start/Stop Server ---
        btn_layout = QHBoxLayout()
        self.start_btn = QPushButton("▶️ Mulai Server")
        self.start_btn.setStyleSheet("background-color: green; color: white; font-size: 14px; padding: 10px;")
        self.stop_btn = QPushButton("⏹️ Hentikan Server")
        self.stop_btn.setStyleSheet("background-color: red; color: white; font-size: 14px; padding: 10px;")
        self.start_btn.clicked.connect(self.start_server)
        self.stop_btn.clicked.connect(self.stop_server)
        btn_layout.addWidget(self.start_btn)
        btn_layout.addWidget(self.stop_btn)
        main_layout.addLayout(btn_layout)

        # --- Tabel Data ---
        self.table = QTableWidget(0, 6)
        self.table.setHorizontalHeaderLabels([
            "Waktu", "Sensor ID", "Lokasi", "Tahap Proses", "Suhu (°C)", "Kelembaban (%)"
        ])
        self.table.setStyleSheet("color: white; background-color: #2b2b2b;")
        main_layout.addWidget(self.table)

        # --- Grafik + Realtime Display ---
        bottom_layout = QHBoxLayout()

        # Grafik
        self.plot = pg.PlotWidget(title="📈 Grafik Suhu & Kelembaban")
        self.plot.setBackground('k')
        self.plot.addLegend()
        self.plot.showGrid(x=True, y=True)
        self.temp_plot = self.plot.plot(pen=pg.mkPen('r', width=2), name="Suhu (°C)")
        self.hum_plot = self.plot.plot(pen=pg.mkPen('c', width=2), name="Kelembaban (%)")
        bottom_layout.addWidget(self.plot, stretch=3)

        # Display Realtime Angka
        display_box = QGroupBox("📌 Nilai Sensor Saat Ini")
        display_box.setStyleSheet("color: white;")
        display_layout = QVBoxLayout()
        self.temp_label = QLabel("Suhu: 0°C")
        self.temp_label.setFont(QFont("Arial", 16))
        self.hum_label = QLabel("Kelembaban: 0%")
        self.hum_label.setFont(QFont("Arial", 16))
        display_layout.addWidget(self.temp_label)
        display_layout.addWidget(self.hum_label)
        display_box.setLayout(display_layout)
        bottom_layout.addWidget(display_box, stretch=1)

        main_layout.addLayout(bottom_layout)
        self.setLayout(main_layout)

    def set_dark_theme(self):
        """Terapkan tema gelap untuk GUI."""
        dark_palette = QPalette()
        dark_palette.setColor(QPalette.Window, QColor(30, 30, 30))
        dark_palette.setColor(QPalette.WindowText, Qt.white)
        dark_palette.setColor(QPalette.Base, QColor(45, 45, 45))
        dark_palette.setColor(QPalette.AlternateBase, QColor(60, 60, 60))
        dark_palette.setColor(QPalette.ToolTipBase, Qt.white)
        dark_palette.setColor(QPalette.ToolTipText, Qt.white)
        dark_palette.setColor(QPalette.Text, Qt.white)
        dark_palette.setColor(QPalette.Button, QColor(53, 53, 53))
        dark_palette.setColor(QPalette.ButtonText, Qt.white)
        dark_palette.setColor(QPalette.BrightText, Qt.red)
        self.setPalette(dark_palette)

    # --- Start/Stop Server ---
    def start_server(self):
        if not self.server_running:
            self.server_running = True
            self.tcp_thread = threading.Thread(target=self.listen_tcp, daemon=True)
            self.tcp_thread.start()
            print("✅ Server dimulai.")

    def stop_server(self):
        self.server_running = False
        print("🛑 Server dihentikan.")

    # --- TCP Server ---
    def listen_tcp(self):
        host = '0.0.0.0'
        port = 9001
        server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_sock.bind((host, port))
        server_sock.listen(5)
        print(f"👂 Menunggu koneksi pada port {port}...")

        while self.server_running:
            try:
                server_sock.settimeout(1.0)
                conn, addr = server_sock.accept()
                threading.Thread(target=self.handle_client, args=(conn,), daemon=True).start()
            except socket.timeout:
                continue

        server_sock.close()

    def handle_client(self, conn):
        with conn:
            buffer = ""
            while True:
                try:
                    data = conn.recv(1024)
                    if not data:
                        break
                    buffer += data.decode('utf-8')
                    while '\n' in buffer:
                        line, buffer = buffer.split('\n', 1)
                        QMetaObject.invokeMethod(self, "update_data_safe", Qt.QueuedConnection, Q_ARG(str, line))
                except Exception as e:
                    print(f"❌ Client error: {e}")
                    break

    @pyqtSlot(str)
    def update_data_safe(self, json_str):
        try:
            print("📥 Data diterima:", json_str)
            data = json.loads(json_str)

            timestamp = data.get("timestamp", "")
            sensor_id = data.get("sensor_id", "")
            location = data.get("location", "")
            stage = data.get("process_stage", "")
            temp = float(data.get("temperature_celsius", 0))
            humidity = float(data.get("humidity_percent", 0))

            # Update tabel
            row = self.table.rowCount()
            self.table.insertRow(row)
            self.table.setItem(row, 0, QTableWidgetItem(timestamp))
            self.table.setItem(row, 1, QTableWidgetItem(sensor_id))
            self.table.setItem(row, 2, QTableWidgetItem(location))
            self.table.setItem(row, 3, QTableWidgetItem(stage))
            self.table.setItem(row, 4, QTableWidgetItem(f"{temp:.1f}"))
            self.table.setItem(row, 5, QTableWidgetItem(f"{humidity:.1f}"))

            # Update grafik
            self.counter += 1
            self.time_data.append(self.counter)
            self.temp_data.append(temp)
            self.hum_data.append(humidity)
            self.temp_plot.setData(self.time_data[-100:], self.temp_data[-100:])
            self.hum_plot.setData(self.time_data[-100:], self.hum_data[-100:])
            self.plot.setXRange(max(0, self.counter - 100), self.counter)

            # Update angka suhu/kelembaban
            self.temp_label.setText(f"Suhu: {temp:.1f}°C")
            self.hum_label.setText(f"Kelembaban: {humidity:.1f}%")

        except Exception as e:
            print(f"❌ Gagal memproses data: {e}")


if __name__ == "__main__":
    app = QApplication(sys.argv)
    window = SensorMonitor()
    window.show()
    sys.exit(app.exec_())
