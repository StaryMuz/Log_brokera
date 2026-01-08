import csv
import os
import time
from datetime import datetime, timedelta
import pytz
import glob
import paho.mqtt.client as mqtt

# === MQTT nastavení ===
HOST  = os.environ["MQTT_HOST"]
PORT  = int(os.environ["MQTT_PORT"])
USER  = os.environ["MQTT_USER"]
PASS  = os.environ["MQTT_PASS"]
TOPIC = os.environ["MQTT_TOPIC"]

# === Lokální časová zóna ===
LOCAL_TZ = pytz.timezone("Europe/Prague")

# === Logování ===
RUN_LOG = f"mqtt_log_{int(time.time())}.csv"  # dočasný log pro aktuální run
MERGED_LOG = "mqtt_log_24h.csv"              # sloučený 24h log

# === Délka runu v sekundách ===
END_TIME = time.time() + 55 * 60  # cca 55 minut

# --- MQTT callbacky ---
def on_connect(client, userdata, flags, rc):
    client.subscribe(TOPIC)

def on_message(client, userdata, msg):
    payload = msg.payload.decode(errors="replace").strip()
    if payload not in ("0", "1"):
        return

    local_time = datetime.now(LOCAL_TZ).strftime("%Y-%m-%d %H:%M:%S")
    with open(RUN_LOG, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([local_time, msg.topic, payload])

# --- MQTT client setup ---
client = mqtt.Client()
client.username_pw_set(USER, PASS)
client.on_connect = on_connect
client.on_message = on_message

client.connect(HOST, PORT, 60)

# --- Hlavička CSV pro aktuální run ---
if not os.path.exists(RUN_LOG):
    with open(RUN_LOG, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["local_time", "topic", "value"])

client.loop_start()

try:
    while time.time() < END_TIME:
        time.sleep(1)
finally:
    client.loop_stop()
    client.disconnect()

# --- Sloučení do 24hodinového CSV ---
# vybere všechny soubory mqtt_log_*.csv z posledních 24h
now = datetime.now(LOCAL_TZ)
start_window = now - timedelta(hours=24)

files = glob.glob("mqtt_log_*.csv")
all_rows = []

for file in files:
    try:
        with open(file, newline="", encoding="utf-8") as f:
            reader = csv.reader(f)
            header = next(reader, None)  # přeskočí hlavičku
            for row in reader:
                if len(row) < 3:
                    continue
                row_time = datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S")
                row_time = LOCAL_TZ.localize(row_time)
                if row_time >= start_window:
                    all_rows.append(row)
    except Exception as e:
        print(f"Chyba při čtení {file}: {e}")

# seřadit podle času
all_rows.sort(key=lambda r: r[0])

# zapsat do sloučeného logu
with open(MERGED_LOG, "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(["local_time", "topic", "value"])
    writer.writerows(all_rows)
