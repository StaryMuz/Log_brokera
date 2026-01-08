import csv
import os
import time
from datetime import datetime
import paho.mqtt.client as mqtt

HOST  = os.environ["MQTT_HOST"]
PORT  = int(os.environ["MQTT_PORT"])
USER  = os.environ["MQTT_USER"]
PASS  = os.environ["MQTT_PASS"]
TOPIC = os.environ["MQTT_TOPIC"]

LOG_FILE = "mqtt_log.csv"
END_TIME = time.time() + 55 * 60  # cca 55 minut

def on_connect(client, userdata, flags, rc):
    client.subscribe(TOPIC)

def on_message(client, userdata, msg):
    payload = msg.payload.decode(errors="replace").strip()

    # logujeme pouze 0 / 1
    if payload not in ("0", "1"):
        return

    with open(LOG_FILE, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow([
            datetime.utcnow().isoformat(),
            msg.topic,
            payload
        ])

client = mqtt.Client()
client.username_pw_set(USER, PASS)
client.on_connect = on_connect
client.on_message = on_message

client.connect(HOST, PORT, 60)

if not os.path.exists(LOG_FILE):
    with open(LOG_FILE, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["timestamp_utc", "topic", "value"])

client.loop_start()

try:
    while time.time() < END_TIME:
        time.sleep(1)
finally:
    client.loop_stop()
    client.disconnect()
