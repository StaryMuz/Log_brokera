import csv
import time
import glob
import os
from datetime import datetime, timedelta
import pytz
import paho.mqtt.client as mqtt

# ====== NAČTENÍ SECRETŮ (POVINNÉ) ======

MQTT_BROKER = os.environ["MQTT_HOST"]
MQTT_USER   = os.environ["MQTT_USER"]
MQTT_PASS   = os.environ["MQTT_PASS"]

MQTT_PORT  = 1883
MQTT_TOPIC = "starymuz@centrum.cz/rele/1/set"

TIMEZONE = pytz.timezone("Europe/Prague")

RUN_LOG    = f"mqtt_log_{datetime.now(TIMEZONE).strftime('%Y-%m-%d_%H-%M-%S')}.csv"
MERGED_LOG = "mqtt_log_24h.csv"

# ====== CALLBACKY ======

def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        print("Připojeno k MQTT brokeru")
        client.subscribe(MQTT_TOPIC)
    else:
        print(f"Chyba připojení: {reason_code}")

def on_message(client, userdata, msg):
    payload = msg.payload.decode(errors="ignore").strip()

    if payload not in ("0", "1"):
        return

    timestamp = datetime.now(TIMEZONE)

    with open(RUN_LOG, "a", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow([
            timestamp.strftime("%Y-%m-%d %H:%M:%S"),
            msg.topic,
            payload
        ])

    print(f"{timestamp} | {msg.topic} | {payload}")

# ====== SLOUČENÍ POSLEDNÍCH 24 H ======

def merge_last_24h():
    cutoff = datetime.now(TIMEZONE) - timedelta(hours=24)
    rows = []

    for file in glob.glob("mqtt_log_*.csv"):
        if file == MERGED_LOG:
            continue

        with open(file, "r", encoding="utf-8") as f:
            reader = csv.reader(f, delimiter=";")
            for row in reader:
                if not row or row[0] == "čas":
                    continue

                ts = TIMEZONE.localize(
                    datetime.strptime(row[0], "%Y-%m-%d %H:%M:%S")
                )
                if ts >= cutoff:
                    rows.append(row)

    rows.sort(key=lambda r: r[0])

    with open(MERGED_LOG, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(["čas", "topic", "hodnota"])
        writer.writerows(rows)

# ====== HLAVNÍ ======

def main():
    # vytvoření CSV pro tento run
    with open(RUN_LOG, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f, delimiter=";")
        writer.writerow(["čas", "topic", "hodnota"])

    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2
    )
    client.username_pw_set(MQTT_USER, MQTT_PASS)
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)
    client.loop_start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        client.loop_stop()
        client.disconnect()
        merge_last_24h()
        print("Logger ukončen, CSV sloučeno")

if __name__ == "__main__":
    main()
