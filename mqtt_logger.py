import csv
import time
import glob
import os
from datetime import datetime, timedelta
import pytz
import paho.mqtt.client as mqtt

# ====== SECRETS ======
MQTT_BROKER = os.environ["MQTT_HOST"]
MQTT_USER   = os.environ["MQTT_USER"]
MQTT_PASS   = os.environ["MQTT_PASS"]

MQTT_PORT  = 1883
MQTT_TOPIC = "starymuz@centrum.cz/rele/1/set"

TIMEZONE = pytz.timezone("Europe/Prague")

RUN_LOG    = f"mqtt_log_{datetime.now(TIMEZONE).strftime('%Y-%m-%d_%H-%M-%S')}.csv"
MERGED_LOG = "mqtt_log_24h.csv"

SAFETY_SECONDS = 60   # rezerva před další hodinou

# ====== CALLBACKY ======
def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        client.subscribe(MQTT_TOPIC)
        print("Připojeno k MQTT brokeru")
    else:
        print(f"Chyba připojení: {reason_code}")

def on_message(client, userdata, msg):
    # ignorujeme retained zprávy (stav brokeru)
    if msg.retain:
        return

    payload = msg.payload.decode(errors="ignore").strip()
    if payload not in ("0", "1"):
        return

    ts = datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M:%S")
    with open(RUN_LOG, "a", newline="", encoding="utf-8") as f:
        csv.writer(f, delimiter=";").writerow([ts, msg.topic, payload])

# ====== ČASOVÁ LOGIKA ======
def seconds_until_hour_end():
    now = datetime.now(TIMEZONE)
    next_hour = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
    return max(0, int((next_hour - now).total_seconds()) - SAFETY_SECONDS)

# ====== SLOUČENÍ ======
def merge_last_24h():
    cutoff = datetime.now(TIMEZONE) - timedelta(hours=24)
    rows = []

    for file in glob.glob("mqtt_log_*.csv"):
        if file == MERGED_LOG:
            continue
        with open(file, encoding="utf-8") as f:
            for r in csv.reader(f, delimiter=";"):
                if not r or r[0] == "čas":
                    continue
                ts = TIMEZONE.localize(datetime.strptime(r[0], "%Y-%m-%d %H:%M:%S"))
                if ts >= cutoff:
                    rows.append(r)

    rows.sort(key=lambda r: r[0])

    with open(MERGED_LOG, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f, delimiter=";")
        w.writerow(["čas", "topic", "hodnota"])
        w.writerows(rows)

# ====== MAIN ======
def main():
    # vytvoření CSV pro tento run
    with open(RUN_LOG, "w", newline="", encoding="utf-8") as f:
        csv.writer(f, delimiter=";").writerow(["čas", "topic", "hodnota"])

    run_seconds = seconds_until_hour_end()
    print(f"Poběžím {run_seconds} sekund")

    if run_seconds <= 0:
        print("Běžící hodina téměř končí – ukončuji run")
        return

    client = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    client.username_pw_set(MQTT_USER, MQTT_PASS)
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.loop_start()

    time.sleep(run_seconds)

    client.loop_stop()
    client.disconnect()

    merge_last_24h()
    print("Run korektně ukončen a CSV sloučeno")

if __name__ == "__main__":
    main()
