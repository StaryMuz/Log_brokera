import csv
import time
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
SAFETY_SECONDS = 60

TODAY_FILE = "mqtt_log_dnes.csv"

# ====== SOUBOROVÁ LOGIKA ======
def rotate_logs_if_needed():
    now = datetime.now(TIMEZONE)
    today_str = now.strftime("%Y-%m-%d")
    yesterday_str = (now - timedelta(days=1)).strftime("%Y-%m-%d")

    yesterday_file = f"mqtt_log_{yesterday_str}.csv"

    # Přesun dnešního souboru při změně dne
    if os.path.exists(TODAY_FILE):
        mtime = datetime.fromtimestamp(os.path.getmtime(TODAY_FILE), TIMEZONE)
        if mtime.strftime("%Y-%m-%d") != today_str:
            os.replace(TODAY_FILE, f"mqtt_log_{mtime.strftime('%Y-%m-%d')}.csv")

    # WHITELIST – pouze tyto CSV smí zůstat
    allowed = {TODAY_FILE, yesterday_file}

    # Smazání všech ostatních CSV
    for f in os.listdir("."):
        if f.endswith(".csv") and f not in allowed:
            try:
                os.remove(f)
            except:
                pass

def ensure_today_header():
    if not os.path.exists(TODAY_FILE):
        with open(TODAY_FILE, "w", newline="", encoding="utf-8") as f:
            csv.writer(f, delimiter=";").writerow(
                ["čas", "topic", "hodnota"]
            )

def log_run_marker(text):
    rotate_logs_if_needed()
    ensure_today_header()
    ts = datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M:%S")
    with open(TODAY_FILE, "a", newline="", encoding="utf-8") as f:
        csv.writer(f, delimiter=";").writerow([ts, "", text])

# ====== MQTT CALLBACKY ======
subscribed = False

def on_connect(client, userdata, flags, reason_code, properties):
    global subscribed
    if reason_code == 0:
        if not subscribed:
            client.subscribe(MQTT_TOPIC)
            subscribed = True
        print("Připojeno k MQTT brokeru")

def on_message(client, userdata, msg):
    if msg.retain:
        return

    payload = msg.payload.decode(errors="ignore").strip()
    if payload not in ("0", "1"):
        return

    rotate_logs_if_needed()
    ensure_today_header()

    ts = datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M:%S")
    with open(TODAY_FILE, "a", newline="", encoding="utf-8") as f:
        csv.writer(f, delimiter=";").writerow(
            [ts, msg.topic, payload]
        )

# ====== ČASOVÁ LOGIKA ======
def seconds_until_run_end():
    now = datetime.now(TIMEZONE)

    run_end = now.replace(minute=51, second=0, microsecond=0)
    if now.minute >= 51:
        run_end += timedelta(hours=1)

    return max(0, int((run_end - now).total_seconds()) - SAFETY_SECONDS)

# ====== MAIN ======
def main():
    run_seconds = seconds_until_run_end()
    print(f"Poběžím {run_seconds} sekund")

    if run_seconds <= 0:
        return

    rotate_logs_if_needed()
    ensure_today_header()
    log_run_marker("RUN START")

    client = mqtt.Client(
        callback_api_version=mqtt.CallbackAPIVersion.VERSION2
    )
    client.username_pw_set(MQTT_USER, MQTT_PASS)
    client.on_connect = on_connect
    client.on_message = on_message

    client.connect(MQTT_BROKER, MQTT_PORT, 60)
    client.loop_start()

    time.sleep(run_seconds)

    client.loop_stop()
    client.disconnect()

    log_run_marker("RUN END")
    print("Run korektně ukončen")

if __name__ == "__main__":
    main()
