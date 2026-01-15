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

MQTT_PORT = 1883

MQTT_TOPIC_SET = "starymuz@centrum.cz/rele/2/#"
# MQTT_TOPIC_GET = "starymuz@centrum.cz/rele/2/get"

TIMEZONE = pytz.timezone("Europe/Prague")
SAFETY_SECONDS = 60

TODAY_FILE = "mqtt_log_dnes.csv"

# ====== SOUBOROVÁ LOGIKA ======
def rotate_logs_if_needed():
    now = datetime.now(TIMEZONE)
    today_str = now.strftime("%Y-%m-%d")
    yesterday_str = (now - timedelta(days=1)).strftime("%Y-%m-%d")

    # 1️⃣ Přejmenování dnešního logu, pokud je z jiného dne
    if os.path.exists(TODAY_FILE):
        mtime = datetime.fromtimestamp(os.path.getmtime(TODAY_FILE), TIMEZONE)
        if mtime.strftime("%Y-%m-%d") != today_str:
            os.replace(TODAY_FILE, f"mqtt_log_{mtime.strftime('%Y-%m-%d')}.csv")

    # 2️⃣ Smazání všech CSV souborů kromě dnešního a včerejšího
    for f in os.listdir("."):
        if f.endswith(".csv") and f not in (
            TODAY_FILE,
            f"mqtt_log_{yesterday_str}.csv",
        ):
            try:
                os.remove(f)
                print(f"Smazán starý log: {f}")
            except Exception as e:
                print(f"Chyba při mazání souboru {f}: {e}")

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
def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code == 0:
        client.subscribe([
            (MQTT_TOPIC_SET, 0),
#            (MQTT_TOPIC_GET, 0),
        ])
        print("Připojeno k MQTT brokeru")
    else:
        print(f"Chyba připojení: {reason_code}")

def on_message(client, userdata, msg):
    if msg.retain:
        return

    payload = msg.payload.decode(errors="ignore").strip()
    if payload not in ("0", "1"):
        return

    # ---- SET: zapisovat vždy ----
    if msg.topic == MQTT_TOPIC_SET:
        rotate_logs_if_needed()
        ensure_today_header()
        ts = datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M:%S")
        with open(TODAY_FILE, "a", newline="", encoding="utf-8") as f:
            csv.writer(f, delimiter=";").writerow(
                [ts, msg.topic, payload]
            )

    # ---- GET: zapisovat pouze hodnotu 1 ----
#    elif msg.topic == MQTT_TOPIC_GET and payload == "1":
#        rotate_logs_if_needed()
#        ensure_today_header()
#        ts = datetime.now(TIMEZONE).strftime("%Y-%m-%d %H:%M:%S")
#        with open(TODAY_FILE, "a", newline="", encoding="utf-8") as f:
#            csv.writer(f, delimiter=";").writerow(
#                [ts, msg.topic, payload]
#            )

# ====== ČASOVÁ LOGIKA ======
def seconds_until_run_end():
    now = datetime.now(TIMEZONE)

    run_end = now.replace(minute=47, second=0, microsecond=0)
    if now.minute >= 47:
        run_end += timedelta(hours=1)

    return max(0, int((run_end - now).total_seconds()) - SAFETY_SECONDS)

# ====== MAIN ======
def main():
    # ⬇⬇⬇ DŮLEŽITÉ – vynutit úklid hned
    rotate_logs_if_needed()

    run_seconds = seconds_until_run_end()
    print(f"Poběžím {run_seconds} sekund")

    if run_seconds <= 0:
        print("Běžící hodina téměř končí – run se nespustí")
        return

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
