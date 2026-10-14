#!/usr/bin/env python3
# S0.4 verification: Eclipse Paho MQTT (Python) minimal surface.
# Goal: prove the client library is obtainable and its connect API is usable.
# Run: pip3 install --target ./pylibs paho-mqtt && PYTHONPATH=./pylibs python3 mqtt_python.py
import paho.mqtt.client as m

c = m.Client(m.CallbackAPIVersion.VERSION2, client_id="s0_4")
print("paho-mqtt imported; Client() constructed:", type(c).__name__)
# paho v2 connect() is async; without a broker it schedules a reconnect rather
# than raising, so this only proves the API is callable. Full pub/sub needs a
# broker (S5).
try:
    c.connect("127.0.0.1", 1883, 2)
    print("connect() callable (async; no broker to fully verify)")
except Exception as e:
    print("connect rejected:", type(e).__name__, e)
