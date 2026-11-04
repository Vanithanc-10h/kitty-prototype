"""Consumer group 'alerts': watches the same event stream, forecasts each house's pot, and
publishes warnings to a second topic (kitty.alerts).

Because it is a different consumer group from the ledger, it gets its own copy of every event.

    python alert_consumer.py
"""
import json
from confluent_kafka import Consumer, Producer
from common import BOOTSTRAP, EVENTS_TOPIC, ALERTS_TOPIC, load_reference
from logic import apply_event, make_alerts

reference = load_reference()
state, seen, at_risk = {}, set(), {}

consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "alerts",
                     "auto.offset.reset": "earliest", "enable.auto.commit": False})
consumer.subscribe([EVENTS_TOPIC])
producer = Producer({"bootstrap.servers": BOOTSTRAP})
print("alert consumer running (Ctrl+C to stop)")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("error:", msg.error())
            continue
        ev = json.loads(msg.value())
        if apply_event(state, seen, ev):
            for alert in make_alerts(state, at_risk, ev, reference):
                producer.produce(ALERTS_TOPIC, key=alert["household_id"], value=json.dumps(alert))
                if alert["type"] == "shortfall_warning":
                    print(f'{alert["date"]}  WARNING {alert["household_id"]}: pot will be ${alert["shortfall"]:,.0f} short '
                          f'in the week of {alert["week_of"]} ({alert["bill"]} due {alert["bill_due"]})')
                elif alert["type"] == "shortfall_cleared":
                    print(f'{alert["date"]}  cleared  {alert["household_id"]}: forecast is healthy again')
                else:
                    print(f'{alert["date"]}  debit failed  {alert["household_id"]} {alert["member"]}')
            producer.poll(0)
        consumer.commit(msg)
except KeyboardInterrupt:
    pass
finally:
    producer.flush()
    consumer.close()
