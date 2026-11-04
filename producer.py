"""Replays the synthetic dataset into Kafka, in date order, like a payments provider would.

Messages are keyed by household_id, so all events for one house go to the same partition
and are processed in order.

    python producer.py            # about 20 seconds for the whole 6 months
    python producer.py --delay 0  # as fast as possible
"""
import argparse, json, time
from datetime import date
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from common import BOOTSTRAP, EVENTS_TOPIC, ALERTS_TOPIC

ap = argparse.ArgumentParser()
ap.add_argument("--delay", type=float, default=0.1, help="seconds of wall-clock time per simulated day")
args = ap.parse_args()

# Create topics (4 partitions so the 4 households can be spread across consumers)
admin = AdminClient({"bootstrap.servers": BOOTSTRAP})
for name, fut in admin.create_topics([NewTopic(EVENTS_TOPIC, num_partitions=4, replication_factor=1),
                                      NewTopic(ALERTS_TOPIC, num_partitions=1, replication_factor=1)]).items():
    try:
        fut.result()
        print("created topic", name)
    except Exception as e:                      # already exists is fine
        print("topic", name, "->", str(e).split("\n")[0])

producer = Producer({"bootstrap.servers": BOOTSTRAP, "enable.idempotence": True})
failed = []


def on_delivery(err, msg):
    if err:
        failed.append(err)


events = [json.loads(line) for line in open("data/events.jsonl")]
prev = None
for ev in events:
    d = date.fromisoformat(ev["date"])
    if prev and args.delay and d > prev:
        producer.flush()
        time.sleep((d - prev).days * args.delay)
    prev = max(prev, d) if prev else d
    producer.produce(EVENTS_TOPIC, key=ev["household_id"], value=json.dumps(ev), on_delivery=on_delivery)
    producer.poll(0)
    if ev["type"] == "contribution_failed":
        print(f'{ev["date"]}  debit FAILED  {ev["household_id"]} {ev["member_name"]}')

producer.flush()
print(f"Sent {len(events)} events, {len(failed)} delivery errors")
