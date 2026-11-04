"""Consumer group 'ledger': builds the household ledger (pot balance) from the event stream.

Writes to a local SQLite file. The event_id primary key makes it idempotent, and offsets are
committed only after the row is saved (at-least-once delivery + idempotent write).

    python ledger_consumer.py
Then query it:  sqlite3 ledger.db "select * from pot"
"""
import json, sqlite3
from confluent_kafka import Consumer
from common import BOOTSTRAP, EVENTS_TOPIC
from logic import IN_EVENTS

db = sqlite3.connect("ledger.db")
db.execute("create table if not exists ledger (event_id text primary key, household_id text, date text,"
           " type text, description text, amount real, balance real)")
db.execute("create view if not exists pot as select household_id, round(sum(amount),2) as balance,"
           " max(date) as as_of from ledger group by household_id")

consumer = Consumer({"bootstrap.servers": BOOTSTRAP, "group.id": "ledger",
                     "auto.offset.reset": "earliest", "enable.auto.commit": False})
consumer.subscribe([EVENTS_TOPIC])
print("ledger consumer running (Ctrl+C to stop)")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("error:", msg.error())
            continue
        ev = json.loads(msg.value())
        if ev["type"] in IN_EVENTS:
            amount = ev["amount"]
        elif ev["type"] == "bill_paid":
            amount = -ev["amount"]
        else:
            consumer.commit(msg)                # failed debits do not move money
            continue
        prev = db.execute("select coalesce(sum(amount),0) from ledger where household_id=?",
                          (ev["household_id"],)).fetchone()[0]
        db.execute("insert or ignore into ledger values (?,?,?,?,?,?,?)",
                   (ev["event_id"], ev["household_id"], ev["date"], ev["type"],
                    ev.get("description") or ev.get("member_name", ""), amount, prev + amount))
        db.commit()
        consumer.commit(msg)
        print(f'{ev["date"]} {ev["household_id"]} {ev["type"]:<22} {amount:>10.2f}   pot {prev + amount:>10.2f}')
except KeyboardInterrupt:
    pass
finally:
    consumer.close()
