# Kitty on Kafka

A small event-driven version of Kitty (a shared payments account for share houses). It replays six months of
synthetic household activity into Apache Kafka and runs two independent services on the stream:
one builds each house's pot balance, the other forecasts the pot and warns **before** a bill can't be paid.

All data is synthetic and no real money moves.

```
data/events.jsonl --> producer.py --> [ kitty.events ] --+--> ledger_consumer.py  (group "ledger")  --> ledger.db
 (from the workbook)    keyed by        4 partitions      |
                        household_id                      +--> alert_consumer.py   (group "alerts")  --> [ kitty.alerts ]
```

## Files

| File | What it does |
|---|---|
| `prepare_events.py` | Converts the Excel dataset into `data/events.jsonl` (613 events) and `data/reference.json` |
| `logic.py` | All business logic (balance, 8-week forecast, alert rules). No Kafka, so it is easy to test |
| `producer.py` | Creates the topics and publishes events in date order, keyed by household |
| `ledger_consumer.py` | Consumer group `ledger`: pot balance per house into SQLite, idempotent, commits after saving |
| `alert_consumer.py` | Consumer group `alerts`: forecasts each house and publishes warnings to `kitty.alerts` |
| `tests/test_logic.py` | Offline checks (no Kafka needed) |

## Run it

```bash
pip install -r requirements.txt
docker compose up -d                 # one Kafka broker, no ZooKeeper
python producer.py                   # terminal 1: create topics and replay events (~20 s). Start this first.
python ledger_consumer.py            # terminal 2
python alert_consumer.py             # terminal 3
```

Expected: in terminal 3, a warning on **2026-06-08** that Newtown (H01) will be about $613 short for the Sydney Power Co
bill due 14 July, six weeks ahead, after Jai's debits fail. The house has no other warnings.

Check the results:
```bash
sqlite3 ledger.db "select * from pot"
docker exec kitty-kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic kitty.alerts --from-beginning
python tests/test_logic.py           # no Kafka needed
```

To replay from the start, stop the consumers and reset their offsets:
```bash
docker exec kitty-kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group alerts --topic kitty.events --reset-offsets --to-earliest --execute
```

## Kafka ideas this demonstrates

- **Topics and partitions:** `kitty.events` has 4 partitions; events are keyed by `household_id`, so each house stays in order.
- **Consumer groups:** `ledger` and `alerts` each read the full stream independently.
- **Offsets and replay:** run a consumer again from the earliest offset and it rebuilds the same state.
- **At-least-once delivery + idempotency:** manual offset commits after processing, and duplicates are ignored by `event_id`.
- **Producer settings:** idempotent producer, delivery callbacks, flush.
- **Event-sourced ledger:** the balance is derived from events, and `tests/test_logic.py` checks it against the workbook's own ledger (within $1, since events are rounded to cents).

## Limits (say these out loud)

- Synthetic data and simulated payments. There is no bank connection and no real PayTo.
- Single local broker with replication factor 1. This is a learning setup, not production.
- The forecast is simple: each member's weekly share x their success rate over the last four collections, minus rent, minus *typical* bill amounts. It cannot know a bill will be bigger than usual (Newtown's July bill was $742, not the typical $620).
- Alerts under $100 projected shortfall are ignored, to avoid noise.

## Next steps (pick one or two)

- A `bill_received` event that updates the expected bill amount, so the forecast sees the $742 bill early.
- `member_joined` / `member_left` events and a move-out settlement.
- A dead-letter topic for malformed events.
- A tiny web page or Streamlit app that reads `kitty.alerts`.
- Schema validation (JSON Schema or Avro with a schema registry).

## Resume bullets (edit to match what you actually build)

- Built an event-driven payments pipeline on Apache Kafka (Docker, KRaft) that replays 600+ simulated household payment events, partitioned by household for per-house ordering.
- Implemented two independent Python consumer groups: an idempotent, event-sourced ledger (SQLite, manual offset commits) and a forecasting service that publishes shortfall alerts to a separate topic, warning six weeks before a missed bill in the test scenario.
- Validated the replayed ledger against the source dataset and tested replay/duplicate handling with automated checks.
