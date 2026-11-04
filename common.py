import json

BOOTSTRAP = "localhost:9092"
EVENTS_TOPIC = "kitty.events"
ALERTS_TOPIC = "kitty.alerts"


def load_reference():
    return json.load(open("data/reference.json"))
