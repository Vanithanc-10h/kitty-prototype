"""Run offline (no Kafka needed):  python tests/test_logic.py"""
import json, sys
sys.path.insert(0, ".")
import pandas as pd
from logic import apply_event, make_alerts

events = [json.loads(l) for l in open("data/events.jsonl")]
reference = json.load(open("data/reference.json"))


def replay(evs):
    state, seen, alerted, alerts = {}, set(), {}, []
    for ev in evs:
        if apply_event(state, seen, ev):
            alerts += make_alerts(state, alerted, ev, reference)
    return state, alerts


state, alerts = replay(events)

# 1. Final balances should match the workbook's own ledger
ledger = pd.read_excel("data/kitty_sharehouse_dataset.xlsx", sheet_name="Pot_Ledger")
ledger = ledger[ledger.Entry_ID.astype(str).str.match(r"^L\d+$")]
for hid, grp in ledger.groupby("Household_ID"):
    expected = float(grp.iloc[-1].Running_Pot_Balance_AUD)
    got = state[hid]["balance"]
    print(f"{hid}: replayed {got:10.2f}  workbook {expected:10.2f}")
    assert abs(got - expected) < 1.0, hid  # events are rounded to cents

# 2. Replaying the same events twice must not change anything (idempotency)
dup_state, _ = replay(events + events)
assert all(abs(dup_state[h]["balance"] - state[h]["balance"]) < 1e-6 for h in state)

# 3. Only Newtown (H01) should get a shortfall warning, and before the July bill
warn = [a for a in alerts if a["type"] == "shortfall_warning"]
for a in alerts:
    if a["type"] != "debit_failed":
        print(a)
assert len(warn) == 1 and warn[0]["household_id"] == "H01"
assert warn[0]["bill"] == "Sydney Power Co" and warn[0]["date"] < "2026-07-14"
print("All checks passed")
