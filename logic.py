"""Pure business logic. No Kafka in here, so it is easy to test.

state = {household_id: {"balance": float, "last_collection": "YYYY-MM-DD",
                        "members": {member_id: {"due": float, "recent": [1, 0, ...]}}}}
"""
from datetime import date, timedelta

MIN_SHORTFALL = 100  # ignore tiny projected gaps so the house is not nagged over pennies
IN_EVENTS = {"pot_opened", "contribution_collected", "arrears_payment"}


def new_house():
    return {"balance": 0.0, "last_collection": None, "weeks": set(), "members": {}}


def _days_between(a, b):
    return (date.fromisoformat(b) - date.fromisoformat(a)).days


def active_members(house, as_of):
    """Members who had a collection in the last two weeks (so leavers drop out)."""
    return {k: m for k, m in house["members"].items() if _days_between(m["last_date"], as_of) < 14}


def apply_event(state, seen, ev):
    """Update state from one event. Returns False if the event was a duplicate."""
    if ev["event_id"] in seen:          # idempotent: Kafka delivers at-least-once
        return False
    seen.add(ev["event_id"])
    house = state.setdefault(ev["household_id"], new_house())

    if ev["type"] in IN_EVENTS:
        house["balance"] += ev["amount"]
    elif ev["type"] == "bill_paid":
        house["balance"] -= ev["amount"]

    if ev["type"].startswith("contribution"):
        m = house["members"].setdefault(ev["member_id"], {"due": 0.0, "recent": [], "last_date": ev["date"]})
        m["due"] = ev["amount_due"]
        ok = 1 if ev["type"] == "contribution_collected" else 0
        if ev.get("retry") and m["recent"]:
            m["recent"][-1] = ok        # a retry fixes the failed attempt, it is not a new one
        else:
            m["recent"].append(ok)
        m["recent"] = m["recent"][-4:]  # only the last four collections count
        if not ev.get("retry"):         # retries land mid-week; they do not start a new week
            m["last_date"] = ev["date"]
            house["last_collection"] = ev["date"]
            house["weeks"].add(ev["date"])
    return True


def success_rate(member):
    r = member["recent"]
    return sum(r) / len(r) if r else 1.0


def forecast(house, house_id, reference, horizon_weeks=8):
    """Project the pot week by week. Returns the first shortfall, or None.

    Each Monday: + (each member's weekly share x their recent success rate) - rent.
    Scheduled bills come out on their due dates.
    """
    cursor = date.fromisoformat(house["last_collection"])
    rent = reference["rent_weekly"][house_id]
    # We forecast right after this week's collections arrive, before this week's rent
    # has been paid out, so take this week's rent off first.
    pot = house["balance"] - rent
    expected_in = sum(m["due"] * success_rate(m)
                      for m in active_members(house, house["last_collection"]).values())
    bills = [b for b in reference["bills"] if b["household_id"] == house_id]

    for _ in range(horizon_weeks):
        monday = cursor + timedelta(days=7)
        mid = [b for b in bills if cursor < date.fromisoformat(b["due_date"]) < monday]
        pot -= sum(b["amount"] for b in mid)
        if pot < 0:
            return _shortfall(pot, mid, monday)
        pot += expected_in - rent
        on_monday = [b for b in bills if date.fromisoformat(b["due_date"]) == monday]
        pot -= sum(b["amount"] for b in on_monday)
        if pot < 0:
            return _shortfall(pot, on_monday, monday)
        cursor = monday
    return None


def _shortfall(pot, due_bills, when):
    biggest = max(due_bills, key=lambda b: b["amount"]) if due_bills else None
    return {"shortfall": round(-pot, 2), "week_of": when.isoformat(),
            "bill": biggest["biller"] if biggest else "rent",
            "bill_due": biggest["due_date"] if biggest else when.isoformat()}


def week_is_complete(house, ev):
    """True once every active member has a collection result for this week's date."""
    if len(house["weeks"]) < 2:         # first week: we do not know the members yet
        return False
    return all(m["last_date"] == ev["date"] for m in active_members(house, ev["date"]).values())


def make_alerts(state, at_risk, ev, reference):
    """Decide which alerts (if any) this event should trigger.

    at_risk[household_id] remembers whether a warning is already open,
    so we alert when the house becomes at risk and again when it recovers.
    """
    out = []
    hid = ev["household_id"]
    if ev["type"] == "contribution_failed":
        out.append({"type": "debit_failed", "household_id": hid, "date": ev["date"],
                    "member": ev["member_name"], "reason": ev["reason"]})
    house = state[hid]
    if ev["type"].startswith("contribution") and not ev.get("retry") and week_is_complete(house, ev):
        f = forecast(house, hid, reference)
        if f and f["shortfall"] >= MIN_SHORTFALL and not at_risk.get(hid):
            at_risk[hid] = True
            out.append({"type": "shortfall_warning", "household_id": hid, "date": ev["date"], **f})
        elif not f and at_risk.get(hid):
            at_risk[hid] = False
            out.append({"type": "shortfall_cleared", "household_id": hid, "date": ev["date"]})
    return out
