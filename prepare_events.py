"""Turn the synthetic Kitty workbook into a time-ordered stream of events.

Output:
  data/events.jsonl    one JSON event per line (what the producer replays)
  data/reference.json  weekly rent per house + the bill schedule (used by the forecast)
"""
import json
import pandas as pd

SRC = "data/kitty_sharehouse_dataset.xlsx"
x = pd.ExcelFile(SRC)


def day(v):
    return pd.Timestamp(v).strftime("%Y-%m-%d")


def clean(df, id_col, pattern):
    """The sheets have footnote rows at the bottom; keep only real data rows."""
    return df[df[id_col].astype(str).str.match(pattern)].copy()


events = []

# 1. Opening pot balance for each house
opening = clean(x.parse("Opening_Balances"), "Household_ID", r"^H\d+$")
for _, r in opening.iterrows():
    events.append(dict(event_id=f"OPEN-{r.Household_ID}", type="pot_opened",
                       household_id=r.Household_ID, date="2026-03-02",
                       amount=round(float(r.iloc[1]), 2), description="Opening pot balance"))

# 2. Weekly PayTo collections (success, failure, failure then retry)
contrib = clean(x.parse("Contributions"), "Contribution_ID", r"^C\d{4}$")
for _, r in contrib.iterrows():
    base = dict(household_id=r.Household_ID, member_id=r.Member_ID,
                member_name=r.Member_Name, amount_due=round(float(r.Amount_Due_AUD), 2))
    if r.Status == "Successful":
        events.append(dict(base, event_id=f"{r.Contribution_ID}-ok", type="contribution_collected",
                           date=day(r.Collection_Date), amount=round(float(r.Amount_Collected_AUD), 2)))
    else:
        events.append(dict(base, event_id=f"{r.Contribution_ID}-fail", type="contribution_failed",
                           date=day(r.Collection_Date), amount=0.0, reason=r.Failure_Reason))
        if r.Status.startswith("Failed — retried"):
            events.append(dict(base, event_id=f"{r.Contribution_ID}-retry", type="contribution_collected",
                               date=day(r.Retry_Date), amount=round(float(r.Amount_Collected_AUD), 2), retry=True))

# 3. Bills and rent paid out of the pot
bills = clean(x.parse("Bill_Instances"), "Instance_ID", r"^BI\d+$")
for _, r in bills.iterrows():
    events.append(dict(event_id=r.Instance_ID, type="bill_paid", household_id=r.Household_ID,
                       date=day(r.Paid_Date), amount=round(float(r.Amount_AUD), 2),
                       category=r.Category, biller=r.Biller, due_date=day(r.Due_Date),
                       description=f"{r.Biller} - {r.Period_Covered}"))

# 4. The one-off arrears catch-up transfer (only recorded in the ledger sheet)
ledger = clean(x.parse("Pot_Ledger"), "Entry_ID", r"^L\d+$")
for _, r in ledger[ledger.Category == "Arrears"].iterrows():
    events.append(dict(event_id=r.Entry_ID, type="arrears_payment", household_id=r.Household_ID,
                       date=day(r.Date), amount=round(float(r.Amount_AUD), 2), description=r.Description))

order = {"pot_opened": 0, "contribution_collected": 1, "contribution_failed": 1,
         "arrears_payment": 2, "bill_paid": 3}
events.sort(key=lambda e: (e["date"], order[e["type"]], e["event_id"]))

with open("data/events.jsonl", "w") as f:
    for e in events:
        f.write(json.dumps(e) + "\n")

# Reference data for the forecast: weekly rent and the *expected* bill schedule.
# Each recurring bill repeats at its usual interval for its typical amount, so the forecast
# never "knows" a bill will be bigger than usual (that is what a real bill reader would add).
from datetime import date, timedelta

hh = clean(x.parse("Households"), "Household_ID", r"^H\d+$")
rec = clean(x.parse("Recurring_Bills"), "Bill_ID", r"^B\d+$").set_index("Bill_ID")
nonrent = bills[bills.Category != "Rent"]
schedule = []
for bill_id, grp in nonrent.groupby("Bill_ID"):
    dues = sorted(pd.to_datetime(grp.Due_Date))
    gap = int(pd.Series(dues).diff().dt.days.median()) if len(dues) > 1 else 91
    typical = round(float(rec.loc[bill_id, "Typical_Amount_AUD"]), 2)
    d = dues[0].date()
    while d <= date(2026, 12, 31):
        schedule.append(dict(household_id=rec.loc[bill_id, "Household_ID"], due_date=d.isoformat(),
                             amount=typical, biller=rec.loc[bill_id, "Biller"]))
        d += timedelta(days=gap)

reference = {"rent_weekly": {r.Household_ID: float(r.Weekly_Rent_Total_AUD) for _, r in hh.iterrows()},
             "bills": schedule}
json.dump(reference, open("data/reference.json", "w"), indent=1)
print(f"Wrote {len(events)} events and {len(schedule)} expected bills")
