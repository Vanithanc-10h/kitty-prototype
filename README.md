# Kitty: share house payments prototype

FINS5548 capstone project, team FinTech Crew.

Kitty is a shared payments account for share houses. It splits the bills, collects each housemate's share every week by PayTo, holds the money in a household pot, pays the rent and bills on the due date, warns the house early when the pot will not cover a bill, and settles fairly when a housemate moves out.

Live app: https://kitty-fins5548.netlify.app/

All households, members and payments are synthetic. No real money moves.

## Two parts

| Part | Where | What it is |
|---|---|---|
| Web app | `index.html` | The working prototype. It runs in the browser with no installation. It holds the dataset, the calculation engine and the screens (Home, Split, Forecast, Bills, Move in / out, Kitty vs today, Business). |
| Kafka pipeline | `producer.py`, `ledger_consumer.py`, `alert_consumer.py`, `logic.py`, `prepare_events.py`, `docker-compose.yml`, `tests/`, `data/` | An event-driven version of the same idea on Apache Kafka. See `KAFKA_PIPELINE.md` for how to run it. |

## Files

| File | What it is |
|---|---|
| `index.html` | The web app. Netlify serves this file as the site. |
| `kitty-app.html` | The first prototype: six screens with a short explanation under each. Kept for reference. |
| `kitty_sharehouse_dataset.xlsx` | The synthetic dataset: four share houses, 17 residents, 26 weeks of bills, collections and pot balances. |
| `Kitty_Proposal_Brief.pdf` | The written proposal brief. |
| `KAFKA_PIPELINE.md` | Guide to the Kafka pipeline. |

## How to view the app

Open the live link above, or open `index.html` in a browser. Click **Present** to step through the demo.

## Note on the two forecasts

The web app and the Kafka pipeline both warn the Newtown house about the Sydney Power Co bill due on 14 July, but they use different forecast rules, so the shortfall figures differ. The web app uses the bill's actual amount and shows $541 short on 8 June. The pipeline uses a simpler rule (typical bill amounts) and reports about $613. Use the web app figures in the pitch and the report.

## Deploy

The site deploys automatically from the `main` branch through Netlify. There is no build step. Netlify publishes the repository root, so a change to `index.html` on `main` goes live within a minute.

## Roles

| Role | Responsibility |
|---|---|
| CEO | Coordination, introduction and conclusion, pitch opening and the ask |
| CTO | Prototype, technology section, dataset |
| COO | Regulation, privacy, partnerships, risk |
| CMO | Market sizing, competitors, go-to-market |
| CFO | Business model, projections, funding ask |
