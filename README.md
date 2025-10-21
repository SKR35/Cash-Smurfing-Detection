# Cash-Smurfing-Detection

Smurfing (structuring) detector for **SQLite** TM datasets.  
Designed to work out-of-the-box with the sample DB from <a href="https://github.com/SKR35/FCC-Synthetic-TM" target="_blank" rel="noopener noreferrer">**fcc-synthetic-tm**</a> (tables: `customers`, `accounts`, `cash_transactions`, `alerts`).  
Stdlib-only (no external dependencies).

---

## Quickstart (Conda on Windows)

```bat
conda create -n smurfdet python=3.11 -y
conda activate smurfdet
python --version

pip install -e .
python -m smurf_detector run --db data\fcc_tm.sqlite --channels CASH,ATM --direction IN --per-tx-threshold 10000 --window-days 10 --min-count 3 --min-total 20000 --write-alerts```

Make sure data\fcc_tm.sqlite exists in this repo (copy it from your fcc-synthetic-tm project or generate one there and copy over).

## What it does

Scans CASH/ATM IN transactions below a per-tx threshold.

Slides a rolling window (e.g., 10 days) to find bursts with:

at least min_count qualifying tx

cumulative min_total amount

Writes results back to the same DB:

smurf_clusters — one row per detected burst (per account)

smurf_cluster_tx — mapping: cluster → member transactions

smurf_alert_tx — mapping: alert_id → transaction(s) (when --write-alerts)

Optionally inserts ACCOUNT-level alerts into alerts (rule_id='R_STRUCTURING_01')

## CLI

python -m smurf_detector run --db <path> \
  --channels CASH,ATM \
  --direction IN \
  --per-tx-threshold 10000 \
  --window-days 10 \
  --min-count 3 \
  --min-total 20000 \
  --write-alerts

Parameters (key)

--per-tx-threshold – max amount per transaction (major units) to be considered “below threshold”.

--window-days – rolling window size (days).

--min-count – minimum number of qualifying tx in the window.

--min-total – minimum cumulative amount (major units).

--channels, --direction – filter the population (defaults are CASH/ATM + IN).

Advanced:

--cluster-table (default: smurf_clusters)

--link-table (default: smurf_cluster_tx)

--alert-tx-table (default: smurf_alert_tx)

## Peek at results (SQLite snippets)

Top clusters:
SELECT account_id, tx_count, ROUND(total_amount_minor/100.0,2) AS total,
       start_ts_utc, end_ts_utc
FROM smurf_clusters
ORDER BY total_amount_minor DESC
LIMIT 10;

Alert → transactions:
SELECT a.alert_id, atx.tx_id
FROM alerts a
JOIN smurf_alert_tx atx ON atx.alert_id = a.alert_id
WHERE a.rule_id = 'R_STRUCTURING_01'
LIMIT 20;