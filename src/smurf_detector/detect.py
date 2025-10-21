import sqlite3, uuid, json
from datetime import datetime, timedelta
from collections import deque
from typing import List, Tuple, Dict

def _uuid() -> str:
    return str(uuid.uuid4())

def _ensure_tables(conn: sqlite3.Connection, cluster_table: str, link_table: str, alert_tx_table: str):
    conn.execute(f"""
    CREATE TABLE IF NOT EXISTS {cluster_table} (
      cluster_id TEXT PRIMARY KEY,
      account_id TEXT NOT NULL,
      customer_id TEXT,
      start_ts_utc TEXT NOT NULL,
      end_ts_utc TEXT NOT NULL,
      tx_count INTEGER NOT NULL,
      total_amount_minor INTEGER NOT NULL,
      params_json TEXT NOT NULL
    );""")
    conn.execute(f"""
    CREATE TABLE IF NOT EXISTS {link_table} (
      cluster_id TEXT NOT NULL,
      tx_id TEXT NOT NULL,
      PRIMARY KEY (cluster_id, tx_id)
    );""")
    conn.execute(f"""
    CREATE TABLE IF NOT EXISTS {alert_tx_table} (
      alert_id TEXT NOT NULL,
      tx_id TEXT NOT NULL,
      PRIMARY KEY (alert_id, tx_id)
    );""")

def _load_tx(conn: sqlite3.Connection, channels: List[str], direction: str, per_tx_threshold_minor: int
            ) -> List[Tuple]:
    q = f"""
    SELECT t.tx_id, t.account_id, t.customer_id, t.ts_utc, t.amount_minor
    FROM cash_transactions t
    JOIN accounts a ON a.account_id = t.account_id
    WHERE t.channel IN ({','.join(['?']*len(channels))})
      AND t.direction = ?
      AND t.amount_minor < ?
    ORDER BY t.account_id, t.ts_utc
    """
    cur = conn.execute(q, (*channels, direction, per_tx_threshold_minor))
    return cur.fetchall()

def _iter_clusters_for_account(rows: List[Tuple], window_days: int, min_count: int, min_total_minor: int):
    win = deque()
    total = 0
    day_span = timedelta(days=window_days)
    for tx_id, account_id, customer_id, ts_iso, amt in rows:
        ts = datetime.fromisoformat(ts_iso.replace("Z",""))
        win.append((tx_id, account_id, customer_id, ts, amt))
        total += amt
        cutoff = ts - day_span
        while win and win[0][3] < cutoff:
            _, _, _, _, old_amt = win.popleft()
            total -= old_amt
        if len(win) >= min_count and total >= min_total_minor:
            yield (
                account_id,
                customer_id,
                win[0][3].isoformat(timespec="seconds")+"Z",
                ts.isoformat(timespec="seconds")+"Z",
                len(win),
                total,
                [w[0] for w in win],
            )

def _group_by_account(rows: List[Tuple]) -> Dict[str, List[Tuple]]:
    by = {}
    for r in rows:
        by.setdefault(r[1], []).append(r)
    return by

def run_detection(db_path: str,
                  channels: List[str],
                  direction: str,
                  per_tx_threshold_minor: int,
                  window_days: int,
                  min_count: int,
                  min_total_minor: int,
                  write_alerts: bool,
                  cluster_table: str,
                  link_table: str,
                  alert_tx_table: str):
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON;")
    try:
        _ensure_tables(conn, cluster_table, link_table, alert_tx_table)

        rows = _load_tx(conn, channels, direction, per_tx_threshold_minor)
        by_acct = _group_by_account(rows)

        clusters = []
        for account_id, txs in by_acct.items():
            last = None
            for acct_id, cust_id, start_ts, end_ts, count, total, tx_ids in _iter_clusters_for_account(
                txs, window_days, min_count, min_total_minor
            ):
                if last and start_ts <= last["end_ts"] and acct_id == last["account_id"]:
                    last["end_ts"] = end_ts
                    last["tx_ids"].update(tx_ids)
                    last["count"] = len(last["tx_ids"])
                else:
                    if last:
                        clusters.append(last)
                    last = {
                        "cluster_id": _uuid(),
                        "account_id": acct_id,
                        "customer_id": cust_id,
                        "start_ts": start_ts,
                        "end_ts": end_ts,
                        "count": count,
                        "total": total,
                        "tx_ids": set(tx_ids),
                    }
            if last:
                clusters.append(last)

        # persist clusters + links
        params = {
            "channels": channels,
            "direction": direction,
            "per_tx_threshold_minor": per_tx_threshold_minor,
            "window_days": window_days,
            "min_count": min_count,
            "min_total_minor": min_total_minor,
        }
        link_count = 0
        for c in clusters:
            conn.execute(f"""
              INSERT OR IGNORE INTO {cluster_table}
              (cluster_id, account_id, customer_id, start_ts_utc, end_ts_utc, tx_count, total_amount_minor, params_json)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                c["cluster_id"], c["account_id"], c["customer_id"],
                c["start_ts"], c["end_ts"], c["count"], c["total"], json.dumps(params)
            ))
            for tx_id in c["tx_ids"]:
                conn.execute(f"INSERT OR IGNORE INTO {link_table} (cluster_id, tx_id) VALUES (?, ?)",
                             (c["cluster_id"], tx_id))
                link_count += 1

        alerts_written = 0
        if write_alerts and clusters:
            for c in clusters:
                # ACCOUNT-level alert
                alert_id = _uuid()
                score = min(100.0, c["total"] / max(1, per_tx_threshold_minor))
                conn.execute("""
                  INSERT INTO alerts
                  (alert_id, created_ts_utc, entity_type, entity_id, rule_id, score, label, typology, outcome, closed_ts_utc)
                  VALUES (?, datetime('now'), 'ACCOUNT', ?, 'R_STRUCTURING_01', ?, NULL, 'STRUCTURING', 'OPEN', NULL)
                """, (alert_id, c["account_id"], round(score, 2)))
                alerts_written += 1
                # Map alert -> all member transactions
                for tx_id in c["tx_ids"]:
                    conn.execute(f"INSERT OR IGNORE INTO {alert_tx_table} (alert_id, tx_id) VALUES (?, ?)",
                                 (alert_id, tx_id))

        conn.commit()
        return {"clusters": len(clusters), "links": link_count, "alerts": alerts_written}
    finally:
        conn.close()