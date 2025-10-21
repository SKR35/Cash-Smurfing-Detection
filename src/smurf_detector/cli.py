import argparse
from .detect import run_detection

# … existing imports …

def main():
    p = argparse.ArgumentParser(prog="smurf-detector", description="Cash smurfing (structuring) detector for SQLite")
    sub = p.add_subparsers(dest="cmd", required=True)

    r = sub.add_parser("run", help="Run detector and write results back to the same DB")
    r.add_argument("--db", required=True, help="Path to SQLite DB (from fcc-synthetic-tm)")
    r.add_argument("--channels", default="CASH,ATM", help="Comma-separated channels to consider (default: CASH,ATM)")
    r.add_argument("--direction", default="IN", choices=["IN","OUT"], help="Direction to consider (default: IN)")
    r.add_argument("--per-tx-threshold", type=float, default=10000.0, help="Per-transaction cap (major units)")
    r.add_argument("--window-days", type=int, default=10, help="Rolling window size in days")
    r.add_argument("--min-count", type=int, default=3, help="Min transactions in window")
    r.add_argument("--min-total", type=float, default=20000.0, help="Min total (major units) in window")
    r.add_argument("--write-alerts", action="store_true", help="Also insert account-level alerts (R_STRUCTURING_01)")
    r.add_argument("--cluster-table", default="smurf_clusters", help="Output table for clusters")
    r.add_argument("--link-table", default="smurf_cluster_tx", help="Output mapping of cluster->tx")
    r.add_argument("--alert-tx-table", default="smurf_alert_tx", help="Output mapping of alert_id->tx_id")  # NEW
    args = p.parse_args()

    stats = run_detection(
        db_path=args.db,
        channels=[c.strip().upper() for c in args.channels.split(",") if c.strip()],
        direction=args.direction,
        per_tx_threshold_minor=int(round(args.per_tx_threshold * 100)),
        window_days=args.window_days,
        min_count=args.min_count,
        min_total_minor=int(round(args.min_total * 100)),
        write_alerts=args.write_alerts,
        cluster_table=args.cluster_table,
        link_table=args.link_table,
        alert_tx_table=args.alert_tx_table,   # NEW
    )
    print("Done. clusters={clusters} links={links} alerts={alerts}".format(**stats))