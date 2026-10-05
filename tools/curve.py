#!/usr/bin/env python3
"""Turn ramp results (results/*.jsonl) into a throughput/latency curve with the KNEE marked.

    python3 tools/curve.py results/noop-*.jsonl
    python3 tools/curve.py results/*.jsonl --latency e2e_p99 --factor 3

Knee = first step where EITHER achieved rate falls below 95% of target, OR the chosen latency
exceeds `factor` x the latency of the lowest-rate step. That is where you go looking for the
first resource that saturated (see the dashboard at that timestamp).
"""
import argparse
import json
import sys


def load(path):
    lines = [json.loads(l) for l in open(path) if l.strip()]
    return lines[0], lines[1:]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("files", nargs="+")
    ap.add_argument("--latency", default="rpc_p99", help="column to detect the knee on (rpc_p99 | intended_p99 | e2e_p99)")
    ap.add_argument("--factor", type=float, default=3.0)
    a = ap.parse_args()
    for path in a.files:
        meta, rows = load(path)
        m = meta["meta"]
        print(f"\n{path}\n  shape={m['shape']} processes={m['processes']} params={json.dumps(meta.get('params'))[:100]}")
        print(f"  {'target/s':>9} {'achieved/s':>11} {'err':>6} {'rpc p99':>9} {'e2e p50':>9} {'e2e p99':>9} {'lag p99':>9}  ")
        base = next((r[a.latency] for r in rows if r.get(a.latency)), None)
        knee = None
        for r in rows:
            lat = r.get(a.latency)
            bad_rate = r["target_rate"] and r["achieved_rate"] < 0.95 * r["target_rate"]
            bad_lat = base and lat and lat > a.factor * base
            mark = ""
            if knee is None and (bad_rate or bad_lat):
                knee, mark = r["step"], "  <-- KNEE (" + ("throughput shortfall" if bad_rate else f"{a.latency} > {a.factor}x baseline") + ")"
            f = lambda x: "-" if x is None else f"{x:.0f}"  # noqa: E731
            print(f"  {r['target_rate']:>9.0f} {r['achieved_rate']:>11.1f} {r['errors']:>6} {f(r['rpc_p99']):>9} "
                  f"{f(r['e2e_p50']):>9} {f(r['e2e_p99']):>9} {f(r['lag_p99']):>9}{mark}")
        if knee is None:
            print("  no knee inside this ramp: raise the top rate")
        if any((r["lag_p99"] or 0) > 250 for r in rows):
            print("  CAUTION: generator lag > 250 ms in some steps; the numbers may reflect the LOAD GENERATOR, not Temporal")
    return 0


if __name__ == "__main__":
    sys.exit(main())
