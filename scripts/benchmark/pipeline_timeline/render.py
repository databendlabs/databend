#!/usr/bin/env python3
"""Render pipeline timelines recorded with `pipeline_timeline_interval_ms` as one HTML page.

usage: render.py <out.html> <log file>...

The input is the `[PIPELINE-TIMELINE]` log lines of one query from every node, one
per line, e.g. exported from system_history.log_history:

  select message from system_history.log_history
  where message like '[PIPELINE-TIMELINE] query_id=<id> %' order by timestamp

Other text on a line before the marker is ignored. Every node gets a section: a summary
strip (worker threads busy, scan bytes, exchange bytes sent and answered, mean frame
round trip) and one row per (plan node, processor) group. A row shows, per bucket,
how the processors of the group split their time between the executor states,
stacked to the number of processors, and its output rows per second.

States, as the executor sees a processor:
  NeedData     its last event was NeedData: waits for input
  NeedConsume  its last event was NeedConsume: waits for its output to be taken
  SyncQueued   ready to run, no worker runs it yet
  SyncRunning  a worker runs its process()
  Async        its async_process() has not completed (IO, network, waits)
  InEvent      the executor runs its event(); some processors work there, such as
               the scatter and encoding of an exchange sender
  Unscheduled  the executor has not run its event() yet (e.g. join probe side
               before the build side finished)
"""

import html
import json
import re
import sys

STATES = ["SyncRunning", "InEvent", "SyncQueued", "Async", "NeedConsume", "NeedData", "Unscheduled"]
COLORS = {
    "SyncRunning": "#2b6cb0",
    "InEvent": "#38b2ac",
    "SyncQueued": "#e53e3e",
    "Async": "#805ad5",
    "NeedConsume": "#dd6b20",
    "NeedData": "#cbd5e0",
    "Unscheduled": "#f1e9c6",
}
WIDTH = 900
ROW_H = 46
LABEL_W = 330


def series(group, key, kind, n):
    values = group.get(kind, {}).get(key, [])
    return (values + [0] * n)[:n]


def human(v):
    for unit in ["", "K", "M", "G", "T"]:
        if abs(v) < 1000:
            return f"{v:.3g}{unit}"
        v /= 1000
    return f"{v:.3g}P"


def stacked(group, n, interval_us):
    """SVG of the processors of a group in each state, per bucket."""
    procs = max(group["processors"], 1)
    w = WIDTH / n
    parts = []
    for i in range(n):
        y = ROW_H
        for state in STATES:
            v = series(group, state, "state_us", n)[i] / interval_us
            if v <= 0:
                continue
            h = ROW_H * v / procs
            y -= h
            parts.append(
                f'<rect x="{i * w:.2f}" y="{y:.2f}" width="{w + 0.05:.2f}" height="{h:.2f}" '
                f'fill="{COLORS[state]}"><title>{state} {v:.2f} of {procs}</title></rect>'
            )
    return parts


def line(values, n, color, height, label):
    peak = max(values) if values else 0
    if peak <= 0:
        return []
    w = WIDTH / n
    pts = " ".join(
        f"{(i + 0.5) * w:.2f},{height - height * v / peak:.2f}" for i, v in enumerate(values)
    )
    return [
        f'<polyline points="{pts}" fill="none" stroke="{color}" stroke-width="1.2">'
        f"<title>{label}, peak {human(peak)}</title></polyline>"
    ]


def node_section(report):
    n = report["buckets"]
    interval_us = report["interval_us"]
    per_s = 1e6 / interval_us
    # Scans have the largest plan ids: show them first, the result sink last.
    groups = sorted(
        report["groups"], key=lambda g: (g["plan_id"] is None, -(g["plan_id"] or 0), g["processor"])
    )
    # Groups that only waited for input and recorded nothing are left out.
    groups = [
        g
        for g in groups
        if g["stats"]
        or any(sum(v) > 0 for k, v in g["state_us"].items() if k not in ("NeedData", "Unscheduled"))
    ]

    busy = [0.0] * n
    queued = [0.0] * n
    scan = [0.0] * n
    sent = [0.0] * n
    acked = [0.0] * n
    rtt = [0.0] * n
    frames = [0.0] * n
    send_wait = [0.0] * n
    held_back = [0.0] * n
    recv_wait = [0.0] * n
    totals = []
    for g in groups:
        for state in ("SyncRunning", "InEvent"):
            for i, v in enumerate(series(g, state, "state_us", n)):
                busy[i] += v / interval_us
        for i, v in enumerate(series(g, "SyncQueued", "state_us", n)):
            queued[i] += v / interval_us
        for name, acc in [
            ("ScanBytes", scan),
            ("ExchangeBytes", sent),
            ("ExchangeAckedBytes", acked),
            ("ExchangeFrameRoundTripTime", rtt),
            ("ExchangeFramesAcked", frames),
            ("ExchangeSendQueueWaitTime", send_wait),
            ("ExchangeHeldBackTime", held_back),
            ("ExchangeReceiveQueueWaitTime", recv_wait),
        ]:
            for i, v in enumerate(series(g, name, "stats", n)):
                acc[i] += v
        totals.append(
            {s: sum(series(g, s, "state_us", n)) / 1e6 for s in STATES}
        )
    mean_rtt_ms = [r / f / 1e6 if f else 0 for r, f in zip(rtt, frames)]

    out = []
    title = f'node {report["node_id"]} · graph {report["graph_seq"]}'
    out.append(f"<h2>{html.escape(title)}</h2>")
    out.append(
        f'<p class="meta">start {report["start_unix_ms"]} ms · bucket {interval_us / 1000:g} ms · '
        f'{n} buckets · max_threads {report["max_threads"]}</p>'
    )

    summary = [
        ("worker threads running / queued", None),
        ("scan bytes/s", scan),
        ("exchange bytes queued/s", sent),
        ("exchange bytes answered/s", acked),
        ("mean frame round trip ms", mean_rtt_ms),
        # Recorded when a wait ends, so a long wait shows up in the bucket it ended in.
        ("senders waiting for send queue, s/s", [v / 1e9 for v in send_wait]),
        ("queued data held back by frames in flight, s/s", [v / 1e9 for v in held_back]),
        ("receivers waiting for receive queue, s/s", [v / 1e9 for v in recv_wait]),
    ]
    out.append('<table class="tl">')
    for label, values in summary:
        svg = []
        if values is None:
            svg += line(busy, n, COLORS["SyncRunning"], ROW_H, "threads running")
            svg += line(queued, n, COLORS["SyncQueued"], ROW_H, "processors queued")
            peak = f"run peak {max(busy):.1f}, queued peak {max(queued):.1f}"
        else:
            scale = per_s if label.endswith("/s") or label.endswith("s/s") else 1
            scaled = [v * scale for v in values]
            svg += line(scaled, n, "#2d3748", ROW_H, label)
            peak = f"peak {human(max(scaled) if scaled else 0)}"
        out.append(
            f'<tr><td class="label">{html.escape(label)}<br><span>{peak}</span></td>'
            f'<td><svg width="{WIDTH}" height="{ROW_H}">{"".join(svg)}</svg></td></tr>'
        )
    out.append("</table>")

    out.append('<table class="tl">')
    for g, total in zip(groups, totals):
        rows = [v * per_s for v in series(g, "OutputRows", "stats", n)]
        svg = stacked(g, n, interval_us) + line(rows, n, "#1a202c", ROW_H, "output rows/s")
        plan = f'#{g["plan_id"]} {g["plan_name"]}' if g["plan_id"] is not None else "(no plan)"
        shares = " ".join(
            f'<b style="color:{COLORS[s]}">{s[:6]}</b> {total[s]:.1f}s' for s in STATES if total[s] > 0.05
        )
        out.append(
            f'<tr><td class="label">{html.escape(plan)}<br>{html.escape(g["processor"])} ×{g["processors"]}'
            f"<br><span>{shares}</span></td>"
            f'<td><svg width="{WIDTH}" height="{ROW_H}">{"".join(svg)}</svg></td></tr>'
        )
    out.append("</table>")
    return "\n".join(out)


LINE = re.compile(
    r"\[PIPELINE-TIMELINE\] query_id=(\S+) node_id=(\S+) graph=(\d+) start_unix_ms=(\d+) "
    r"interval_us=(\d+) max_threads=(\d+) buckets=(\d+) offset=(\d+) group=(\{.*\})\s*$"
)


def read_reports(paths):
    """Rebuild one report per (node, graph) from the log lines."""
    reports = {}
    for path in paths:
        for text in open(path, errors="replace"):
            m = LINE.search(text)
            if not m:
                continue
            qid, node, graph, start, interval, threads, buckets, offset = m.groups()[:8]
            part = json.loads(m.group(9))
            key = (qid, node, int(graph))
            report = reports.setdefault(
                key,
                {
                    "query_id": qid,
                    "node_id": node,
                    "graph_seq": int(graph),
                    "start_unix_ms": int(start),
                    "interval_us": int(interval),
                    "max_threads": int(threads),
                    "buckets": int(buckets),
                    "groups": {},
                },
            )
            gkey = (part["plan_id"], part["plan_name"], part["processor"])
            group = report["groups"].setdefault(
                gkey,
                {k: part[k] for k in ("plan_id", "plan_name", "processor", "title", "processors")}
                | {"state_us": {}, "stats": {}},
            )
            offset = int(offset)
            for kind in ("state_us", "stats"):
                for name, values in part[kind].items():
                    series = group[kind].setdefault(name, [0] * int(buckets))
                    series[offset : offset + len(values)] = values
    out = []
    for report in reports.values():
        report["groups"] = list(report["groups"].values())
        out.append(report)
    return out


def main():
    if len(sys.argv) < 3:
        print(__doc__)
        sys.exit(1)
    reports = read_reports(sys.argv[2:])
    if not reports:
        sys.exit("no [PIPELINE-TIMELINE] lines found")
    qids = {r["query_id"] for r in reports}
    if len(qids) > 1:
        sys.exit(f"lines of several queries: {sorted(qids)}")
    reports.sort(key=lambda r: (r["node_id"], r["graph_seq"]))
    legend = " ".join(
        f'<span class="key" style="background:{COLORS[s]}"></span>{s}' for s in STATES
    )
    page = f"""<!doctype html>
<html><head><meta charset="utf-8"><title>Pipeline timeline</title>
<style>
body {{ background:#fff; color:#1a202c; font:12px -apple-system, sans-serif; margin:16px; }}
table.tl {{ border-collapse:collapse; margin-bottom:12px; }}
table.tl td {{ border-bottom:1px solid #edf2f7; padding:2px 6px; vertical-align:middle; }}
td.label {{ width:{LABEL_W}px; font-size:11px; }}
td.label span {{ color:#4a5568; font-size:10px; }}
.key {{ display:inline-block; width:10px; height:10px; margin:0 3px 0 10px; }}
.meta {{ color:#4a5568; }}
</style></head><body>
<h1>Pipeline timeline · {html.escape(reports[0]["query_id"])}</h1>
<p>{legend} · black line: output rows/s (row scale per group)</p>
{"".join(node_section(r) for r in reports)}
</body></html>"""
    with open(sys.argv[1], "w") as f:
        f.write(page)


if __name__ == "__main__":
    main()
