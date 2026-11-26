"""Regenerate the encoded-columns comparison charts from the archived timings."""
import json
import math
import statistics
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT.parents[1]))
from charts import COLORS, Svg, grouped_bars, query_label, speedup_chart  # noqa: E402

OUT = ROOT / 'charts'
ARMS = ('default', 'encoded')
LABELS = ('Default', 'Encoded columns only')
QUERIES = range(43)


def hot(cell):
    return min(run['s'] for run in cell['runs'][1:])


def stable(baseline, encoded):
    deltas = [e - b for b, e in zip(baseline, encoded)]
    thresholds = [max(0.005, b * 0.05) for b in baseline]
    return any(all(sign * d > t for d, t in zip(deltas, thresholds)) for sign in (-1, 1))


def main():
    rounds = [json.loads((ROOT / 'results' / f'round{i}.json').read_text()) for i in range(2)]
    cold, warm, peak = {}, {}, {}
    for arm in ARMS:
        for q in QUERIES:
            key = (arm, q)
            cells = [r[f'{arm}/Q{q:02d}'] for r in rounds]
            cold[key] = statistics.mean(c['runs'][0]['s'] for c in cells)
            warm[key] = [hot(c) for c in cells]
            peak[key] = max(run['peak_mib'] for c in cells for run in c['runs']) / 1024

    rows = []
    for q in QUERIES:
        base, enc = [warm[arm, q] for arm in ARMS]
        rows.append((query_label(q), statistics.mean(base), statistics.mean(enc), stable(base, enc)))
    speedup_chart(
        'speedup-vs-default.svg', 'Encoded columns only vs default on main',
        'Mean per-query hot time over two rounds on the same Parquet snapshot; only enable_encoded_columns changes.',
        rows, 'Grey: the delta does not exceed both 5% and 5 ms in the same direction in both rounds.',
        out=OUT, neutral_label='not consistent',
    )

    svg = Svg(1500, 440)
    svg.text(750, 26, 'Peak RSS per query (GiB, max over all runs): encoded columns only vs default',
             15, 'middle', 'bold')
    series = [(label, color, [peak[arm, q] for q in QUERIES])
              for arm, label, color in zip(ARMS, LABELS, (COLORS[1], COLORS[0]))]
    ymax = math.ceil(max(peak.values()) / 2) * 2
    grouped_bars(svg, 70, 80, 1400, 295, series, list(range(0, ymax + 1, 2)),
                 False, 'GiB', [f'Q{q:02d}' for q in QUERIES])
    svg.legend(1190, 55, [(LABELS[0], COLORS[1])])
    svg.legend(1280, 55, [(LABELS[1], COLORS[0])])
    svg.text(70, 420, 'Source: results/round*.json; RSS sampled every 5 ms from /proc/PID/statm.', 11, fill='#555')
    svg.save('peak-rss.svg', out=OUT)

    cold_sums = [sum(cold[arm, q] for q in QUERIES) for arm in ARMS]
    hot_sums = [sum(statistics.mean(warm[arm, q]) for q in QUERIES) for arm in ARMS]
    svg = Svg(900, 420)
    svg.text(450, 26, 'Sum over the 43 queries (seconds): encoded columns only vs default',
             15, 'middle', 'bold')
    left, top, width, height = 70, 85, 800, 265
    ymax = math.ceil(max(cold_sums + hot_sums) / 5) * 5 + 5
    grouped_bars(svg, left, top, width, height,
                 [('cold', COLORS[1], cold_sums), ('hot', COLORS[0], hot_sums)],
                 list(range(0, ymax + 1, 5)), False, 'seconds', LABELS)
    slot = width / len(ARMS)
    bar_width = slot * 0.8 / 2
    for i, values in enumerate(zip(cold_sums, hot_sums)):
        for j, value in enumerate(values):
            x = left + slot * (i + 0.1) + bar_width * (j + 0.5)
            y = top + height * (1 - value / ymax) - 4
            svg.text(x, y, f'{value:.2f}', 11, 'middle')
    svg.legend(675, 53, [('cold (run 1)', COLORS[1]), ('hot (min of runs 2–3)', COLORS[0])])
    svg.text(70, 400, 'Source: results/round*.json; cold and hot times averaged across two rounds before summing.', 11, fill='#555')
    svg.save('summary.svg', out=OUT)


if __name__ == '__main__':
    main()
