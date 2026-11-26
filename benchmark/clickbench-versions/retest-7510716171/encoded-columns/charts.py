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
OLD = 'v0.9.53'
THREE_WAY_LABELS = ('v0.9.53 (Native)', 'main (default)', 'main (encoded only)')
THREE_WAY_COLORS = (COLORS[1], COLORS[0], COLORS[2])
CROSS_RUN_NOTE = 'v0.9.53 (Native) is from the version retest; both main runs read the same Parquet snapshot in the settings retest.'


def hot(cell):
    return min(run['s'] for run in cell['runs'][1:])


def stable(baseline, encoded):
    deltas = [e - b for b, e in zip(baseline, encoded)]
    thresholds = [max(0.005, b * 0.05) for b in baseline]
    return any(all(sign * d > t for d, t in zip(deltas, thresholds)) for sign in (-1, 1))


def load_rounds(root):
    return [json.loads((root / 'results' / f'round{i}.json').read_text()) for i in range(2)]


def summarize(rounds, arms, cell_key):
    cold, warm, peak = {}, {}, {}
    for arm in arms:
        for q in QUERIES:
            key = (arm, q)
            cells = [r[cell_key(arm, q)] for r in rounds]
            cold[key] = statistics.mean(c['runs'][0]['s'] for c in cells)
            warm[key] = [hot(c) for c in cells]
            peak[key] = max(run['peak_mib'] for c in cells for run in c['runs']) / 1024
    return cold, warm, peak


def main():
    cold, warm, peak = summarize(load_rounds(ROOT), ARMS, lambda a, q: f'{a}/Q{q:02d}')
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

    rss_chart(ARMS, LABELS, (COLORS[1], COLORS[0]), peak, 'encoded columns only vs default',
              'Source: results/round*.json; RSS sampled every 5 ms from /proc/PID/statm.',
              'peak-rss.svg', (1190, 1280))
    sums_chart(ARMS, LABELS, cold, warm, 'encoded columns only vs default',
               'Source: results/round*.json; cold and hot times averaged across two rounds before summing.',
               'summary.svg')

    old_cold, old_warm, old_peak = summarize(load_rounds(ROOT.parent), (OLD,), lambda a, q: f'{a}/{q}')
    arms = (OLD, *ARMS)
    cold, warm, peak = old_cold | cold, old_warm | warm, old_peak | peak
    three_way_speedups(warm)
    rss_chart(arms, THREE_WAY_LABELS, THREE_WAY_COLORS, peak, 'three-way comparison',
              CROSS_RUN_NOTE, 'peak-rss-three-way.svg', (1050, 1185, 1315))
    sums_chart(arms, THREE_WAY_LABELS, cold, warm, 'three-way comparison',
               CROSS_RUN_NOTE, 'summary-three-way.svg')


def rss_chart(arms, labels, colors, peak, title, note, filename, legend_x):
    svg = Svg(1500, 440)
    svg.text(750, 26, f'Peak RSS per query (GiB, max over all runs): {title}', 15, 'middle', 'bold')
    series = [(label, color, [peak[arm, q] for q in QUERIES])
              for arm, label, color in zip(arms, labels, colors)]
    ymax = math.ceil(max(peak.values()) / 2) * 2
    grouped_bars(svg, 70, 80, 1400, 295, series, list(range(0, ymax + 1, 2)),
                 False, 'GiB', [f'Q{q:02d}' for q in QUERIES])
    for x, label, color in zip(legend_x, labels, colors):
        svg.legend(x, 55, [(label, color)])
    svg.text(70, 420, note, 11, fill='#555')
    svg.save(filename, out=OUT)


def sums_chart(arms, labels, cold, warm, title, note, filename):
    cold_sums = [sum(cold[arm, q] for q in QUERIES) for arm in arms]
    hot_sums = [sum(statistics.mean(warm[arm, q]) for q in QUERIES) for arm in arms]
    svg = Svg(900, 420)
    svg.text(450, 26, f'Sum over the 43 queries (seconds): {title}', 15, 'middle', 'bold')
    left, top, width, height = 70, 85, 800, 265
    ymax = math.ceil(max(cold_sums + hot_sums) / 5) * 5 + 5
    grouped_bars(svg, left, top, width, height,
                 [('cold', COLORS[1], cold_sums), ('hot', COLORS[0], hot_sums)],
                 list(range(0, ymax + 1, 5)), False, 'seconds', labels)
    slot = width / len(arms)
    bar_width = slot * 0.8 / 2
    for i, values in enumerate(zip(cold_sums, hot_sums)):
        for j, value in enumerate(values):
            x = left + slot * (i + 0.1) + bar_width * (j + 0.5)
            y = top + height * (1 - value / ymax) - 4
            svg.text(x, y, f'{value:.2f}', 11, 'middle')
    svg.legend(675, 53, [('cold (run 1)', COLORS[1]), ('hot (min of runs 2–3)', COLORS[0])])
    svg.text(70, 400, note, 11, fill='#555')
    svg.save(filename, out=OUT)


def three_way_speedups(warm):
    """Plot both main/old ratios on the same axis; also show all three hot times."""
    arms = (OLD, *ARMS)
    times = {q: [statistics.mean(warm[arm, q]) for arm in arms] for q in QUERIES}
    ratios = {q: [math.log2(t / old) for t in (default, enc)] for q, (old, default, enc) in times.items()}
    span = max(1, math.ceil(max(abs(ratio) for pair in ratios.values() for ratio in pair)))
    order = sorted(QUERIES, key=lambda q: ratios[q][1])
    width, top, row_h = 1300, 100, 22
    left, mid, right = 380, 640, 900
    bottom = top + row_h * len(order)
    svg = Svg(width, bottom + 37)
    svg.text(16, 26, 'ClickBench hot time per query: v0.9.53, main, and main with encoded columns', 16, weight='bold')
    svg.text(16, 46, 'Bars show each main variant relative to v0.9.53 (1×); values at right are mean hot times in ms.', 12, fill='#444')
    svg.legend(16, 68, [(THREE_WAY_LABELS[1], THREE_WAY_COLORS[1])])
    svg.legend(158, 68, [(THREE_WAY_LABELS[2], THREE_WAY_COLORS[2])])
    svg.text(310, 68, 'Sorted by encoded vs v0.9.53', 12, fill='#555')
    for k in range(-span, span + 1):
        x = mid + (right - mid) * k / span
        svg.line(x, top - 4, x, bottom, '#eee')
        label = '1×' if k == 0 else f'{2 ** abs(k)}× {"faster" if k < 0 else "slower"}'
        svg.text(x, top - 9, label, 10, 'middle', fill='#666')
    value_x = (963, 1092, 1229)
    for x, label in zip(value_x, ('v0.9.53 (ms)', 'main (ms)', 'main encoded (ms)')):
        svg.text(x, top - 9, label, 11, 'middle', 'bold')
    for i, q in enumerate(order):
        y = top + i * row_h
        if i % 2 == 0:
            svg.rect(0, y, width, row_h, '#f5f5f5')
        svg.text(left - 8, y + 15, query_label(q), 11, 'end')
        for j, (ratio, color) in enumerate(zip(ratios[q], THREE_WAY_COLORS[1:])):
            end = mid + (right - mid) * ratio / span
            factor = 2 ** abs(ratio)
            direction = 'faster' if ratio < 0 else 'slower' if ratio > 0 else 'equal'
            svg.rect(min(mid, end), y + 3 + 8 * j, abs(end - mid), 7, color,
                     f'{THREE_WAY_LABELS[j + 1]}: {factor:.2f}× {direction} vs v0.9.53')
        for x, t in zip(value_x, times[q]):
            svg.text(x, y + 15, f'{t * 1000:.1f}', 11, 'middle')
    svg.line(mid, top - 4, mid, bottom, '#333', 1.2)
    svg.text(16, bottom + 24, CROSS_RUN_NOTE, 11, fill='#555')
    svg.save('speedup-three-way.svg', out=OUT)


if __name__ == '__main__':
    main()
