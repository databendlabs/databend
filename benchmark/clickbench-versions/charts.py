"""Draw the comparison charts as plain SVG from the committed results; no third-party packages.

usage: charts.py        writes charts/{speedup-vs-v0.9.53,speedup-same-format,speedup-vs-967,peak-rss,summary}.svg
Definitions match report.py: per query and release, cold = mean of run 1 over rounds,
hot = min over rounds of min(run 2, run 3), peak = max RSS over all runs. The same-format
series comes from probes/format.json, where both sides are measured by the probe itself
(min of 4 hot runs after a warm-up), so it is only compared with itself."""
import json, math, statistics
from pathlib import Path
from xml.sax.saxutils import escape

ROOT = Path(__file__).resolve().parent
NAMES = list(json.loads((ROOT / 'releases.json').read_text()))
ROUNDS = [json.loads(p.read_text()) for p in sorted((ROOT / 'results').glob('round*.json'))]
FORMAT = json.loads((ROOT / 'probes' / 'format.json').read_text())
ALL_SQL = [q.strip() for q in (ROOT / 'clickbench' / 'queries.sql').read_text().strip().split('\n') if q.strip()]
OUT = ROOT / 'charts'
BASE, OLD, NEW = 'v0.9.53', 'official-967', 'official-main-prs'
COLORS = ['#4e79a7', '#f28e2b', '#59a14f', '#e15759', '#b07aa1']
FONT = 'font-family="Helvetica, Arial, sans-serif"'


def agg(n, q):
    cs = [R[f'{n}/{q}'] for R in ROUNDS]
    return {'cold': statistics.mean(c['runs'][0]['s'] for c in cs),
            'hot': min(min(c['runs'][1]['s'], c['runs'][2]['s']) for c in cs),
            'peak': max(x['peak_mib'] for c in cs for x in c['runs'])}


class Svg:
    def __init__(self, w, h):
        self.w, self.h, self.items = w, h, []

    def add(self, s):
        self.items.append(s)

    def text(self, x, y, s, size=12, anchor='start', weight='normal', rotate=None, fill='#222'):
        tr = f' transform="rotate({rotate} {x:.1f} {y:.1f})"' if rotate is not None else ''
        self.add(f'<text x="{x:.1f}" y="{y:.1f}" font-size="{size}" text-anchor="{anchor}" font-weight="{weight}" '
                 f'fill="{fill}" {FONT}{tr}>{escape(s)}</text>')

    def line(self, x1, y1, x2, y2, stroke='#ccc', width=1, dash=None):
        d = f' stroke-dasharray="{dash}"' if dash else ''
        self.add(f'<line x1="{x1:.1f}" y1="{y1:.1f}" x2="{x2:.1f}" y2="{y2:.1f}" stroke="{stroke}" stroke-width="{width}"{d}/>')

    def rect(self, x, y, w, h, fill, title=None):
        t = f'<title>{escape(title)}</title>' if title else ''
        self.add(f'<rect x="{x:.1f}" y="{y:.1f}" width="{max(w, 0.5):.1f}" height="{max(h, 0.5):.1f}" fill="{fill}">{t}</rect>')

    def legend(self, x, y, labels):
        for i, (label, color) in enumerate(labels):
            self.rect(x, y + i * 18 - 10, 12, 12, color)
            self.text(x + 18, y + i * 18, label, 12)

    def save(self, name, *, out=OUT):
        out.mkdir(exist_ok=True)
        body = '\n'.join(self.items)
        (out / name).write_text(f'<svg xmlns="http://www.w3.org/2000/svg" width="{self.w}" height="{self.h}" '
                                f'viewBox="0 0 {self.w} {self.h}">\n<rect width="100%" height="100%" fill="white"/>\n{body}\n</svg>\n')

def grouped_bars(svg, left, top, width, height, series, ys, ylog, ylabel, xlabels, baseline=None):
    """series: [(label, color, [value or None per x])]; ys: tick values; ylog: log scale."""
    lo, hi = ys[0], ys[-1]
    if ylog:
        pos = lambda v: top + height * (1 - (math.log(v) - math.log(lo)) / (math.log(hi) - math.log(lo)))
    else:
        pos = lambda v: top + height * (1 - (v - lo) / (hi - lo))
    clamp = lambda v: min(max(v, lo), hi)
    for t in ys:
        svg.line(left, pos(t), left + width, pos(t), '#e5e5e5')
        svg.text(left - 6, pos(t) + 4, f'{t:g}', 11, 'end', fill='#555')
    svg.text(left - 46, top + height / 2, ylabel, 12, 'middle', rotate=-90)
    n = len(xlabels)
    slot = width / n
    bw = slot * 0.8 / len(series)
    ref = pos(baseline) if baseline is not None else pos(lo)
    for i, xl in enumerate(xlabels):
        x0 = left + i * slot + slot * 0.1
        for j, (label, color, vals) in enumerate(series):
            v = vals[i]
            if v is None:
                continue
            y = pos(clamp(v))
            svg.rect(x0 + j * bw, min(y, ref), bw - 0.5, abs(ref - y), color, f'{xl} {label}: {v:.3g}')
        svg.text(left + i * slot + slot / 2, top + height + 14, xl, 10, 'middle', fill='#333')
    if baseline is not None:
        svg.line(left, ref, left + width, ref, '#333', 1.2)
    svg.line(left, top, left, top + height, '#999')
    svg.line(left, top + height, left + width, top + height, '#999')


def speedup_chart(fname, title, subtitle, rows, note, *, out=OUT, neutral_label='within noise'):
    """One horizontal bar per query, sorted from biggest speedup to biggest slowdown.

    rows: [(label, baseline_s, new_s, significant)]. A bar goes left (green) when the new
    build is faster and right (red) when it is slower; its length is log2 of the speedup, and
    its label says "N× faster" / "N× slower" with both times. Grey bars do not meet the
    caller's significance threshold."""
    rows = sorted(rows, key=lambda r: r[2] / r[1])
    span = max(abs(math.log2(r[2] / r[1])) for r in rows)
    span = max(1.0, math.ceil(span))
    row_h, top, label_w, bar_w = 18, 92, 380, 560
    mid = label_w + bar_w / 2
    width, height = label_w + bar_w + 230, top + row_h * len(rows) + 44
    unit = (bar_w / 2) / span
    svg = Svg(width, height)
    svg.text(16, 26, title, 16, weight='bold')
    svg.text(16, 46, subtitle, 12, fill='#444')
    svg.rect(16, 58, 12, 12, '#59a14f'); svg.text(34, 68, 'faster', 12)
    svg.rect(90, 58, 12, 12, '#e15759'); svg.text(108, 68, 'slower', 12)
    svg.rect(166, 58, 12, 12, '#bbbbbb'); svg.text(184, 68, neutral_label, 12)
    for k in range(1, int(span) + 1):
        for sign, word in ((-1, 'faster'), (1, 'slower')):
            x = mid + sign * k * unit
            svg.line(x, top - 6, x, top + row_h * len(rows), '#eeeeee')
            svg.text(x, top - 10, f'{2 ** k}× {word}', 10, 'middle', fill='#777')
    for i, (label, base, new, significant) in enumerate(rows):
        y = top + i * row_h
        r = new / base
        lg = math.log2(r)
        color = '#bbbbbb' if not significant else ('#59a14f' if r < 1 else '#e15759')
        x0, x1 = sorted((mid, mid + lg * unit))
        svg.rect(x0, y + 3, x1 - x0, row_h - 6, color, f'{label}: {base * 1000:.1f} ms -> {new * 1000:.1f} ms')
        svg.text(label_w - 8, y + 13, label, 11, 'end')
        factor = 1 / r if r < 1 else r
        word = 'faster' if r < 1 else 'slower'
        svg.text(label_w + bar_w + 8, y + 13, f'{factor:.2f}× {word}   {base * 1000:.0f} → {new * 1000:.0f} ms', 11,
                 fill='#222' if significant else '#888')
    svg.line(mid, top - 4, mid, top + row_h * len(rows), '#333', 1.2)
    svg.text(16, height - 14, note, 11, fill='#555')
    svg.save(fname, out=out)


def query_label(q, n=52):
    sql = ALL_SQL[q].rstrip(';')
    return f'Q{q:02d}  ' + (sql if len(sql) <= n else sql[:n - 1] + '…')


def consistent(a, b, q):
    """Same rule as report.py: every round moves the same way by > 5% and > 5 ms."""
    s = set()
    for R in ROUNDS:
        ca, cb = R[f'{a}/{q}'], R[f'{b}/{q}']
        x, y = min(ca['runs'][1]['s'], ca['runs'][2]['s']), min(cb['runs'][1]['s'], cb['runs'][2]['s'])
        if abs(x - y) <= 0.005 or abs(x / y - 1) <= 0.05:
            return False
        s.add(x < y)
    return len(s) == 1


def speedups():
    for base, fname, title, subtitle in (
            (BASE, 'speedup-vs-v0.9.53.svg', 'official-main-prs vs v0.9.53, as ClickBench runs them',
             'Hot time per query. v0.9.53 reads its default Native format; 1.x can only use Parquet.'),
            (OLD, 'speedup-vs-967.svg', 'official-main-prs vs official-967: effect of the recent PRs',
             'Hot time per query. Same build parameters; main + #21138 + #21144 against the v1.2.967 commit.')):
        rows = [(query_label(q), agg(base, q)['hot'], agg(NEW, q)['hot'], consistent(NEW, base, q)) for q in range(43)]
        speedup_chart(fname, title, subtitle, rows,
                      'Source: results/round*.json (2 rounds). Grey: not faster/slower by > 5% and > 5 ms in both rounds.')
    rows = []
    for q in FORMAT['queries']:
        b, n = FORMAT['v0.9.53 parquet'][str(q)], FORMAT['main-prs parquet'][str(q)]
        rows.append((query_label(q), b, n, abs(n / b - 1) > 0.05 and abs(n - b) > 0.005))
    speedup_chart('speedup-same-format.svg', 'official-main-prs vs v0.9.53, both reading Parquet',
                  'Hot time per query for the 17 queries probed. v0.9.53 reads a Parquet copy of hits, so the format is the same.',
                  rows, 'Source: probes/format.json (fastest of 4 hot runs). Grey: within 5% or 5 ms.')


def peak_rss():
    qs = list(range(43))
    a = [agg(OLD, q)['peak'] / 1024 for q in qs]
    b = [agg(NEW, q)['peak'] / 1024 for q in qs]
    top = math.ceil(max(a + b) / 2) * 2
    svg = Svg(1500, 440)
    svg.text(750, 26, 'Peak RSS per query (GiB, max over all runs): official-967 vs official-main-prs', 15, 'middle', 'bold')
    grouped_bars(svg, 70, 70, 1400, 300, [(OLD, COLORS[1], a), (NEW, COLORS[0], b)],
                 list(range(0, top + 1, 2)), False, 'GiB', [f'Q{q}' for q in qs])
    svg.legend(1250, 48, [(OLD, COLORS[1])])
    svg.legend(1360, 48, [(NEW, COLORS[0])])
    svg.text(70, 420, 'Source: results/round*.json; RSS sampled every 5 ms from /proc/PID/statm.', 11, fill='#555')
    svg.save('peak-rss.svg')


def summary():
    cold = [sum(agg(n, q)['cold'] for q in range(43)) for n in NAMES]
    hot = [sum(agg(n, q)['hot'] for q in range(43)) for n in NAMES]
    svg = Svg(900, 420)
    svg.text(450, 26, 'Sum over the 43 queries (seconds)', 15, 'middle', 'bold')
    left, topy, width, height = 70, 70, 800, 280
    ymax = math.ceil(max(cold) / 10) * 10
    grouped_bars(svg, left, topy, width, height, [('cold', COLORS[1], cold), ('hot', COLORS[0], hot)],
                 list(range(0, ymax + 1, 10)), False, 'seconds', NAMES)
    slot = width / len(NAMES)
    bw = slot * 0.8 / 2
    for i in range(len(NAMES)):
        for j, v in enumerate((cold[i], hot[i])):
            x = left + i * slot + slot * 0.1 + j * bw + bw / 2
            svg.text(x, topy + height * (1 - v / ymax) - 4, f'{v:.2f}', 11, 'middle')
    svg.legend(740, 48, [('cold (run 1)', COLORS[1])])
    svg.legend(740, 66, [('hot (min of runs 2-3)', COLORS[0])])
    svg.text(70, 400, 'Source: results/round*.json, 2 rounds; cold = mean over rounds, hot = min over rounds.', 11, fill='#555')
    svg.save('summary.svg')


if __name__ == '__main__':
    speedups(); peak_rss(); summary()
    print('charts written:', sorted(p.name for p in OUT.glob('*.svg')))
