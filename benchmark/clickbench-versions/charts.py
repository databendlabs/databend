"""Draw the comparison charts as plain SVG from the committed results; no third-party packages.

usage: charts.py        writes charts/{hot-ratio,peak-rss,summary}.svg
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

    def save(self, name):
        OUT.mkdir(exist_ok=True)
        body = '\n'.join(self.items)
        (OUT / name).write_text(f'<svg xmlns="http://www.w3.org/2000/svg" width="{self.w}" height="{self.h}" '
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


def hot_ratio():
    qs = list(range(43))
    new = {q: agg(NEW, q)['hot'] for q in qs}
    s1 = [new[q] / agg(BASE, q)['hot'] for q in qs]
    s2 = [FORMAT['main-prs parquet'][str(q)] / FORMAT['v0.9.53 parquet'][str(q)] if str(q) in FORMAT['main-prs parquet'] else None for q in qs]
    s3 = [new[q] / agg(OLD, q)['hot'] for q in qs]
    svg = Svg(1500, 470)
    svg.text(750, 26, 'Hot time ratio per query: official-main-prs / baseline (log scale; below 1 = main-prs faster)', 15, 'middle', 'bold')
    grouped_bars(svg, 70, 80, 1400, 320, [
        ('vs v0.9.53 as ClickBench runs it (v0.9.53 reads Native)', COLORS[3], s1),
        ('vs v0.9.53 on the same format (both Parquet; 17 queries probed)', COLORS[2], s2),
        ('vs official-967 (same build parameters)', COLORS[0], s3),
    ], [0.03, 0.1, 0.25, 0.5, 1, 2, 4], True, 'ratio', [f'Q{q}' for q in qs], baseline=1)
    svg.legend(80, 48, [('vs v0.9.53 as ClickBench runs it (v0.9.53 reads Native)', COLORS[3])])
    svg.legend(560, 48, [('vs v0.9.53 on the same format (both Parquet; 17 queries probed)', COLORS[2])])
    svg.legend(1090, 48, [('vs official-967 (same build parameters)', COLORS[0])])
    svg.text(70, 450, 'Sources: results/round*.json (red, blue) and probes/format.json (green). Bars are clamped to 0.03-4.', 11, fill='#555')
    svg.save('hot-ratio.svg')


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
    hot_ratio(); peak_rss(); summary()
    print('charts written:', sorted(p.name for p in OUT.glob('*.svg')))
