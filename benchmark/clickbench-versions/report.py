"""Markdown report from manifest.json, /tmp/cb-versions/*/load.json and results/round*.json.

usage: report.py > report.md
Per query and release: cold = mean of run 1 over rounds; hot = min over rounds of min(run 2, run 3);
peak = max RSS over all runs. A change counts as consistent only when every round moves the
same way by more than 5% and by more than 5 ms."""
import json, math, statistics
from pathlib import Path

ROOT = Path(__file__).resolve().parent
M = json.loads((ROOT / 'manifest.json').read_text())
NAMES = list(json.loads((ROOT / 'releases.json').read_text()))
ROUNDS = [json.loads(p.read_text()) for p in sorted((ROOT / 'results').glob('round*.json'))]
LOADS = {n: json.loads((ROOT / 'loads' / f'{n}.json').read_text()) for n in NAMES}  # copied from /tmp/cb-versions/NAME/load.json
BASE, OLD, NEW, REF = 'v0.9.53', 'official-967', 'official-main-prs', 'v1.2.967'


def cell(r, n, q):
    c = ROUNDS[r].get(f'{n}/{q}')
    if not c or any('error' in x for x in c['runs']) or len(c['runs']) < 3:
        return None
    return c


def hot_r(r, n, q):
    c = cell(r, n, q); return c and min(c['runs'][1]['s'], c['runs'][2]['s'])


def cold_r(r, n, q):
    c = cell(r, n, q); return c and c['runs'][0]['s']


def agg(n, q):
    cs = [cell(r, n, q) for r in range(len(ROUNDS))]
    if not all(cs):
        return None
    return {'cold': statistics.mean(c['runs'][0]['s'] for c in cs),
            'hot': min(min(c['runs'][1]['s'], c['runs'][2]['s']) for c in cs),
            'peak': max(x['peak_mib'] for c in cs for x in c['runs'])}


Q = range(43)
OK = [q for q in Q if all(agg(n, q) for n in NAMES)]
geo = lambda xs: math.exp(statistics.mean(math.log(x) for x in xs))


def consistent(a, b, q):
    """+1 if `a` is faster than `b` in every round, -1 if slower in every round, else 0."""
    s = set()
    for r in range(len(ROUNDS)):
        x, y = hot_r(r, a, q), hot_r(r, b, q)
        if abs(x - y) <= 0.005 or abs(x / y - 1) <= 0.05:
            return 0
        s.add(1 if x < y else -1)
    return s.pop() if len(s) == 1 else 0


out = []
p = out.append
p('## Environment\n')
mc = M['machine']
p(f"- Recorded {M['created']}; CPU {mc['cpu'].split(':', 1)[1].strip()}; kernel {mc['kernel']}; governor `{mc['governor']}`; THP `{mc['thp'].splitlines()[0]}`")
p(f"- Pinned with `taskset -c {mc['cpu_pinning']}`; cgroup cpu.max `{mc['cgroup_cpu_max']}`, memory.max `{mc['cgroup_mem_max']}`; drop_caches writable: {mc['drop_caches_writable']}")
p(f"- Source data `{M['source_data']['path']}`, {M['source_data']['bytes']} bytes, sha256 `{M['source_data']['sha256'][:16]}…`")
p(f"- {len(ROUNDS)} rounds; before each cell the harness waits until host CPU use is below 10% (1 s sample, at most 300 s); the highest observed was {max(c['busy_before'] for R in ROUNDS for c in R.values()):.1f}%\n")
p('| release | source | databend-query sha256 | `--version` |')
p('|---|---|---|---|')
for n in NAMES:
    r = M['releases'][n]
    p(f"| {n} | {r['source']} | `{r['databend-query']['sha256'][:12]}` | `{r['databend-query']['version']}` |")
p('\n## Load\n')
p('| release | COPY | INSERT … SELECT | total | rows | blocks | data size | non-default settings |')
p('|---|---:|---:|---:|---:|---:|---:|---|')
for n in NAMES:
    L = LOADS[n]; nd = L['non_default_settings']
    p(f"| {n} | {L['copy_s']} s | {L['insert_s']} s | {L['load_s']} s | {L['rows']} | {L['blocks']} | {L['data_bytes'] / 1e9:.2f} GB | {'none' if nd == [] else nd} |")
p(f'\n## Summary ({len(OK)} / 43 queries succeeded on every release)\n')
hdr = ' | '.join(f'hot sum R{r + 1}' for r in range(len(ROUNDS)))
p(f'| release | cold sum | hot sum | {hdr} | Q32 peak RSS | median startup | hot geomean / v0.9.53 | hot geomean / {OLD} |')
p('|---|---:|---:|' + '---:|' * len(ROUNDS) + '---:|---:|---:|---:|')
for n in NAMES:
    st = statistics.median(c['startup_s'] for R in ROUNDS for k, c in R.items() if k.startswith(n + '/'))
    rs = ' | '.join(f'{sum(hot_r(r, n, q) for q in OK):.2f} s' for r in range(len(ROUNDS)))
    p(f"| {n} | {sum(agg(n, q)['cold'] for q in OK):.2f} s | {sum(agg(n, q)['hot'] for q in OK):.2f} s | {rs} | "
      f"{agg(n, 32)['peak'] / 1024:.1f} GiB | {st:.2f} s | {geo(agg(n, q)['hot'] / agg(BASE, q)['hot'] for q in OK):.3f} | "
      f"{geo(agg(n, q)['hot'] / agg(OLD, q)['hot'] for q in OK):.3f} |")
if len(ROUNDS) > 1:
    p('\nRound-to-round noise (geomean absolute deviation of the hot-time ratio between the two rounds of a release): ' + ', '.join(
        f"{n} {100 * (geo(max(hot_r(0, n, q), hot_r(1, n, q)) / min(hot_r(0, n, q), hot_r(1, n, q)) for q in OK) - 1):.1f}%" for n in NAMES))
p('\n## Result consistency\n')
p('Row count and result hash (rows sorted) of run 1 of every query. Releases may format numbers differently, so a different hash does not by itself mean a wrong result.\n')
bad = []
for q in Q:
    rows = {n: cell(0, n, q)['runs'][0]['rows'] for n in NAMES if cell(0, n, q)}
    hashes = {}
    for n in NAMES:
        c = cell(0, n, q)
        if c:
            hashes.setdefault(c['runs'][0]['hash'], []).append(n)
        runs_same = c and len({x['hash'] for x in c['runs']}) == 1
        if c and not runs_same:
            bad.append(f'Q{q:02d} {n}: the 3 runs of this release return different results')
    if len(set(rows.values())) > 1:
        bad.append(f'Q{q:02d} row counts differ: {rows}')
    elif len(hashes) > 1:
        bad.append(f'Q{q:02d} hash groups: ' + ' / '.join(', '.join(v) for v in hashes.values()))
p('\n'.join(f'- {b}' for b in bad) if bad else '- Every query has the same row count and hash on every release')


def fmt(a):
    return f"{a['cold']:.3f} / {a['hot']:.3f}"


p('\n## Per query (cold / hot, seconds; ratios are hot)\n')
p('| Q | ' + ' | '.join(NAMES) + f' | {NEW} / {OLD} | {NEW} / {BASE} | {OLD} / {REF} |')
p('|---|' + '---|' * len(NAMES) + '---:|---:|---:|')
for q in Q:
    if q not in OK:
        p(f'| Q{q:02d} | ' + ' | '.join(fmt(agg(n, q)) if agg(n, q) else 'error' for n in NAMES) + ' | | | |'); continue
    h = lambda n: agg(n, q)['hot']
    mark = lambda a, b: {1: ' ↓', -1: ' ↑', 0: ''}[consistent(a, b, q)]
    p(f'| Q{q:02d} | ' + ' | '.join(fmt(agg(n, q)) for n in NAMES) +
      f' | {h(NEW) / h(OLD):.2f}{mark(NEW, OLD)} | {h(NEW) / h(BASE):.2f}{mark(NEW, BASE)} | {h(OLD) / h(REF):.2f}{mark(OLD, REF)} |')
p('\n↓ / ↑: faster / slower in every round by more than 5% and more than 5 ms.\n')

p('## Per-case comparison\n')
for a, b, title in ((NEW, OLD, 'Code change: main + #21138 + #21144 vs the v1.2.967 commit (same build parameters)'),
                    (OLD, REF, 'Build reproduction: 967 built locally with the CI parameters vs the official package'),
                    (NEW, BASE, 'Latest code vs v0.9.53, the release ClickBench runs')):
    fast = [q for q in OK if consistent(a, b, q) == 1]
    slow = [q for q in OK if consistent(a, b, q) == -1]
    p(f'### {title}\n')
    p(f'Hot geomean {geo(agg(a, q)["hot"] / agg(b, q)["hot"] for q in OK):.3f}, hot sum '
      f'{sum(agg(b, q)["hot"] for q in OK):.2f} → {sum(agg(a, q)["hot"] for q in OK):.2f} s, '
      f'peak geomean {geo(agg(a, q)["peak"] / agg(b, q)["peak"] for q in OK):.3f}. '
      f'{len(fast)} queries consistently faster, {len(slow)} consistently slower, {len(OK) - len(fast) - len(slow)} within noise.\n')
    for label, qs in (('faster', fast), ('slower', slow)):
        if qs:
            p(f'| {label} | {b} hot | {a} hot | ratio | {b} peak | {a} peak |')
            p('|---|---:|---:|---:|---:|---:|')
            for q in sorted(qs, key=lambda q: agg(a, q)['hot'] / agg(b, q)['hot']):
                x, y = agg(b, q), agg(a, q)
                p(f"| Q{q:02d} | {x['hot']:.3f} | {y['hot']:.3f} | {y['hot'] / x['hot']:.2f} | {x['peak'] / 1024:.1f} GiB | {y['peak'] / 1024:.1f} GiB |")
            p('')
print('\n'.join(out))
