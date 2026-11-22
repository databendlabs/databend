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
p('## 环境\n')
mc = M['machine']
p(f"- 生成：{M['created']}；CPU：{mc['cpu'].split(':', 1)[1].strip()}；内核 {mc['kernel']}；调频 `{mc['governor']}`；THP `{mc['thp'].splitlines()[0]}`")
p(f"- 绑核 `taskset -c {mc['cpu_pinning']}`；cgroup cpu.max `{mc['cgroup_cpu_max']}`，memory.max `{mc['cgroup_mem_max']}`；drop_caches 可写：{mc['drop_caches_writable']}")
p(f"- 源数据 `{M['source_data']['path']}`，{M['source_data']['bytes']} 字节，sha256 `{M['source_data']['sha256'][:16]}…`")
p(f"- 轮数 {len(ROUNDS)}；每格开始前等主机 CPU 占用 < 10%（1 秒采样，最多等 300 秒），实测最大 {max(c['busy_before'] for R in ROUNDS for c in R.values()):.1f}%\n")
p('| 版本 | 来源 | databend-query sha256 | `--version` |')
p('|---|---|---|---|')
for n in NAMES:
    r = M['releases'][n]
    p(f"| {n} | {r['source']} | `{r['databend-query']['sha256'][:12]}` | `{r['databend-query']['version']}` |")
p('\n## 导入\n')
p('| 版本 | COPY | INSERT … SELECT | 合计 | 行数 | 块数 | 数据大小 | 非默认 settings |')
p('|---|---:|---:|---:|---:|---:|---:|---|')
for n in NAMES:
    L = LOADS[n]; nd = L['non_default_settings']
    p(f"| {n} | {L['copy_s']} s | {L['insert_s']} s | {L['load_s']} s | {L['rows']} | {L['blocks']} | {L['data_bytes'] / 1e9:.2f} GB | {'无' if nd == [] else nd} |")
p(f'\n## 汇总（{len(OK)} / 43 题全部成功）\n')
hdr = ' | '.join(f'热跑总和 R{r + 1}' for r in range(len(ROUNDS)))
p(f'| 版本 | 冷跑总和 | 热跑总和 | {hdr} | Q32 峰值 RSS | 启动中位数 | 热跑几何平均 / v0.9.53 | 热跑几何平均 / {OLD} |')
p('|---|---:|---:|' + '---:|' * len(ROUNDS) + '---:|---:|---:|---:|')
for n in NAMES:
    st = statistics.median(c['startup_s'] for R in ROUNDS for k, c in R.items() if k.startswith(n + '/'))
    rs = ' | '.join(f'{sum(hot_r(r, n, q) for q in OK):.2f} s' for r in range(len(ROUNDS)))
    p(f"| {n} | {sum(agg(n, q)['cold'] for q in OK):.2f} s | {sum(agg(n, q)['hot'] for q in OK):.2f} s | {rs} | "
      f"{agg(n, 32)['peak'] / 1024:.1f} GiB | {st:.2f} s | {geo(agg(n, q)['hot'] / agg(BASE, q)['hot'] for q in OK):.3f} | "
      f"{geo(agg(n, q)['hot'] / agg(OLD, q)['hot'] for q in OK):.3f} |")
if len(ROUNDS) > 1:
    p('\n轮间噪声（同一版本两轮热跑之比的几何平均绝对偏差）：' + '，'.join(
        f"{n} {100 * (geo(max(hot_r(0, n, q), hot_r(1, n, q)) / min(hot_r(0, n, q), hot_r(1, n, q)) for q in OK) - 1):.1f}%" for n in NAMES))
p('\n## 结果一致性\n')
p('每题第 1 次运行的行数与结果哈希（行排序后）。不同版本的数值格式可能不同，哈希不同不一定是结果错。\n')
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
            bad.append(f'Q{q:02d} {n}：同一版本 3 次运行结果不同')
    if len(set(rows.values())) > 1:
        bad.append(f'Q{q:02d} 行数不同：{rows}')
    elif len(hashes) > 1:
        bad.append(f'Q{q:02d} 哈希分组：' + ' / '.join('、'.join(v) for v in hashes.values()))
p('\n'.join(f'- {b}' for b in bad) if bad else '- 所有题在所有版本上行数和哈希都一致')


def fmt(a):
    return f"{a['cold']:.3f} / {a['hot']:.3f}"


p('\n## 逐题（冷 / 热，秒；比值为热跑）\n')
p('| Q | ' + ' | '.join(NAMES) + f' | {NEW} / {OLD} | {NEW} / {BASE} | {OLD} / {REF} |')
p('|---|' + '---|' * len(NAMES) + '---:|---:|---:|')
for q in Q:
    if q not in OK:
        p(f'| Q{q:02d} | ' + ' | '.join(fmt(agg(n, q)) if agg(n, q) else 'error' for n in NAMES) + ' | | | |'); continue
    h = lambda n: agg(n, q)['hot']
    mark = lambda a, b: {1: ' ↓', -1: ' ↑', 0: ''}[consistent(a, b, q)]
    p(f'| Q{q:02d} | ' + ' | '.join(fmt(agg(n, q)) for n in NAMES) +
      f' | {h(NEW) / h(OLD):.2f}{mark(NEW, OLD)} | {h(NEW) / h(BASE):.2f}{mark(NEW, BASE)} | {h(OLD) / h(REF):.2f}{mark(OLD, REF)} |')
p('\n↓ / ↑：每一轮都快 / 慢 5% 以上且超过 5 ms。\n')

p('## 逐 case 对比\n')
for a, b, title in ((NEW, OLD, '代码改动：main + #21138 + #21144 对 v1.2.967 的提交（同一构建参数）'),
                    (OLD, REF, '构建复现：本地按 CI 参数构建的 967 对官方包'),
                    (NEW, BASE, '最新代码对 ClickBench 现用的 v0.9.53')):
    fast = [q for q in OK if consistent(a, b, q) == 1]
    slow = [q for q in OK if consistent(a, b, q) == -1]
    p(f'### {title}\n')
    p(f'热跑几何平均 {geo(agg(a, q)["hot"] / agg(b, q)["hot"] for q in OK):.3f}，热跑总和 '
      f'{sum(agg(b, q)["hot"] for q in OK):.2f} → {sum(agg(a, q)["hot"] for q in OK):.2f} s，'
      f'峰值几何平均 {geo(agg(a, q)["peak"] / agg(b, q)["peak"] for q in OK):.3f}。'
      f'稳定变快 {len(fast)} 题，稳定变慢 {len(slow)} 题，其余 {len(OK) - len(fast) - len(slow)} 题在噪声内。\n')
    for label, qs in (('变快', fast), ('变慢', slow)):
        if qs:
            p(f'| {label} | {b} 热 | {a} 热 | 比值 | {b} 峰值 | {a} 峰值 |')
            p('|---|---:|---:|---:|---:|---:|')
            for q in sorted(qs, key=lambda q: agg(a, q)['hot'] / agg(b, q)['hot']):
                x, y = agg(b, q), agg(a, q)
                p(f"| Q{q:02d} | {x['hot']:.3f} | {y['hot']:.3f} | {y['hot'] / x['hot']:.2f} | {x['peak'] / 1024:.1f} GiB | {y['peak'] / 1024:.1f} GiB |")
            p('')
print('\n'.join(out))
