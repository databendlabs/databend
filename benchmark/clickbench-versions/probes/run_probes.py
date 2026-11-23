"""Follow-up probes on top of bench.py, run against the releases loaded in /tmp/cb-versions.

usage: run_probes.py format|q03|small
- format: v0.9.53 writes Native by default and 1.x no longer supports Native, so give v0.9.53 a
  Parquet copy of `hits` (ClickBench's DDL + STORAGE_FORMAT = 'parquet', filled by
  INSERT ... SELECT * FROM hits) and time queries hot on v0.9.53 Native, v0.9.53 Parquet and
  official-main-prs Parquet.
- q03: single-column aggregates on v0.9.53 (Native) and official-main-prs, plus EXPLAIN ANALYZE
  and EXPLAIN PIPELINE of Q03.
- small: official-main-prs only. Segment pruning stats of small queries over repeated hot runs,
  the same queries with the block meta caches enabled, and Q27 with `length` vs `octet_length`
  (also on v0.9.53).
Hot time = min of 5 runs (4 for `format`) after one warm-up run. Default configuration except
the one `small` case that enables block meta caches. Results go to probes/<name>.json."""
import json, re, sys, time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
exec((ROOT / 'bench.py').read_text().split("if __name__ == '__main__':")[0]
     .replace("Path(__file__).resolve().parent", f"Path('{ROOT}')"))
ALL = [q.strip() for q in (CB / 'queries.sql').read_text().strip().split('\n') if q.strip()]


def best(sql, n=5):
    http(sql)
    ts = []
    for _ in range(n):
        t = time.perf_counter(); http(sql); ts.append(time.perf_counter() - t)
    return round(min(ts), 4)


def format_probe():
    pick = [1, 2, 3, 7, 9, 10, 11, 20, 21, 22, 23, 24, 25, 26, 27, 30, 41]
    out = {'queries': pick, 'v0.9.53 native': {}, 'v0.9.53 parquet': {}, 'main-prs parquet': {}}
    start('v0.9.53')
    try:
        ddl = (CB / 'create.sql').read_text().strip().rstrip(';').replace('CREATE TABLE hits', 'CREATE TABLE hits_parquet')
        http('DROP TABLE IF EXISTS hits_parquet')
        out['ddl'] = ddl + " STORAGE_FORMAT = 'parquet'"
        http(out['ddl'])
        t0 = time.time(); http('INSERT INTO hits_parquet SELECT * FROM hits', timeout=7200)
        out['insert_s'] = round(time.time() - t0, 1)
        out['rows'] = http('SELECT count(*) FROM hits_parquet')[0][0]
        out['pipeline native'] = [x[0] for x in http('EXPLAIN PIPELINE SELECT AVG(UserID) FROM hits')]
        out['pipeline parquet'] = [x[0] for x in http('EXPLAIN PIPELINE SELECT AVG(UserID) FROM hits_parquet')]
        for q in pick:
            out['v0.9.53 native'][q] = best(ALL[q], 4)
            out['v0.9.53 parquet'][q] = best(ALL[q].replace('FROM hits', 'FROM hits_parquet'), 4)
    finally:
        stop()
    start('official-main-prs')
    try:
        try:
            http("CREATE TABLE native_probe (a INT) STORAGE_FORMAT = 'native'")
            out['native_on_1x'] = 'created'
            http('DROP TABLE native_probe')
        except RuntimeError as e:
            out['native_on_1x'] = str(e)
        for q in pick:
            out['main-prs parquet'][q] = best(ALL[q], 4)
    finally:
        stop()
    return out

def q03_probe():
    qs = {'Q03 AVG(UserID)': 'SELECT AVG(UserID) FROM hits', 'SUM(UserID)': 'SELECT SUM(UserID) FROM hits',
          'COUNT(UserID)': 'SELECT COUNT(UserID) FROM hits', 'MAX(CounterID)': 'SELECT MAX(CounterID) FROM hits',
          'AVG(ResolutionWidth)': 'SELECT AVG(ResolutionWidth) FROM hits'}
    out = {}
    for name in ('v0.9.53', 'official-main-prs'):
        start(name)
        try:
            out[name] = {'s': {k: best(q) for k, q in qs.items()},
                         'max_threads': http("SELECT value FROM system.settings WHERE name = 'max_threads'")[0][0],
                         'explain_analyze': [x[0] for x in http('EXPLAIN ANALYZE ' + qs['Q03 AVG(UserID)'])],
                         'explain_pipeline': [x[0] for x in http('EXPLAIN PIPELINE ' + qs['Q03 AVG(UserID)'])]}
        finally:
            stop()
    return out


def small_probe():
    global CONFIG
    small = [1, 2, 3, 7, 24, 26, 41]
    q27 = ALL[27]
    out = {'queries': small, 'segment_stats': {}, 'hot': {}}
    base = CONFIG
    for label, extra in (('default', ''),
                         ('block meta caches on', '\n[cache]\nblock_meta_count = 1000000\nsegment_block_metas_count = 10000\n')):
        CONFIG = base + extra
        start('official-main-prs')
        try:
            out['hot'][label] = {q: best(ALL[q]) for q in small}
            if label == 'default':
                for q in small:
                    out['segment_stats'][q] = [re.search(r'segments: <([^>]*)>', '\n'.join(
                        x[0] for x in http('EXPLAIN ANALYZE ' + ALL[q]))).group(1) for _ in range(3)]
                out['caches'] = http("SELECT name, num_items, size FROM system.caches WHERE name LIKE '%segment%' OR name LIKE '%block_meta%'")
                out['main-prs length'] = {'length/octet_length of абв': http("SELECT length('абв'), octet_length('абв')")[0],
                                          'Q27 length': best(q27), 'Q27 octet_length': best(q27.replace('length(URL)', 'octet_length(URL)')),
                                          'Q27 first row length': http(q27)[0],
                                          'Q27 first row octet_length': http(q27.replace('length(URL)', 'octet_length(URL)'))[0]}
        finally:
            stop()
    CONFIG = base
    start('v0.9.53')
    try:
        out['v0.9.53 length'] = {'length/octet_length of абв': http("SELECT length('абв'), octet_length('абв')")[0],
                                 'Q27 length': best(q27), 'Q27 first row length': http(q27)[0]}
    finally:
        stop()
    return out


if __name__ == '__main__':
    what = sys.argv[1]
    res = {'format': format_probe, 'q03': q03_probe, 'small': small_probe}[what]()
    res['created'] = time.strftime('%FT%TZ', time.gmtime())
    (ROOT / 'probes' / f'{what}.json').write_text(json.dumps(res, indent=1, ensure_ascii=False))
    print(json.dumps({k: v for k, v in res.items() if 'pipeline' not in k and 'explain' not in str(v)[:20]}, ensure_ascii=False)[:3000])
