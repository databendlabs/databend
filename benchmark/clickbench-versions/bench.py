"""ClickBench-style comparison of Databend releases under the default configuration.

usage:
  bench.py manifest              record machine, harness, release binaries -> manifest.json
  bench.py load NAME             create ClickBench's `hits` for release NAME and load it
  bench.py run ROUNDS            run the 43 queries, interleaved across releases
Releases are listed in releases.json; each lives in /tmp/cb-versions/NAME (bin/, data, meta).

Procedure for one (round, query, release) cell, the same as ClickBench's benchmark.sh except
for the page cache step: wait until the host is idle, stop meta and query, evict the files of
every release from the page cache, start meta then query (both pinned to CPUs 0-15), wait for
`SELECT 1`, then run the query 3 times. Run 1 is cold; hot = min(run 2, run 3).
Within a round, queries go in order Q00..Q42; for each query every release runs once, in
releases.json order on even rounds and reversed on odd rounds, so drift hits all releases."""
import base64, hashlib, json, os, platform, signal, subprocess, sys, threading, time, urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent
RELEASES = json.loads((ROOT / 'releases.json').read_text())
DATA = Path('/tmp/cb-versions')
CB = ROOT / 'clickbench'
PARQUET = '/tmp/tpch-local/hits/hits.parquet'
CPUS = '0-15'
QUIET_LIMIT = 10.0  # percent of all host CPUs busy, sampled over 1 s
PAGE = os.sysconf('SC_PAGE_SIZE') / 2**20
AUTH = 'Basic ' + base64.b64encode(b'root:').decode()
# ClickBench's databend/config.toml, plus the ids and the root user that 1.x requires.
CONFIG = """[query]
tenant_id = "default"
cluster_id = "clickbench"
{users}
[storage]
type = "fs"
allow_insecure = true

[storage.fs]
data_path = "{vd}/_data"

[meta]
endpoints = ["127.0.0.1:9191"]
username = "root"
password = "root"
client_timeout_in_second = 60
auto_sync_interval = 60

[log.file]
dir = "{vd}/logs"
"""
USERS_1X = '\n[[query.users]]\nname = "root"\nauth_type = "no_password"\n'
procs = {}
sh = lambda c: subprocess.run(c, shell=True, capture_output=True, text=True).stdout.strip()

def http(sql, timeout=3600):
    body = json.dumps({'sql': sql, 'pagination': {'wait_time_secs': 30, 'max_rows_per_page': 100000}}).encode()
    req = urllib.request.Request('http://127.0.0.1:8000/v1/query', body,
                                 {'Content-Type': 'application/json', 'Authorization': AUTH})
    r = json.load(urllib.request.urlopen(req, timeout=timeout))
    rows = []
    while True:
        if r.get('error'):
            raise RuntimeError(json.dumps(r['error'])[:500])
        rows.extend(r.get('data') or [])
        nxt = r.get('next_uri')
        if not nxt:
            return rows
        req = urllib.request.Request('http://127.0.0.1:8000' + nxt, headers={'Authorization': AUTH})
        r = json.load(urllib.request.urlopen(req, timeout=timeout))


def vdir(name):
    return DATA / name


def start(name):
    vd = vdir(name)
    users = '' if RELEASES[name].get('builtin_root') else USERS_1X
    (vd / 'config.toml').write_text(CONFIG.format(users=users, vd=vd))
    os.makedirs(vd / 'logs', exist_ok=True)
    meta = subprocess.Popen(['taskset', '-c', CPUS, str(vd / 'bin/databend-meta'), '--single'], cwd=vd,
                            stdout=open(vd / 'meta.log', 'a'), stderr=subprocess.STDOUT, start_new_session=True)
    for _ in range(60):
        if subprocess.run(['bash', '-c', 'echo > /dev/tcp/127.0.0.1/9191'], capture_output=True).returncode == 0:
            break
        time.sleep(0.2)
    # The shell execs into databend-query, so the Popen pid is the query process.
    query = subprocess.Popen(['bash', '-c', f'ulimit -n 65536; exec taskset -c {CPUS} {vd}/bin/databend-query -c {vd}/config.toml'],
                             cwd=vd, stdout=open(vd / 'query.log', 'a'), stderr=subprocess.STDOUT, start_new_session=True)
    procs.update(meta=meta, query=query)
    t0 = time.time()
    while True:
        try:
            http('SELECT 1', timeout=10)
            return time.time() - t0
        except Exception:
            if query.poll() is not None or time.time() - t0 > 600:
                raise RuntimeError(f'{name}: databend-query did not come up, see query.log')
            time.sleep(0.05)


def stop():
    for name in ('query', 'meta'):  # query first: it unregisters from meta on shutdown
        p = procs.pop(name, None)
        if p and p.poll() is None:
            p.send_signal(signal.SIGTERM)
            try:
                p.wait(30)
            except subprocess.TimeoutExpired:
                p.kill(); p.wait()


def evict_all():
    # /proc/sys is read-only in this container (no drop_caches): evict every release's files
    # (binaries, meta, table data) with posix_fadvise(DONTNEED) instead.
    os.sync()
    for name in RELEASES:
        for f in vdir(name).rglob('*'):
            if f.is_file() and not f.is_symlink():
                try:
                    fd = os.open(f, os.O_RDONLY)
                except OSError:
                    continue
                try:
                    os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
                finally:
                    os.close(fd)


def host_busy(seconds=1.0):
    def sample():
        v = list(map(int, open('/proc/stat').readline().split()[1:9]))
        return sum(v), v[3] + v[4]
    t0, i0 = sample(); time.sleep(seconds); t1, i1 = sample()
    return 100 * (1 - (i1 - i0) / max(t1 - t0, 1))


def wait_quiet(limit=QUIET_LIMIT, timeout=300):
    t0 = time.time()
    while True:
        b = host_busy()
        if b < limit or time.time() - t0 > timeout:
            return round(b, 2)


class Peak:
    """Peak RSS of a pid, sampled every 5 ms from /proc/PID/statm."""
    def __init__(self, pid):
        self.f = Path(f'/proc/{pid}/statm'); self.v = 0; self.ev = threading.Event()
        self.t = threading.Thread(target=self.loop, daemon=True); self.t.start()

    def loop(self):
        while not self.ev.wait(0.005):
            try:
                self.v = max(self.v, int(self.f.read_text().split()[1]) * PAGE)
            except (FileNotFoundError, ProcessLookupError):
                return

    def close(self):
        self.ev.set(); self.t.join(); return round(self.v)

def file_sha(p):
    h = hashlib.sha256()
    with open(p, 'rb') as f:
        for b in iter(lambda: f.read(1 << 24), b''):
            h.update(b)
    return h.hexdigest()


def drop_caches_writable():
    try:
        open('/proc/sys/vm/drop_caches', 'w').close()
        return True
    except OSError as e:
        return f'no: {e}'


def manifest():
    m = {'created': time.strftime('%FT%TZ', time.gmtime()),
         'machine': {'cpu': sh("lscpu | grep 'Model name'"), 'lscpu': sh('lscpu'), 'kernel': platform.release(),
                     'mem': sh('free -b | head -2'), 'governor': sh('cat /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor'),
                     'thp': sh('cat /sys/kernel/mm/transparent_hugepage/enabled /sys/kernel/mm/transparent_hugepage/defrag'),
                     'cpu_pinning': CPUS, 'cpus_pinned_topology': sh(f"lscpu -e=CPU,CORE,MAXMHZ | awk 'NR==1 || ($1>=0 && $1<=15)'"),
                     'cgroup_cpu_max': sh('cat /sys/fs/cgroup/cpu.max'), 'cgroup_mem_max': sh('cat /sys/fs/cgroup/memory.max'),
                     'data_fs': sh(f'df -hT {DATA}'), 'block_device': sh('lsblk -d -o NAME,MODEL,SIZE,ROTA'),
                     'drop_caches_writable': drop_caches_writable()},
         'harness': {f: file_sha(ROOT / f) for f in ('bench.py', 'report.py', 'releases.json', 'build-official.sh',
                                                       'clickbench/create.sql', 'clickbench/queries.sql')},
         'source_data': {'path': PARQUET, 'bytes': os.path.getsize(PARQUET), 'sha256': file_sha(PARQUET)},
         'config_template': CONFIG, 'config_users_1x': USERS_1X,
         'query_client': 'HTTP POST /v1/query, wait_time_secs=30, max_rows_per_page=100000, client-side wall time',
         'releases': {}}
    for name, r in RELEASES.items():
        vd = vdir(name)
        e = dict(r)
        for b in ('databend-query', 'databend-meta'):
            e[b] = {'sha256': file_sha(vd / 'bin' / b), 'version': sh(f'{vd}/bin/{b} --version 2>&1 | head -1')}
        bi = ROOT / 'bin' / name / 'build-info.json'
        if bi.exists():
            e['build_info'] = json.loads(bi.read_text())
        m['releases'][name] = e
    (ROOT / 'manifest.json').write_text(json.dumps(m, indent=1))
    print('manifest.json written')

def load(name):
    """Two steps for every release: v0.9.53 cannot COPY hits.parquet's integer seconds / days into
    TIMESTAMP / DATE, so COPY into `hits_raw` with the Parquet types, then INSERT ... SELECT into
    ClickBench's `hits` converting those four columns, and drop `hits_raw` with its files."""
    import re
    vd = vdir(name); out = vd / 'load.json'
    if out.exists():
        print(name, 'already loaded'); return
    res, raw_id = {}, None
    start(name)
    try:
        ddl = (CB / 'create.sql').read_text().strip().rstrip(';')
        conv = {'EventTime': ('BIGINT', 'to_timestamp'), 'ClientEventTime': ('BIGINT', 'to_timestamp'),
                'LocalEventTime': ('BIGINT', 'to_timestamp'), 'EventDate': ('UINT16', 'to_date')}
        cols = re.findall(r'^\s+(\w+)\s+\w+', ddl.split('(', 1)[1], re.M)
        raw = ddl.replace('CREATE TABLE hits', 'CREATE TABLE hits_raw').split(')\nCLUSTER BY')[0] + ')'
        for c, (ty, _) in conv.items():
            raw = re.sub(rf'(\n\s+{c}\s+)\w+', rf'\g<1>{ty}', raw)
        select = ', '.join(f'{conv[c][1]}({c})' if c in conv else c for c in cols)
        for t in ('hits', 'hits_raw'):
            http(f'DROP TABLE IF EXISTS {t}')
        http(ddl); http(raw)
        res['sql'] = {'hits': ddl, 'hits_raw': raw,
                      'copy': f"COPY INTO hits_raw FROM 'fs://{PARQUET}' FILE_FORMAT = (TYPE = 'PARQUET')",
                      'insert': f'INSERT INTO hits SELECT {select} FROM hits_raw'}
        t0 = time.time(); http(res['sql']['copy'], timeout=7200)
        t1 = time.time(); http(res['sql']['insert'], timeout=7200)
        t2 = time.time()
        res.update(copy_s=round(t1 - t0, 1), insert_s=round(t2 - t1, 1), load_s=round(t2 - t0, 1))
        raw_id = http("SELECT snapshot_location FROM fuse_snapshot('default', 'hits_raw') LIMIT 1")[0][0].split('/')[1]
        hits_id = http("SELECT snapshot_location FROM fuse_snapshot('default', 'hits') LIMIT 1")[0][0].split('/')[1]
        assert raw_id != hits_id
        http('DROP TABLE hits_raw')
        res['rows'] = http('SELECT count(*) FROM hits')[0][0]
        try:
            res['blocks'] = http("SELECT block_count FROM fuse_snapshot('default', 'hits') LIMIT 1")[0][0]
        except Exception as e:
            res['blocks'] = f'unavailable: {e}'[:200]
        res['version'] = http('SELECT version()')[0][0]
        res['show_create'] = http('SHOW CREATE TABLE hits')[0][1]
        # Effective configuration and any setting that differs from its default.
        res['configs'] = http('SELECT * FROM system.configs')
        try:
            res['non_default_settings'] = http("SELECT name, value, default FROM system.settings WHERE value <> default")
        except Exception as e:
            res['non_default_settings'] = f'unavailable: {e}'[:200]
    finally:
        stop()
    for d in (vd / '_data').glob(f'*/{raw_id}'):
        subprocess.run(['rm', '-r', str(d)], check=True)
    os.sync()
    res['data_bytes'] = int(sh(f'du -bs {vd}/_data').split()[0])
    out.write_text(json.dumps(res, indent=1))
    print(name, {k: res[k] for k in ('load_s', 'rows', 'blocks', 'data_bytes', 'version')})


def run(rounds):
    qs = [q.strip() for q in (CB / 'queries.sql').read_text().strip().split('\n') if q.strip()]
    assert len(qs) == 43
    names = list(RELEASES)
    for r in range(rounds):
        out = ROOT / 'results' / f'round{r}.json'
        out.parent.mkdir(exist_ok=True)
        res = json.loads(out.read_text()) if out.exists() else {}
        order = names if r % 2 == 0 else names[::-1]
        for qi, q in enumerate(qs):
            for name in order:
                key = f'{name}/{qi}'
                if key in res:
                    continue
                busy = wait_quiet()
                stop(); evict_all()
                startup = start(name)
                cell = {'ts': time.strftime('%FT%TZ', time.gmtime()), 'busy_before': busy,
                        'startup_s': round(startup, 3), 'runs': []}
                try:
                    for i in range(3):
                        peak = Peak(procs['query'].pid); t0 = time.perf_counter()
                        try:
                            rows = http(q)
                            run_ = {'s': round(time.perf_counter() - t0, 4), 'peak_mib': peak.close(), 'rows': len(rows),
                                    'hash': hashlib.sha256(json.dumps(sorted(map(json.dumps, rows))).encode()).hexdigest()[:16]}
                            if i == 0:
                                cell['result'] = rows[:20]
                        except Exception as e:
                            peak.close(); run_ = {'error': str(e)[:300]}
                        cell['runs'].append(run_)
                finally:
                    stop()
                res[key] = cell
                out.write_text(json.dumps(res, indent=1))
                print(f'r{r} Q{qi:02d} {name:18s}', [x.get('s', 'ERR') for x in cell['runs']], flush=True)


if __name__ == '__main__':
    cmd = sys.argv[1]
    try:
        if cmd == 'manifest':
            manifest()
        elif cmd == 'load':
            load(sys.argv[2])
        elif cmd == 'run':
            run(int(sys.argv[2]))
        else:
            sys.exit(__doc__)
    finally:
        stop()
