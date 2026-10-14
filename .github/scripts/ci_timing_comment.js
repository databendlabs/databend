// Post / update a "CI timing" comment on the PR of a finished Dev or Cloud
// workflow run. One comment per workflow, keyed by TITLE, so each push
// updates in place.
//
// Dev   : total, linux/build breakdown (parsed from the cargo log), test_unit,
//         mac_check, downstream test groups. Baseline = median of recent
//         successful `Main` runs (same reuse.linux.yml).
// Cloud : release builds per arch with breakdown, docker, benchmark.
//         Baseline = median of recent successful `Cloud` runs (any branch,
//         since Cloud only runs on PRs).

const BASELINE_RUNS = 10;
const FLAG_MIN = 1.0; // |delta| >= this many minutes: bold
const WARN_MIN = 3.0; // delta >= this many minutes: warning sign

const WORKFLOWS = {
    Dev: { title: '## ⏱ CI timing · Dev', baseline: 'main.yml' },
    Cloud: { title: '## ⏱ CI timing · Cloud (PR image)', baseline: 'cloud.yml' },
};

// Downstream Dev test groups, matched against job names from reuse.linux.yml.
const DEV_GROUPS = [
    ['sqllogic standalone', /^linux \/ sqllogic \/ standalone \(/],
    ['sqllogic cluster', /^linux \/ sqllogic \/ cluster \(/],
    ['sqllogic stage', /^linux \/ sqllogic \/ stage \(/],
    ['sqllogic other', /^linux \/ sqllogic \/ (standalone_|cluster_with|ee|management_mode)/],
    ['stateless', /^linux \/ test_stateless_/],
    ['stateful', /^linux \/ test_stateful_/],
    ['compat', /^linux \/ test_compat_/],
    ['meta / logs / lineage / udf', /^linux \/ test_(meta|metactl|logs|lineage|sandbox_udf)/],
    ['ee', /^linux \/ test_ee_/],
    ['private_tasks', /^linux \/ test_private_tasks/],
    ['fuzz', /^linux \/ test_fuzz_/],
];

const min = (a, b) => (new Date(b) - new Date(a)) / 60000;
const median = (xs) => {
    const v = xs.filter((x) => Number.isFinite(x)).sort((a, b) => a - b);
    if (!v.length) return NaN;
    const m = Math.floor(v.length / 2);
    return v.length % 2 ? v[m] : (v[m - 1] + v[m]) / 2;
};
const fmt = (x, unit = 'min') => (Number.isFinite(x) ? `${x.toFixed(1)} ${unit}` : '—');
const fmtDelta = (x, unit = 'min') => {
    if (!Number.isFinite(x)) return '—';
    const s = `${x >= 0 ? '+' : '−'}${Math.abs(x).toFixed(1)} ${unit}`;
    if (x >= WARN_MIN) return `**${s}** ⚠️`;
    if (Math.abs(x) >= FLAG_MIN) return `**${s}**`;
    return s;
};

async function listJobs(github, context, runId) {
    return github.paginate(github.rest.actions.listJobsForWorkflowRun, {
        owner: context.repo.owner,
        repo: context.repo.repo,
        run_id: runId,
        per_page: 100,
    });
}

// Parse a build job log into phases using cargo's timestamped "Compiling" lines.
//   crates : first Compiling -> last Compiling before enterprise-query/binaries
//            (parallel phase; databend-query starts inside it)
//   query  : from there -> Compiling databend-enterprise-query (or binaries);
//            only databend-query codegen is running by then
//   link   : Compiling databend-binaries -> Finished
async function buildBreakdown(github, context, core, jobId) {
    let log;
    try {
        const res = await github.rest.actions.downloadJobLogsForWorkflowRun({
            owner: context.repo.owner,
            repo: context.repo.repo,
            job_id: jobId,
        });
        log = typeof res.data === 'string' ? res.data : Buffer.from(res.data).toString('utf8');
    } catch (e) {
        core.warning(`log for job ${jobId}: ${e.message}`);
        return null;
    }
    const ts = (line) => (line.match(/^(\S+)/) || [])[1];
    const lines = log.split('\n');
    let first = null, lastOther = null, ee = null, bins = null, finished = null;
    let hits = null, misses = null, requests = null;
    const strip = (s) => s.replace(/\x1b\[[0-9;]*m/g, '');
    for (const raw of lines) {
        const line = strip(raw);
        const m = line.match(/^(\S+) +Compiling (\S+) /);
        if (m) {
            const t = m[1], crate = m[2];
            if (!first) first = t;
            if (crate === 'databend-enterprise-query') ee = ee || t;
            else if (crate === 'databend-binaries') bins = bins || t;
            else if (!ee && !bins) lastOther = t; // last crate to start before the single-core tail
            continue;
        }
        // build_linux may invoke cargo several times (one per --bin); take the
        // last Finished and sum sccache stats across invocations.
        if (/ Finished `?\w+`? profile/.test(line)) finished = ts(line);
        let s;
        if ((s = line.match(/Compile requests executed\s+(\d+)/))) requests = (requests || 0) + +s[1];
        else if ((s = line.match(/Cache hits\s+(\d+)\s*$/))) hits = (hits || 0) + +s[1];
        else if ((s = line.match(/Cache misses\s+(\d+)\s*$/))) misses = (misses || 0) + +s[1];
    }
    if (!first || !finished) return null;
    const queryEnd = ee || bins || finished;
    return {
        crates: lastOther ? min(first, lastOther) : NaN,
        query: lastOther && queryEnd ? min(lastOther, queryEnd) : NaN,
        link: bins ? min(bins, finished) : NaN,
        hit: requests ? (100 * hits) / requests : NaN,
    };
}

function runWall(jobs) {
    const js = jobs.filter((j) => j.started_at && j.completed_at && j.conclusion !== 'skipped');
    if (!js.length) return NaN;
    const a = js.map((j) => new Date(j.started_at)).reduce((x, y) => (x < y ? x : y));
    const b = js.map((j) => new Date(j.completed_at)).reduce((x, y) => (x > y ? x : y));
    return (b - a) / 60000;
}
function jobDur(j) {
    return j && j.started_at && j.completed_at && j.conclusion !== 'skipped' ? min(j.started_at, j.completed_at) : NaN;
}
function jobQueue(j) {
    return j && j.created_at && j.started_at ? min(j.created_at, j.started_at) : NaN;
}

// ---------------------------------------------------------------- Dev

async function devMetrics(github, context, core, run, jobs, withBreakdown) {
    const by = (name) => jobs.find((j) => j.name === name);
    const build = by('linux / build');
    const m = {
        total: runWall(jobs),
        build: jobDur(build),
        test_unit: jobDur(by('linux / test_unit')),
        mac_check: jobDur(by('mac_check')),
        breakdown: null,
        groups: {},
        downstream: NaN,
        queue_p50: NaN,
        queue_max: NaN,
        jobs: 0,
    };
    if (build && build.completed_at) {
        const ds = jobs.filter(
            (j) => j.name.startsWith('linux / ') && !['linux / build', 'linux / test_unit', 'linux / check', 'linux / info'].includes(j.name) && j.conclusion !== 'skipped' && j.started_at
        );
        m.jobs = ds.length;
        if (ds.length) {
            const end = ds.map((j) => new Date(j.completed_at)).reduce((a, b) => (a > b ? a : b));
            m.downstream = (end - new Date(build.completed_at)) / 60000;
            const q = ds.map((j) => min(build.completed_at, j.started_at)).filter((x) => x >= 0);
            m.queue_p50 = median(q);
            m.queue_max = Math.max(...q);
        }
        for (const [name, re] of DEV_GROUPS) {
            const d = ds.filter((j) => re.test(j.name)).map(jobDur).filter(Number.isFinite);
            if (d.length) m.groups[name] = { max: Math.max(...d), n: d.length };
        }
        if (withBreakdown) m.breakdown = await buildBreakdown(github, context, core, build.id);
    }
    return m;
}

function devTable(cur, base, run) {
    const row = (label, a, b, unit = 'min') => `| ${label} | ${fmt(a, unit)} | ${fmt(b, unit)} | ${fmtDelta(Number.isFinite(a) && Number.isFinite(b) ? a - b : NaN, unit)} |`;
    const bd = cur.breakdown || {};
    const bb = base.breakdown || {};
    const lines = [
        `### [run ${run.id}](${run.html_url})${run.run_attempt > 1 ? ` · attempt ${run.run_attempt} (wall time includes re-run gap)` : ''} · **${fmt(cur.total)}** (main p50 ${fmt(base.total)}, ${fmtDelta(cur.total - base.total)})`,
        '',
        '| stage | this run | main p50 | Δ |',
        '|---|--:|--:|--:|',
        row('**total**', cur.total, base.total),
        row('**build (aarch64, ci profile)**', cur.build, base.build),
    ];
    if (cur.breakdown) {
        lines.push(
            row('├ deps + crates (parallel)', bd.crates, bb.crates),
            row('├ databend-query codegen', bd.query, bb.query),
            row('├ databend-binaries link', bd.link, bb.link),
            `| └ sccache hit rate | ${fmt(bd.hit, '%')} | ${fmt(bb.hit, '%')} | ${fmtDelta(bd.hit - bb.hit, 'pp')} |`
        );
    }
    lines.push(row('**test_unit**', cur.test_unit, base.test_unit), row('mac_check', cur.mac_check, base.mac_check));
    lines.push(row(`**tests after build (${cur.jobs} jobs)**`, cur.downstream, base.downstream));
    lines.push(
        `| ├ queue p50 / max | ${fmt(cur.queue_p50)} / ${fmt(cur.queue_max)} | ${fmt(base.queue_p50)} / ${fmt(base.queue_max)} | ${fmtDelta(cur.queue_p50 - base.queue_p50)} |`
    );
    const names = Object.keys(cur.groups);
    names.forEach((g, i) => {
        const pre = i === names.length - 1 ? '└' : '├';
        const c = cur.groups[g], b = base.groups[g] || {};
        lines.push(row(`${pre} ${g} (${c.n} jobs, max)`, c.max, b.max));
    });
    return lines.join('\n');
}

// ---------------------------------------------------------------- Cloud

async function cloudMetrics(github, context, core, run, jobs, withBreakdown) {
    const m = { total: runWall(jobs), builds: {}, docker: NaN, benchmark: NaN, toImage: NaN };
    for (const j of jobs) {
        const b = j.name.match(/^build \((x86_64|aarch64)/);
        if (b) {
            m.builds[b[1]] = { dur: jobDur(j), breakdown: withBreakdown ? await buildBreakdown(github, context, core, j.id) : null };
        } else if (j.name === 'docker') {
            m.docker = jobDur(j);
            if (j.completed_at) m.toImage = min(jobs.filter((x) => x.started_at).map((x) => x.started_at).sort()[0], j.completed_at);
        } else if (j.name === 'benchmark' || j.name.startsWith('benchmark /')) {
            m.benchmark = Number.isFinite(m.benchmark) ? Math.max(m.benchmark, jobDur(j)) : jobDur(j);
        }
    }
    return m;
}

function cloudTable(cur, base, run) {
    const row = (label, a, b, unit = 'min') => `| ${label} | ${fmt(a, unit)} | ${fmt(b, unit)} | ${fmtDelta(Number.isFinite(a) && Number.isFinite(b) ? a - b : NaN, unit)} |`;
    const lines = [
        `### [run ${run.id}](${run.html_url}) · **to image ${fmt(cur.toImage)}** (recent p50 ${fmt(base.toImage)}, ${fmtDelta(cur.toImage - base.toImage)})`,
        '',
        '| stage | this run | recent p50 | Δ |',
        '|---|--:|--:|--:|',
    ];
    for (const arch of Object.keys(cur.builds).sort()) {
        const c = cur.builds[arch], b = base.builds[arch] || {};
        lines.push(row(`**build ${arch} (release)**`, c.dur, b.dur));
        if (c.breakdown) {
            const bd = c.breakdown, bb = b.breakdown || {};
            lines.push(
                row('├ deps + crates (parallel)', bd.crates, bb.crates),
                row('├ databend-query codegen', bd.query, bb.query),
                row('├ databend-binaries link', bd.link, bb.link),
                `| └ sccache hit rate | ${fmt(bd.hit, '%')} | ${fmt(bb.hit, '%')} | ${fmtDelta(bd.hit - bb.hit, 'pp')} |`
            );
        }
    }
    lines.push(row('docker (multi-arch)', cur.docker, base.docker));
    if (Number.isFinite(cur.benchmark)) lines.push(`| benchmark | ${fmt(cur.benchmark)} | — | only with \`ci-benchmark*\` |`);
    return lines.join('\n');
}

// ---------------------------------------------------------------- baseline

async function baseline(github, context, core, workflowFile, metricsFn, excludeRunId) {
    let runs = [];
    try {
        const params = {
            owner: context.repo.owner,
            repo: context.repo.repo,
            workflow_id: workflowFile,
            status: 'success',
            per_page: BASELINE_RUNS + 1,
        };
        if (workflowFile === 'main.yml') params.branch = 'main';
        const { data } = await github.rest.actions.listWorkflowRuns(params);
        runs = data.workflow_runs.filter((r) => r.id !== excludeRunId).slice(0, BASELINE_RUNS);
        runs = data.workflow_runs;
    } catch (e) {
        core.warning(`baseline ${workflowFile}: ${e.message}`);
    }
    const all = [];
    for (const r of runs) {
        const jobs = await listJobs(github, context, r.id);
        all.push(await metricsFn(github, context, core, r, jobs, false));
    }
    // Median of each scalar; groups / builds medianed per key. Breakdown is not
    // computed for baseline (would need 10 log downloads); shown as — .
    const agg = (objs) => {
        const out = {};
        for (const k of new Set(objs.flatMap(Object.keys))) {
            const vals = objs.map((o) => o[k]).filter((v) => v !== undefined);
            if (typeof vals[0] === 'number') out[k] = median(vals);
            else if (vals[0] && typeof vals[0] === 'object') out[k] = agg(vals);
        }
        return out;
    };
    return agg(all);
}

// ---------------------------------------------------------------- comment

async function findPR(github, context, core, run) {
    if (run.pull_requests && run.pull_requests.length) return run.pull_requests[0].number;
    // For fork PRs, workflow_run.pull_requests is [] and commits/{sha}/pulls
    // does not index fork commits, so match open PRs by <head_owner>:<head_branch>.
    const headOwner = run.head_repository && run.head_repository.owner && run.head_repository.owner.login;
    if (headOwner && run.head_branch) {
        try {
            const { data } = await github.rest.pulls.list({
                owner: context.repo.owner,
                repo: context.repo.repo,
                state: 'open',
                head: `${headOwner}:${run.head_branch}`,
            });
            const match = data.find((p) => p.head && p.head.sha === run.head_sha);
            if (match) return match.number;
        } catch (e) {
            core.warning(`PR lookup by head: ${e.message}`);
        }
    }
    try {
        const { data } = await github.rest.repos.listPullRequestsAssociatedWithCommit({
            owner: context.repo.owner,
            repo: context.repo.repo,
            commit_sha: run.head_sha,
        });
        const open = data.filter((p) => p.state === 'open');
        if (open.length) return open[0].number;
    } catch (e) {
        core.warning(`PR lookup: ${e.message}`);
    }
    return null;
}

async function upsertComment(github, context, core, prNumber, title, body) {
    const comments = await github.paginate(github.rest.issues.listComments, {
        owner: context.repo.owner,
        repo: context.repo.repo,
        issue_number: prNumber,
        per_page: 100,
    });
    const existing = comments.find((c) => c.user.type === 'Bot' && c.body.startsWith(title));
    const full = `${title}\n\n${body}\n\n<sub>Minutes unless stated; durations exclude queue time unless stated; "max" = longest job in the group. Baseline = median of the last ${BASELINE_RUNS} successful runs (Dev: \`Main\` on main; Cloud: recent \`Cloud\` runs). Build breakdown has no baseline. Updated on each push.</sub>`;
    if (existing) {
        await github.rest.issues.updateComment({ owner: context.repo.owner, repo: context.repo.repo, comment_id: existing.id, body: full });
        core.info(`updated comment ${existing.id}`);
    } else {
        await github.rest.issues.createComment({ owner: context.repo.owner, repo: context.repo.repo, issue_number: prNumber, body: full });
        core.info('created comment');
    }
}

module.exports = async ({ github, context, core }) => {
    const run = context.payload.workflow_run;
    const cfg = WORKFLOWS[run.name];
    if (!cfg) {
        core.info(`workflow ${run.name} not tracked`);
        return;
    }
    if (run.event !== 'pull_request' && run.event !== 'pull_request_target') {
        core.info(`event ${run.event}, not a PR run`);
        return;
    }
    const prNumber = await findPR(github, context, core, run);
    if (!prNumber) {
        core.info('no open PR for this run');
        return;
    }
    const jobs = await listJobs(github, context, run.id);

    let body;
    if (run.name === 'Dev') {
        const changes = jobs.find((j) => j.name === 'changes');
        const held = (changes?.steps || []).some((s) => s.name === 'Heavy CI Paused' && s.conclusion === 'success');
        if (held) {
            core.info('Dev run held by hold-ci, leaving timing comment unchanged');
            return;
        }
        const cur = await devMetrics(github, context, core, run, jobs, true);
        if (!Number.isFinite(cur.build)) {
            body = `### [run ${run.id}](${run.html_url}) · ${fmt(cur.total)}\n\nbuild skipped (no source changes).`;
        } else {
            const base = await baseline(github, context, core, cfg.baseline, devMetrics, run.id);
            body = devTable(cur, base, run);
        }
    } else {
        const cur = await cloudMetrics(github, context, core, run, jobs, true);
        if (!Object.keys(cur.builds).length) {
            core.info('no build jobs in Cloud run');
            return;
        }
        const base = await baseline(github, context, core, cfg.baseline, cloudMetrics, run.id);
        body = cloudTable(cur, base, run);
    }
    await upsertComment(github, context, core, prNumber, cfg.title, body);
};
