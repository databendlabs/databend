// Run with: node --test .github/scripts/test_pr_ci_lifecycle.js
const assert = require('node:assert/strict');
const { test } = require('node:test');
const { canRetryWorkflowRun, cancelClosedPRRuns, maxAutoRetries } = require('./pr_ci_lifecycle');
const retryFailedJobs = require('./retry_failed_jobs');

function makePR(overrides = {}) {
    return {
        number: 7,
        state: 'open',
        created_at: '2026-11-05T12:00:00Z',
        closed_at: null,
        head: { ref: 'feature', sha: 'current', repo: { id: 2 } },
        ...overrides
    };
}

function makeRun(overrides = {}) {
    return {
        id: 100,
        event: 'pull_request',
        status: 'queued',
        run_attempt: 1,
        created_at: '2026-11-05T13:00:00Z',
        head_branch: 'feature',
        head_sha: 'current',
        head_repository: { id: 2, owner: { login: 'fork-owner' } },
        pull_requests: [{ number: 7 }],
        ...overrides
    };
}

function harness({ pr = makePR(), runs = [], candidates = [pr], readPR, cancelError } = {}) {
    const calls = { cancelled: [], retried: [], reads: 0, lists: [], analyzed: 0, comments: [] };
    const core = { info() {}, warning() {}, error() {} };
    const context = {
        repo: { owner: 'databendlabs', repo: 'databend' },
        eventName: 'pull_request_target',
        runId: 999,
        payload: { action: 'closed', pull_request: { number: 7 } }
    };
    const github = { rest: { pulls: {}, actions: {}, checks: {}, repos: {} } };
    github.rest.pulls.get = async args => {
        calls.reads++;
        return { data: readPR ? readPR(calls.reads, args.pull_number) : pr };
    };
    github.rest.pulls.list = () => {};
    github.rest.actions.listWorkflowRunsForRepo = () => {};
    github.paginate = async (method, args) => {
        calls.lists.push(args);
        if (method === github.rest.pulls.list) return candidates;
        assert.equal(method, github.rest.actions.listWorkflowRunsForRepo);
        assert.ok(['pull_request', 'pull_request_target'].includes(args.event));
        assert.equal(args.branch, 'feature');
        return runs.filter(run => run.event === args.event);
    };
    github.rest.actions.cancelWorkflowRun = async args => {
        if (cancelError) throw cancelError;
        calls.cancelled.push(args.run_id);
    };
    github.rest.actions.getWorkflowRun = async () => ({ data: runs[0] });
    github.rest.actions.listJobsForWorkflowRun = async () => ({
        data: { jobs: [{ id: 42, name: 'linux / test', conclusion: 'failure' }] }
    });
    github.rest.actions.getJobForWorkflowRun = async () => {
        calls.analyzed++;
        return { data: { check_run_url: 'https://api.github.com/check-runs/42' } };
    };
    github.rest.checks.listAnnotations = async () => ({
        data: [{ annotation_level: 'failure', message: 'The operation was canceled.' }]
    });
    github.rest.actions.reRunWorkflowFailedJobs = async args => calls.retried.push(args.run_id);
    github.rest.repos.listPullRequestsAssociatedWithCommit = async () => ({ data: [] });
    github.rest.issues = {
        listComments: async () => ({ data: [] }),
        createComment: async args => calls.comments.push(args.body),
        updateComment: async args => calls.comments.push(args.body),
        deleteComment: async () => {}
    };
    return { github, context, core, calls };
}

const closedPR = () => makePR({ state: 'closed', closed_at: '2026-11-05T14:00:00Z' });

for (const event of ['pull_request', 'pull_request_target']) {
    test(`cancels every unfinished ${event} run, including stale heads`, async () => {
        const statuses = ['queued', 'in_progress', 'waiting', 'pending', 'requested'];
        const runs = statuses.map((status, i) => makeRun({ id: i + 1, event, status, head_sha: 'old' }));
        runs.push(makeRun({ id: 9, event, status: 'completed' }));
        const h = harness({ pr: closedPR(), runs });
        await cancelClosedPRRuns(h);
        assert.deepEqual(h.calls.cancelled, [1, 2, 3, 4, 5]);
    });

    test(`does not retry a closed PR (${event})`, async () => {
        const h = harness({ pr: closedPR() });
        assert.equal(await canRetryWorkflowRun(h.github, h.context, h.core, makeRun({ event })), false);
    });

    test(`can retry a current open PR (${event})`, async () => {
        const h = harness();
        assert.equal(await canRetryWorkflowRun(h.github, h.context, h.core, makeRun({ event })), true);
    });
}

test('never cancels Main, Release, completed runs or the cleanup run itself', async () => {
    const h = harness({ pr: closedPR(), runs: [
        makeRun({ id: 1, name: 'Main', event: 'push' }),
        makeRun({ id: 2, name: 'Release', event: 'workflow_dispatch' }),
        makeRun({ id: 3, status: 'completed' }),
        makeRun({ id: 999, event: 'pull_request_target' })
    ] });
    // Also exercise the event guard if a response unexpectedly contains a
    // non-PR run rather than relying only on the API's event query filter.
    h.github.paginate = async () => [
        makeRun({ id: 1, name: 'Main', event: 'push' }),
        makeRun({ id: 2, name: 'Release', event: 'workflow_dispatch' }),
        makeRun({ id: 3, status: 'completed' }),
        makeRun({ id: 999, event: 'pull_request_target' })
    ];
    await cancelClosedPRRuns(h);
    assert.deepEqual(h.calls.cancelled, []);
});

test('does not cancel a different PR', async () => {
    const h = harness({ pr: closedPR(), runs: [makeRun({ pull_requests: [{ number: 8 }] })],
        readPR: (_count, number) => number === 7 ? closedPR() : makePR({ number: 8 }) });
    await cancelClosedPRRuns(h);
    assert.deepEqual(h.calls.cancelled, []);
});

test('ignores a delayed close event after reopening', async () => {
    const h = harness({ runs: [makeRun()] });
    await cancelClosedPRRuns(h);
    assert.equal(h.calls.lists.length, 0);
    assert.deepEqual(h.calls.cancelled, []);
});

test('checks live state again before cancelling each run', async () => {
    const h = harness({ runs: [makeRun()], readPR: count => count === 1 ? closedPR() : makePR() });
    await cancelClosedPRRuns(h);
    assert.deepEqual(h.calls.cancelled, []);
});

test('ignores events other than PR closure', async () => {
    const h = harness({ pr: closedPR(), runs: [makeRun()] });
    h.context.payload.action = 'synchronize';
    await cancelClosedPRRuns(h);
    assert.equal(h.calls.reads, 0);
});

test('resolves fork runs with empty PR metadata, including a closed PR', async () => {
    const h = harness({ pr: closedPR(), runs: [makeRun({ pull_requests: [] })] });
    await cancelClosedPRRuns(h);
    assert.deepEqual(h.calls.cancelled, [100]);
    const query = h.calls.lists.find(args => args.head);
    assert.equal(query.head, 'fork-owner:feature');
    assert.equal(query.state, 'all');
});

test('does not confuse identically named branches in different forks', async () => {
    const other = makePR({ head: { ref: 'feature', sha: 'current', repo: { id: 3 } } });
    const h = harness({ candidates: [other] });
    assert.equal(await canRetryWorkflowRun(h.github, h.context, h.core, makeRun({ pull_requests: [] })), false);
});

test('disambiguates a branch reused by a later PR', async () => {
    const older = makePR({ number: 6, state: 'closed', closed_at: '2026-11-05T12:30:00Z' });
    const h = harness({ candidates: [older, makePR()] });
    assert.equal(await canRetryWorkflowRun(h.github, h.context, h.core, makeRun({ pull_requests: [] })), true);
});

test('skips ambiguous PR ownership instead of guessing', async () => {
    const h = harness({ pr: closedPR(), candidates: [closedPR(), makePR({ number: 8 })],
        runs: [makeRun({ pull_requests: [] })] });
    await cancelClosedPRRuns(h);
    assert.deepEqual(h.calls.cancelled, []);
    assert.equal(await canRetryWorkflowRun(h.github, h.context, h.core, makeRun({ pull_requests: [] })), false);
    assert.equal(await canRetryWorkflowRun(h.github, h.context, h.core,
        makeRun({ pull_requests: [{ number: 7 }, { number: 8 }] })), false);
});

test('skips missing fork identity instead of guessing', async () => {
    const h = harness();
    assert.equal(await canRetryWorkflowRun(h.github, h.context, h.core,
        makeRun({ pull_requests: [], head_repository: null })), false);
});

test('skips stale Dev commits but does not mistake a target base SHA for a stale head', async () => {
    const h = harness();
    assert.equal(await canRetryWorkflowRun(h.github, h.context, h.core, makeRun({ head_sha: 'old' })), false);
    assert.equal(await canRetryWorkflowRun(h.github, h.context, h.core,
        makeRun({ event: 'pull_request_target', head_sha: 'base-sha' })), true);
});

for (const [name, event] of [['Main', 'push'], ['Release', 'workflow_dispatch']]) {
    test(`${name} bypasses PR checks even with closed PR metadata`, async () => {
        const h = harness({ pr: closedPR() });
        assert.equal(await canRetryWorkflowRun(h.github, h.context, h.core,
            makeRun({ name, event, head_sha: 'old' })), true);
        assert.equal(h.calls.reads, 0);
        assert.equal(h.calls.lists.length, 0);
    });
}

test('ignores cancellation conflict only if the run has actually completed', async () => {
    const error = Object.assign(new Error('conflict'), { status: 409 });
    const h = harness({ pr: closedPR(), runs: [makeRun()], cancelError: error });
    h.github.rest.actions.getWorkflowRun = async () => ({ data: makeRun({ status: 'completed' }) });
    await cancelClosedPRRuns(h);
    h.github.rest.actions.getWorkflowRun = async () => ({ data: makeRun() });
    await assert.rejects(cancelClosedPRRuns(h), error);
});

test('does not hide cancellation permission errors', async () => {
    const error = Object.assign(new Error('forbidden'), { status: 403 });
    const h = harness({ pr: closedPR(), runs: [makeRun()], cancelError: error });
    await assert.rejects(cancelClosedPRRuns(h), error);
});

test('does not retry when PR lookup fails', async () => {
    const h = harness();
    h.github.rest.pulls.get = async () => { throw new Error('API unavailable'); };
    await assert.rejects(canRetryWorkflowRun(h.github, h.context, h.core, makeRun()), /API unavailable/);
    assert.deepEqual(h.calls.retried, []);
});

async function runRetry(h) {
    // The existing entry point uses these environment variables. Keep tests serial.
    const keys = ['WORKFLOW_RUN_ID', 'WORKFLOW_RUN_URL', 'CONCLUSION'];
    const before = keys.map(key => process.env[key]);
    process.env.WORKFLOW_RUN_ID = '100';
    process.env.WORKFLOW_RUN_URL = 'https://github.com/databendlabs/databend/actions/runs/100';
    process.env.CONCLUSION = 'failure';
    try {
        await retryFailedJobs(h);
    } finally {
        keys.forEach((key, i) => {
            if (before[i] === undefined) delete process.env[key];
            else process.env[key] = before[i];
        });
    }
}

test('retry entry point skips closed PRs before job analysis', async () => {
    const h = harness({ pr: closedPR(), runs: [makeRun()] });
    await runRetry(h);
    assert.equal(h.calls.analyzed, 0);
    assert.deepEqual(h.calls.retried, []);
});

test('retry entry point rechecks PR state after job analysis', async () => {
    const h = harness({ runs: [makeRun()], readPR: count => count === 1 ? makePR() : closedPR() });
    await runRetry(h);
    assert.equal(h.calls.analyzed, 1);
    assert.deepEqual(h.calls.retried, []);
});

test('retry entry point rechecks the PR head after job analysis', async () => {
    const newer = makePR({ head: { ref: 'feature', sha: 'newer', repo: { id: 2 } } });
    const h = harness({ runs: [makeRun()], readPR: count => count === 1 ? makePR() : newer });
    await runRetry(h);
    assert.equal(h.calls.analyzed, 1);
    assert.deepEqual(h.calls.retried, []);
});

test('retry entry point still retries infrastructure failures on an open PR', async () => {
    const h = harness({ runs: [makeRun()] });
    await runRetry(h);
    assert.deepEqual(h.calls.retried, ['100']);
    assert.equal(h.calls.reads, 2);
});

for (const [name, event] of [['Main', 'push'], ['Release', 'workflow_dispatch']]) {
    test(`retry entry point preserves ${name} retries`, async () => {
        const h = harness({ pr: closedPR(), runs: [makeRun({ name, event, pull_requests: [] })] });
        // Existing comment lookup may query PRs, independently of the retry guard.
        h.github.rest.pulls.list = async () => ({ data: [] });
        await runRetry(h);
        assert.deepEqual(h.calls.retried, ['100']);
        assert.equal(h.calls.reads, 0);
    });
}

test('Dev and Cloud allow one automatic retry; Main and Release keep three', () => {
    assert.equal(maxAutoRetries({ event: 'pull_request' }), 1);
    assert.equal(maxAutoRetries({ event: 'pull_request_target' }), 1);
    for (const event of ['push', 'workflow_dispatch', 'schedule']) {
        assert.equal(maxAutoRetries({ event }), 3);
    }
});

for (const event of ['pull_request', 'pull_request_target']) {
    test(`retry entry point stops ${event} runs after one retry`, async () => {
        const h = harness({ runs: [makeRun({ event, run_attempt: 2 })] });
        await runRetry(h);
        assert.deepEqual(h.calls.retried, []);
        assert.match(h.calls.comments.at(-1), /Maximum retry count \(1\) has been reached/);
    });
}

for (const [name, event] of [['Main', 'push'], ['Release', 'workflow_dispatch']]) {
    test(`retry entry point keeps three retries for ${name}`, async () => {
        for (const attempt of [2, 3]) {
            const h = harness({ runs: [makeRun({ name, event, run_attempt: attempt, pull_requests: [] })] });
            await runRetry(h);
            assert.deepEqual(h.calls.retried, ['100'], `attempt ${attempt} should retry`);
        }
        const h = harness({ runs: [makeRun({ name, event, run_attempt: 4, pull_requests: [] })] });
        await runRetry(h);
        assert.deepEqual(h.calls.retried, []);
        assert.match(h.calls.comments.at(-1), /Maximum retry count \(3\) has been reached/);
    });
}
