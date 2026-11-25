// Run with: node --test .github/scripts/test_pr_ci_lifecycle.js
const assert = require('node:assert/strict');
const { test } = require('node:test');
const {
    applyDefaultPRCIHold, canRetryWorkflowRun, cancelClosedPRRuns, getPRCIHoldReason, maxAutoRetries
} = require('./pr_ci_lifecycle');

const core = { info() {}, warning() {} };
const repo = { owner: 'databendlabs', repo: 'databend' };

function pr(overrides = {}) {
    return {
        number: 7,
        state: 'open',
        created_at: '2026-11-05T12:00:00Z',
        closed_at: null,
        labels: [],
        user: { login: 'contributor' },
        head: { ref: 'feature', sha: 'head', repo: { id: 2 } },
        ...overrides
    };
}

const closedPR = (overrides = {}) => pr({ state: 'closed', closed_at: '2026-11-05T14:00:00Z', ...overrides });

function run(overrides = {}) {
    return {
        id: 100,
        event: 'pull_request',
        status: 'queued',
        created_at: '2026-11-05T13:00:00Z',
        head_branch: 'feature',
        head_sha: 'head',
        head_repository: { id: 2, owner: { login: 'fork' } },
        pull_requests: [{ number: 7 }],
        ...overrides
    };
}

function mockGitHub({ prs = [pr()], runs = [], issueEvents = [] } = {}) {
    const cancelled = [];
    const addedLabels = [];
    const rest = {
        pulls: {
            get: async ({ pull_number }) => ({ data: prs.find(p => p.number === pull_number) }),
            list() {}
        },
        issues: {
            listEvents() {},
            addLabels: async args => addedLabels.push(args)
        },
        actions: {
            listWorkflowRunsForRepo() {},
            cancelWorkflowRun: async ({ run_id }) => cancelled.push(run_id)
        }
    };
    const paginate = async (method, args) => {
        if (method === rest.pulls.list) return prs;
        if (method === rest.issues.listEvents) return issueEvents;
        return runs.filter(r => r.event === args.event);
    };
    return { rest, paginate, cancelled, addedLabels };
}

const prContext = { repo, issue: { number: 7 } };
const dependabotPR = (overrides = {}) => pr({ user: { login: 'dependabot[bot]' }, ...overrides });
const holdLabel = { name: 'hold-ci' };

test('holds Dependabot CI even when Dev reaches the gate before PR Assistant labels the PR', async () => {
    const github = mockGitHub({
        prs: [dependabotPR()],
        issueEvents: [{ event: 'unlabeled', label: { name: 'dependencies' } }]
    });
    assert.equal(await getPRCIHoldReason(github, prContext), 'dependabot');
    await applyDefaultPRCIHold(github, prContext);
    assert.deepEqual(github.addedLabels, [{ ...repo, issue_number: 7, labels: ['hold-ci'] }]);
});

test('releases Dependabot CI after hold-ci is removed without reapplying it', async () => {
    const github = mockGitHub({
        prs: [dependabotPR()],
        issueEvents: [{ event: 'unlabeled', label: holdLabel }]
    });
    assert.equal(await getPRCIHoldReason(github, prContext), null);
    await applyDefaultPRCIHold(github, prContext);
    assert.deepEqual(github.addedLabels, []);
});

test('honors a live hold-ci label even if it was removed previously', async () => {
    const github = mockGitHub({
        prs: [dependabotPR({ labels: [holdLabel] })],
        issueEvents: [{ event: 'unlabeled', label: holdLabel }]
    });
    assert.equal(await getPRCIHoldReason(github, prContext), 'label');
    await applyDefaultPRCIHold(github, prContext);
    assert.deepEqual(github.addedLabels, []);
});

test('ordinary PRs are held only when they have the hold-ci label', async () => {
    const pull = pr();
    const github = mockGitHub({ prs: [pull] });
    assert.equal(await getPRCIHoldReason(github, prContext), null);
    await applyDefaultPRCIHold(github, prContext);
    assert.deepEqual(github.addedLabels, []);
    pull.labels.push(holdLabel);
    assert.equal(await getPRCIHoldReason(github, prContext), 'label');
});

const closeContext = {
    repo,
    runId: 999,
    eventName: 'pull_request_target',
    payload: { action: 'closed', pull_request: { number: 7 } }
};

test('cancels unfinished runs of the closed PR, including fork runs without PR metadata', async () => {
    const github = mockGitHub({
        prs: [closedPR()],
        runs: [
            run({ id: 1 }),
            run({ id: 2, event: 'pull_request_target', pull_requests: [] }),
            run({ id: 3, status: 'completed' })
        ]
    });
    await cancelClosedPRRuns({ github, context: closeContext, core });
    assert.deepEqual(github.cancelled, [1, 2]);
});

test('never cancels Main, Release or the cleanup run itself', async () => {
    const runs = [
        run({ id: 1, event: 'push' }),
        run({ id: 2, event: 'workflow_dispatch' }),
        run({ id: 999, event: 'pull_request_target' })
    ];
    const github = mockGitHub({ prs: [closedPR()], runs });
    // Return every run regardless of the event filter to exercise the guard itself.
    github.paginate = async method => (method === github.rest.pulls.list ? [closedPR()] : runs);
    await cancelClosedPRRuns({ github, context: closeContext, core });
    assert.deepEqual(github.cancelled, []);
});

test('does not cancel when the PR has been reopened', async () => {
    const github = mockGitHub({ prs: [pr()], runs: [run()] });
    await cancelClosedPRRuns({ github, context: closeContext, core });
    assert.deepEqual(github.cancelled, []);
});

test('does not match a same-named branch from another fork', async () => {
    const other = closedPR({ head: { ref: 'feature', sha: 'head', repo: { id: 3 } } });
    const github = mockGitHub({ prs: [other], runs: [run({ pull_requests: [] })] });
    await cancelClosedPRRuns({ github, context: closeContext, core });
    assert.deepEqual(github.cancelled, []);
});

test('retries open PRs and skips closed ones', async () => {
    assert.equal(await canRetryWorkflowRun(mockGitHub(), { repo }, core, run()), true);
    assert.equal(await canRetryWorkflowRun(mockGitHub({ prs: [closedPR()] }), { repo }, core, run()), false);
});

test('skips Dev runs that are no longer on the PR head', async () => {
    const github = mockGitHub();
    assert.equal(await canRetryWorkflowRun(github, { repo }, core, run({ head_sha: 'old' })), false);
    assert.equal(await canRetryWorkflowRun(github, { repo }, core,
        run({ event: 'pull_request_target', head_sha: 'old' })), true);
});

test('Main and Release bypass the PR checks', async () => {
    const github = mockGitHub({ prs: [closedPR()] });
    for (const event of ['push', 'workflow_dispatch']) {
        assert.equal(await canRetryWorkflowRun(github, { repo }, core, run({ event, head_sha: 'old' })), true);
    }
});

test('Dev and Cloud allow one automatic retry; Main and Release keep three', () => {
    assert.equal(maxAutoRetries({ event: 'pull_request' }), 1);
    assert.equal(maxAutoRetries({ event: 'pull_request_target' }), 1);
    for (const event of ['push', 'workflow_dispatch', 'schedule']) {
        assert.equal(maxAutoRetries({ event }), 3);
    }
});
