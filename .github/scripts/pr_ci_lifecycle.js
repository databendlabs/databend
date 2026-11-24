const HOLD_CI_LABEL = 'hold-ci';
const PR_EVENTS = new Set(['pull_request', 'pull_request_target']);
const ACTIVE_STATUSES = new Set(['queued', 'in_progress', 'waiting', 'pending', 'requested']);
// Every PR retry reruns all failed jobs, which can fan out to dozens of
// downstream jobs. Allow one retry for transient runner failures; repeated
// failures usually mean runner capacity is unavailable, and more retries only
// add to the queue. Main and Release keep the previous limit.
const PR_MAX_AUTO_RETRIES = 1;
const DEFAULT_MAX_AUTO_RETRIES = 3;

function maxAutoRetries(run) {
    return PR_EVENTS.has(run?.event) ? PR_MAX_AUTO_RETRIES : DEFAULT_MAX_AUTO_RETRIES;
}

async function getPRCIHoldReason(github, context) {
    // Read live labels so a full rerun after removing hold-ci sees the change.
    const number = context.issue.number;
    const { data: pr } = await github.rest.pulls.get({ ...context.repo, pull_number: number });
    if (pr.labels.some(label => label.name === HOLD_CI_LABEL)) return 'label';
    if (pr.user.login !== 'dependabot[bot]') return null;

    // Dev can start before PR Assistant adds the label. Only an explicit
    // removal releases the default hold, even if the labeling job is delayed.
    const events = await github.paginate(github.rest.issues.listEvents, {
        ...context.repo,
        issue_number: number,
        per_page: 100
    });
    return events.some(event => event.event === 'unlabeled' && event.label?.name === HOLD_CI_LABEL)
        ? null : 'dependabot';
}

async function applyDefaultPRCIHold(github, context) {
    if (await getPRCIHoldReason(github, context) !== 'dependabot') return;
    await github.rest.issues.addLabels({
        ...context.repo,
        issue_number: context.issue.number,
        labels: [HOLD_CI_LABEL]
    });
}

async function getRunPR(github, context, core, run) {
    const repo = context.repo;
    let number;
    if (run.pull_requests?.length === 1) {
        number = run.pull_requests[0].number;
    } else if (run.pull_requests?.length > 1) {
        core.warning(`Cannot uniquely identify the PR for run ${run.id}; skipping`);
        return null;
    } else {
        // Fork runs often have an empty pull_requests array. Do not match on
        // branch alone: different forks can use the same branch name.
        const headRepo = run.head_repository;
        if (!headRepo?.owner?.login || !run.head_branch || !run.created_at) {
            core.warning(`Missing PR identity for run ${run.id}; skipping`);
            return null;
        }
        const pulls = await github.paginate(github.rest.pulls.list, {
            ...repo,
            head: `${headRepo.owner.login}:${run.head_branch}`,
            state: 'all',
            per_page: 100
        });
        const matches = pulls.filter(pr =>
            pr.head.repo?.id === headRepo.id &&
            pr.head.ref === run.head_branch &&
            pr.created_at <= run.created_at &&
            (!pr.closed_at || pr.closed_at >= run.created_at)
        );
        if (matches.length !== 1) {
            core.warning(`Cannot uniquely identify the PR for run ${run.id}; skipping`);
            return null;
        }
        number = matches[0].number;
    }
    // Always read live state, including after a potentially lengthy list call.
    const { data: pr } = await github.rest.pulls.get({ ...repo, pull_number: number });
    return pr;
}

async function canRetryWorkflowRun(github, context, core, run) {
    // Main (push) and Release (workflow_dispatch) retain their existing behavior,
    // even when GitHub associates their commits with a closed/merged PR.
    if (!PR_EVENTS.has(run.event)) return true;

    const pr = await getRunPR(github, context, core, run);
    if (!pr) return false;
    if (pr.state !== 'open') {
        core.info(`Skipping retry of run ${run.id}: PR #${pr.number} is closed`);
        return false;
    }
    // pull_request_target can report the base SHA rather than the PR head SHA.
    // Only pull_request runs have a head SHA we can safely compare here.
    if (run.event === 'pull_request' && run.head_sha !== pr.head.sha) {
        core.info(`Skipping retry of run ${run.id}: PR #${pr.number} has a newer head`);
        return false;
    }
    return true;
}

async function cancelClosedPRRuns({ github, context, core }) {
    if (context.eventName !== 'pull_request_target' || context.payload.action !== 'closed') return;
    const number = context.payload.pull_request.number;
    const { data: pr } = await github.rest.pulls.get({ ...context.repo, pull_number: number });
    // A delayed close event must not cancel work after the PR has been reopened.
    if (pr.state !== 'closed') {
        core.info(`PR #${number} is open again; skipping cancellation`);
        return;
    }

    for (const event of PR_EVENTS) {
        const runs = await github.paginate(github.rest.actions.listWorkflowRunsForRepo, {
            ...context.repo,
            event,
            branch: pr.head.ref,
            created: `>=${pr.created_at}`,
            per_page: 100
        });
        for (const run of runs) {
            // Defense in depth: never cancel push/dispatch runs, completed runs,
            // or this cleanup workflow itself.
            if (!PR_EVENTS.has(run.event) || !ACTIVE_STATUSES.has(run.status) ||
                run.id === context.runId) continue;
            const related = await getRunPR(github, context, core, run);
            if (!related || related.number !== number || related.state !== 'closed') continue;
            try {
                await github.rest.actions.cancelWorkflowRun({ ...context.repo, run_id: run.id });
                core.info(`Requested cancellation of PR #${number} run ${run.id}`);
            } catch (error) {
                // Completing between enumeration and cancellation is harmless.
                // Do not hide permission errors or other cancellation failures.
                if (error.status !== 409) throw error;
                const { data: current } = await github.rest.actions.getWorkflowRun({
                    ...context.repo, run_id: run.id
                });
                if (current.status !== 'completed') throw error;
                core.info(`Run ${run.id} already completed; skipping`);
            }
        }
    }
}

module.exports = { applyDefaultPRCIHold, canRetryWorkflowRun, cancelClosedPRRuns, getPRCIHoldReason, maxAutoRetries };
