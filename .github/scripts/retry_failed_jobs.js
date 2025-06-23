module.exports = async ({ github, context, core }) => {
    const runID = process.env.WORKFLOW_RUN_ID;
    const runURL = process.env.WORKFLOW_RUN_URL;

    core.info(`Checking failed workflow run: ${runID}`);

    const { data: workflowRun } = await github.rest.actions.getWorkflowRun({
        owner: context.repo.owner,
        repo: context.repo.repo,
        run_id: runID
    });

    core.info(`Workflow run status: ${workflowRun.status}`);
    core.info(`Workflow run conclusion: ${workflowRun.conclusion}`);

    const { data: jobs } = await github.rest.actions.listJobsForWorkflowRun({
        owner: context.repo.owner,
        repo: context.repo.repo,
        run_id: runID
    });

    core.info(`Found ${jobs.jobs.length} jobs in the workflow run`);
    for (const job of jobs.jobs) {
        core.info(`  Job ${job.name} (ID: ${job.id}) status: ${job.status}, conclusion: ${job.conclusion}`);
    }

    const failedJobs = jobs.jobs.filter(job => job.conclusion === 'failure' || job.conclusion === 'cancelled');

    if (failedJobs.length === 0) {
        core.info('No failed jobs found to retry');
        return;
    }

    core.info(`Found ${failedJobs.length} failed jobs to analyze:`);

    function isRetryableError(errorMessage) {
        if (!errorMessage) return false;
        return errorMessage.includes('The self-hosted runner lost communication with the server.') ||
            errorMessage.includes('The operation was canceled.');
    }

    function isPriorityCancelled(errorMessage) {
        if (!errorMessage) return false;
        return errorMessage.includes('Canceling since a higher priority waiting request for');
    }

    const jobsToRetry = [];
    let priorityCancelled = false;
    let analyzedJobs = [];

    for (const job of failedJobs) {
        core.info(`Analyzing job: ${job.name} (ID: ${job.id})`);

        try {
            const { data: jobDetails } = await github.rest.actions.getJobForWorkflowRun({
                owner: context.repo.owner,
                repo: context.repo.repo,
                job_id: job.id
            });

            core.info(`  Job status: ${jobDetails.status}, conclusion: ${jobDetails.conclusion}`);

            const { data: annotations } = await github.rest.checks.listAnnotations({
                owner: context.repo.owner,
                repo: context.repo.repo,
                check_run_id: jobDetails.check_run_url.split('/').pop()
            });

            if (annotations.length === 0) {
                core.info(`  No annotations found for job: ${job.name}`);
                analyzedJobs.push({
                    name: job.name,
                    retryable: false,
                    annotationCount: 0,
                    reason: 'No annotations found'
                });
                continue;
            }

            core.info(`  Found ${annotations.length} annotations for job: ${job.name}`);

            const failureAnnotations = annotations.filter(annotation => annotation.annotation_level === 'failure');

            core.info(`  Found ${failureAnnotations.length} failure-related annotations:`);
            failureAnnotations.forEach(annotation => {
                core.info(`  [${annotation.annotation_level}] ${annotation.message}`);
            });
            const allFailureAnnotationMessages = failureAnnotations.map(annotation => annotation.message).join('');

            const isPriorityCancelledJob = isPriorityCancelled(allFailureAnnotationMessages);
            if (isPriorityCancelledJob) {
                core.info(`  ⛔️ Job "${job.name}" is NOT retryable - cancelled by a higher priority request`);
                analyzedJobs.push({
                    name: job.name,
                    retryable: false,
                    annotationCount: annotations.length,
                    reason: 'Cancelled by higher priority request'
                });
                priorityCancelled = true;
                break;
            }

            const isRetryable = isRetryableError(allFailureAnnotationMessages);
            if (isRetryable) {
                core.info(`  ✅ Job "${job.name}" is retryable - infrastructure issue detected in annotations`);
                jobsToRetry.push(job);
                analyzedJobs.push({
                    name: job.name,
                    retryable: true,
                    annotationCount: annotations.length,
                    reason: 'Infrastructure issue detected'
                });
            } else {
                core.info(`  ⛔️ Job "${job.name}" is NOT retryable - likely a code/test issue based on annotations`);
                analyzedJobs.push({
                    name: job.name,
                    retryable: false,
                    annotationCount: annotations.length,
                    reason: 'Code/test issue detected'
                });
            }

        } catch (error) {
            core.error(`  Failed to analyze job ${job.name}:`, error.message);
            analyzedJobs.push({
                name: job.name,
                retryable: false,
                annotationCount: 0,
                reason: `Analysis failed: ${error.message}`
            });
        }
    }

    if (priorityCancelled) {
        core.info('Cancelling retry since a higher priority request was made');
        await addCommentToPR(github, context, core, runID, runURL, failedJobs, analyzedJobs, 0, true);
        return;
    }

    if (jobsToRetry.length === 0) {
        core.info('No jobs found with retryable errors. Skipping retry.');
        await addCommentToPR(github, context, core, runID, runURL, failedJobs, analyzedJobs, 0, false);
        return;
    }

    core.info(`Found ${jobsToRetry.length} jobs with retryable errors:`);
    jobsToRetry.forEach(job => {
        core.info(`- ${job.name} (ID: ${job.id})`);
    });

    try {
        core.info(`Retrying all failed jobs in workflow run: ${runID}`);

        await github.rest.actions.reRunWorkflowFailedJobs({
            owner: context.repo.owner,
            repo: context.repo.repo,
            run_id: runID
        });

        core.info(`✅ Successfully initiated retry for all failed jobs in workflow run: ${runID}`);
    } catch (error) {
        core.error(`❌ Failed to retry all failed jobs:`, error.message);
    }

    // Add comment to PR
    await addCommentToPR(github, context, core, runID, runURL, failedJobs, analyzedJobs, jobsToRetry.length, false);

    core.info('Retry process completed');
};

async function addCommentToPR(github, context, core, runID, runURL, failedJobs, analyzedJobs, retryableJobsCount, priorityCancelled) {
    try {
        // Get workflow run to find the branch
        const { data: workflowRun } = await github.rest.actions.getWorkflowRun({
            owner: context.repo.owner,
            repo: context.repo.repo,
            run_id: runID
        });

        // Find open PR for this branch
        const { data: pulls } = await github.rest.pulls.list({
            owner: context.repo.owner,
            repo: context.repo.repo,
            head: `${context.repo.owner}:${workflowRun.head_branch}`,
            state: 'open'
        });

        if (pulls.length === 0) {
            core.info('No open PR found for this workflow run');
            return;
        }

        const pr = pulls[0];

        let comment = `🤖 **Smart Auto-retry Analysis (Annotations-based)**
  
The workflow run [${runID}](${runURL}) failed and has been analyzed for retryable errors using job annotations.

**Analysis Results:**
- Total failed/cancelled jobs: ${failedJobs.length}
- Jobs with retryable errors: ${retryableJobsCount}
- Jobs with code/test issues: ${failedJobs.length - retryableJobsCount}`;

        if (priorityCancelled) {
            comment += `
- ⛔️ **Retry cancelled** due to higher priority request`;
        } else if (retryableJobsCount > 0) {
            comment += `
- ✅ **${retryableJobsCount} job(s) have been automatically retried** due to infrastructure issues detected in annotations (runner communication, network timeouts, resource exhaustion, etc.)

You can monitor the retry progress in the [Actions tab](${runURL}).`;
        } else {
            comment += `
- ❌ **No jobs were retried** because all failures appear to be code or test related issues that require manual fixes.`;
        }

        comment += `

**Job Analysis (based on annotations):**
${analyzedJobs.map(job => {
            if (job.reason.includes('Analysis failed')) {
                return `- ${job.name}: ❓ ${job.reason}`;
            }
            if (job.reason.includes('Cancelled by higher priority')) {
                return `- ${job.name}: ⛔️ ${job.reason}`;
            }
            if (job.reason.includes('No annotations found')) {
                return `- ${job.name}: ❓ ${job.reason}`;
            }
            return `- ${job.name}: ${job.retryable ? '🔄 Retryable (infrastructure)' : '❌ Not retryable (code/test)'} - ${job.reason} (${job.annotationCount} annotations)`;
        }).join('\n')}

---
*This is an automated analysis and retry triggered by the smart retry workflow using job annotations.*`;

        await github.rest.issues.createComment({
            owner: context.repo.owner,
            repo: context.repo.repo,
            issue_number: pr.number,
            body: comment
        });

        core.info(`Added smart retry analysis comment to PR #${pr.number}`);
    } catch (error) {
        core.error(`Failed to add comment to PR:`, error.message);
    }
} 
