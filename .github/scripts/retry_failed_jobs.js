module.exports = async ({ github, context, core }) => {
    const runID = process.env.WORKFLOW_RUN_ID;

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
                priorityCancelled = true;
                break;
            }

            const isRetryable = isRetryableError(allFailureAnnotationMessages);
            if (isRetryable) {
                core.info(`  ✅ Job "${job.name}" is retryable - infrastructure issue detected in annotations`);
                jobsToRetry.push(job);
            } else {
                core.info(`  ⛔️ Job "${job.name}" is NOT retryable - likely a code/test issue based on annotations`);
            }

        } catch (error) {
            core.error(`  Failed to analyze job ${job.name}:`, error.message);
        }
    }

    if (priorityCancelled) {
        core.info('Cancelling retry since a higher priority request was made');
        return;
    }

    if (jobsToRetry.length === 0) {
        core.info('No jobs found with retryable errors. Skipping retry.');
        return;
    }

    core.info(`Found ${jobsToRetry.length} jobs with retryable errors. Retrying all failed jobs:`);
    jobsToRetry.forEach(job => {
        core.info(`- ${job.name} (ID: ${job.id})`);
    });

    try {
        core.info(`Retrying all failed jobs in workflow run: ${runID}`);

        await github.rest.actions.reRunWorkflow({
            owner: context.repo.owner,
            repo: context.repo.repo,
            run_id: runID
        });

        core.info(`✅ Successfully initiated retry for all failed jobs in workflow run: ${runID}`);
    } catch (error) {
        core.error(`❌ Failed to retry all failed jobs:`, error.message);
    }

    core.info('Retry process completed');
}; 
