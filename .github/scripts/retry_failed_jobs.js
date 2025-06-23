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
        core.info(`Job ${job.name} (ID: ${job.id}) status: ${job.status}, conclusion: ${job.conclusion}`);
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

    const jobsToRetry = [];

    for (const job of failedJobs) {
        core.info(`Analyzing job: ${job.name} (ID: ${job.id})`);

        try {
            const { data: jobDetails } = await github.rest.actions.getJobForWorkflowRun({
                owner: context.repo.owner,
                repo: context.repo.repo,
                job_id: job.id
            });

            core.info(`Job status: ${jobDetails.status}, conclusion: ${jobDetails.conclusion}`);

            const { data: annotations } = await github.rest.checks.listAnnotations({
                owner: context.repo.owner,
                repo: context.repo.repo,
                check_run_id: jobDetails.check_run_url.split('/').pop()
            });

            core.info(`Found ${annotations.length} annotations for job: ${job.name}`);

            const errorAnnotations = annotations.filter(annotation =>
                annotation.annotation_level === 'failure' ||
                annotation.annotation_level === 'warning' ||
                annotation.message.toLowerCase().includes('error') ||
                annotation.message.toLowerCase().includes('failed') ||
                annotation.message.toLowerCase().includes('timeout') ||
                annotation.message.toLowerCase().includes('connection') ||
                annotation.message.toLowerCase().includes('runner')
            );

            core.info(`Found ${errorAnnotations.length} error-related annotations:`);
            errorAnnotations.forEach(annotation => {
                core.info(`  [${annotation.annotation_level}] ${annotation.message}`);
            });

            const allAnnotationMessages = annotations.map(annotation => annotation.message).join('');

            const isRetryable = isRetryableError(allAnnotationMessages);

            if (isRetryable) {
                core.info(`✅ Job "${job.name}" is retryable - infrastructure issue detected in annotations`);
                jobsToRetry.push(job);
            } else {
                core.info(`❌ Job "${job.name}" is NOT retryable - likely a code/test issue based on annotations`);
            }

        } catch (error) {
            core.error(`Failed to analyze job ${job.name}:`, error.message);
        }
    }

    if (jobsToRetry.length === 0) {
        core.info('No jobs found with retryable errors. Skipping retry.');
        return;
    }

    core.info(`Retrying ${jobsToRetry.length} jobs with retryable errors:`);
    jobsToRetry.forEach(job => {
        core.info(`- ${job.name} (ID: ${job.id})`);
    });

    for (const job of jobsToRetry) {
        try {
            core.info(`Retrying job: ${job.name} (ID: ${job.id})`);

            await github.rest.actions.reRunJob({
                owner: context.repo.owner,
                repo: context.repo.repo,
                job_id: job.id
            });

            core.info(`✅ Successfully initiated retry for job: ${job.name}`);
        } catch (error) {
            core.error(`❌ Failed to retry job ${job.name}:`, error.message);
        }
    }

    core.info('Retry process completed');
}; 
