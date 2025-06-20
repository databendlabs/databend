module.exports = async ({ github, context, core }) => {
    const runID = process.env.WORKFLOW_RUN_ID;
    const runURL = process.env.WORKFLOW_RUN_URL;

    const { data: workflowRun } = await github.rest.actions.getWorkflowRun({
        owner: context.repo.owner,
        repo: context.repo.repo,
        run_id: runID
    });

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

    const { data: jobs } = await github.rest.actions.listJobsForWorkflowRun({
        owner: context.repo.owner,
        repo: context.repo.repo,
        run_id: runID
    });

    const failedJobs = jobs.jobs.filter(job => job.conclusion === 'failure');

    function isRetryableError(errorMessage) {
        if (!errorMessage) return false;

        return errorMessage.includes('The self-hosted runner lost communication with the server.') ||
            errorMessage.includes('The operation was canceled.');
    }

    let retryableJobsCount = 0;
    let analyzedJobs = [];

    for (const job of failedJobs) {
        try {
            const { data: jobDetails } = await github.rest.actions.getJobForWorkflowRun({
                owner: context.repo.owner,
                repo: context.repo.repo,
                job_id: job.id
            });

            const { data: annotations } = await github.rest.checks.listAnnotations({
                owner: context.repo.owner,
                repo: context.repo.repo,
                check_run_id: jobDetails.check_run_url.split('/').pop()
            });

            const allAnnotationMessages = annotations.map(annotation => annotation.message).join('\n');
            const isRetryable = isRetryableError(allAnnotationMessages);

            analyzedJobs.push({
                name: job.name,
                retryable: isRetryable,
                annotationCount: annotations.length
            });

            if (isRetryable) {
                retryableJobsCount++;
            }
        } catch (error) {
            core.info(`Could not analyze job ${job.name}: ${error.message}`);
            analyzedJobs.push({
                name: job.name,
                retryable: false,
                annotationCount: 0,
                error: error.message
            });
        }
    }

    const comment = `🤖 **Smart Auto-retry Analysis (Annotations-based)**
  
  The workflow run [${runID}](${runURL}) failed and has been analyzed for retryable errors using job annotations.
  
  **Analysis Results:**
  - Total failed jobs: ${failedJobs.length}
  - Jobs with retryable errors: ${retryableJobsCount}
  - Jobs with code/test issues: ${failedJobs.length - retryableJobsCount}
  
  ${retryableJobsCount > 0 ?
            `✅ **${retryableJobsCount} job(s) have been automatically retried** due to infrastructure issues detected in annotations (runner communication, network timeouts, resource exhaustion, etc.)
    
    You can monitor the retry progress in the [Actions tab](${runURL}).` :
            `❌ **No jobs were retried** because all failures appear to be code or test related issues that require manual fixes.`
        }
  
  **Job Analysis (based on annotations):**
  ${analyzedJobs.map(job => {
            if (job.error) {
                return `- ${job.name}: ❓ Analysis failed (${job.error})`;
            }
            return `- ${job.name}: ${job.retryable ? '🔄 Retryable (infrastructure)' : '❌ Not retryable (code/test)'} (${job.annotationCount} annotations)`;
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
}; 
