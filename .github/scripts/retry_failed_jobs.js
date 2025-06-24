function isRetryableError(errorMessage) {
    if (!errorMessage) return false;
    return errorMessage.includes('The self-hosted runner lost communication with the server.') ||
        errorMessage.includes('The operation was canceled.');
}

function isPriorityCancelled(errorMessage) {
    if (!errorMessage) return false;
    return errorMessage.includes('Canceling since a higher priority waiting request for');
}

async function analyzeJob(github, context, core, job) {
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
            return {
                job,
                retryable: false,
                annotationCount: 0,
                reason: 'No annotations found',
                priorityCancelled: false
            };
        }

        core.info(`  Found ${annotations.length} annotations for job: ${job.name}`);

        const failureAnnotations = annotations.filter(annotation => annotation.annotation_level === 'failure');

        core.info(`  Found ${failureAnnotations.length} failure-related annotations:`);
        failureAnnotations.forEach(annotation => {
            core.info(`  > [${annotation.annotation_level}] ${annotation.message}`);
        });

        const allFailureAnnotationMessages = failureAnnotations.map(annotation => annotation.message).join('');

        const priorityCancelled = isPriorityCancelled(allFailureAnnotationMessages);
        if (priorityCancelled) {
            core.info(`  ⛔️ Job "${job.name}" is NOT retryable - cancelled by a higher priority request`);
            return {
                job,
                retryable: false,
                annotationCount: annotations.length,
                reason: 'Cancelled by higher priority request',
                priorityCancelled: true
            };
        }

        const isRetryable = isRetryableError(allFailureAnnotationMessages);
        if (isRetryable) {
            core.info(`  ✅ Job "${job.name}" is retryable - infrastructure issue detected in annotations`);
        } else {
            core.info(`  ⛔️ Job "${job.name}" is NOT retryable - likely a code/test issue based on annotations`);
        }

        return {
            job,
            retryable: isRetryable,
            annotationCount: annotations.length,
            reason: isRetryable ? 'Infrastructure issue detected' : 'Code/test issue detected',
            priorityCancelled: false
        };

    } catch (error) {
        core.error(`  Failed to analyze job ${job.name}:`, error.message);
        return {
            job,
            retryable: false,
            annotationCount: 0,
            reason: `Analysis failed: ${error.message}`,
            priorityCancelled: false
        };
    }
}

async function analyzeAllJobs(github, context, core, failedJobs) {
    const jobsToRetry = [];
    const analyzedJobs = [];
    let priorityCancelled = false;

    for (const job of failedJobs) {
        const analysis = await analyzeJob(github, context, core, job);

        analyzedJobs.push({
            name: job.name,
            retryable: analysis.retryable,
            annotationCount: analysis.annotationCount,
            reason: analysis.reason
        });

        if (analysis.priorityCancelled) {
            priorityCancelled = true;
            break;
        }

        if (analysis.retryable) {
            jobsToRetry.push(job);
        }
    }

    return { jobsToRetry, analyzedJobs, priorityCancelled };
}

async function retryFailedJobs(github, context, core, runID, jobsToRetry) {
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
        return true;
    } catch (error) {
        core.error(`❌ Failed to retry all failed jobs:`, error.message);
        return false;
    }
}

async function getWorkflowInfo(github, context, core, runID) {
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

    const failedJobs = jobs.jobs.filter(job => (job.conclusion === 'failure' || job.conclusion === 'cancelled') && job.name !== 'ready');

    if (failedJobs.length === 0) {
        core.info('No failed jobs found to retry');
        return { failedJobs: [], workflowRun };
    }

    core.info(`Found ${failedJobs.length} failed jobs to analyze:`);
    return { failedJobs, workflowRun };
}

async function findRelatedPR(github, context, core, workflowRun) {
    // Try to find PR from context first (most reliable)
    if (context.payload.pull_request) {
        core.info(`Found PR from context: #${context.payload.pull_request.number}`);
        return context.payload.pull_request;
    }

    // Try to find PR by commit SHA
    core.info(`Finding related PR for commit SHA: ${workflowRun.head_sha}`);
    try {
        const { data: pulls } = await github.rest.repos.listPullRequestsAssociatedWithCommit({
            owner: context.repo.owner,
            repo: context.repo.repo,
            commit_sha: workflowRun.head_sha
        });

        if (pulls.length > 0) {
            // Filter for open PRs and get the most recently updated one
            const openPulls = pulls.filter(pr => pr.state === 'open');
            if (openPulls.length > 0) {
                const latestPR = openPulls.sort((a, b) => new Date(b.updated_at) - new Date(a.updated_at))[0];
                core.info(`Found PR by commit SHA ${workflowRun.head_sha}: #${latestPR.number}`);
                return latestPR;
            }
        }
    } catch (error) {
        core.warning(`Failed to find PR by commit SHA ${workflowRun.head_sha}: ${error.message}`);
    }

    // Fallback: try to find PR by branch (handles fork scenarios)
    if (workflowRun.head_branch) {
        try {
            // First try with the current owner (for non-fork scenarios)
            const { data: pulls } = await github.rest.pulls.list({
                owner: context.repo.owner,
                repo: context.repo.repo,
                head: `${context.repo.owner}:${workflowRun.head_branch}`,
                state: 'open',
                sort: 'updated',
                direction: 'desc'
            });

            if (pulls.length > 0) {
                core.info(`Found PR from branch ${workflowRun.head_branch}: #${pulls[0].number}`);
                return pulls[0];
            }

            // If not found, try to find PRs from forks
            const { data: allPulls } = await github.rest.pulls.list({
                owner: context.repo.owner,
                repo: context.repo.repo,
                state: 'open',
                sort: 'updated',
                direction: 'desc',
                per_page: 100 // Get more PRs to search through
            });

            // Look for PRs that match the branch name (could be from forks)
            const matchingPulls = allPulls.filter(pr => {
                // Check if the PR's head ref matches our branch
                return pr.head.ref === workflowRun.head_branch ||
                    pr.head.label.endsWith(`:${workflowRun.head_branch}`);
            });

            if (matchingPulls.length > 0) {
                const latestPR = matchingPulls.sort((a, b) => new Date(b.updated_at) - new Date(a.updated_at))[0];
                core.info(`Found PR from fork branch ${workflowRun.head_branch}: #${latestPR.number} (from ${latestPR.head.user.login})`);
                return latestPR;
            }

        } catch (error) {
            core.warning(`Failed to find PR by branch ${workflowRun.head_branch}: ${error.message}`);
        }
    }

    return null;
}

async function findExistingRetryComment(github, context, core, pr) {
    try {
        // Get comments for the PR
        const { data: comments } = await github.rest.issues.listComments({
            owner: context.repo.owner,
            repo: context.repo.repo,
            issue_number: pr.number,
            per_page: 100
        });

        // Look for our smart retry analysis comment
        const retryComment = comments.find(comment =>
            comment.user.type === 'Bot' &&
            comment.body.includes('## 🤖 Smart Auto-retry Analysis')
        );

        if (retryComment) {
            core.info(`Found existing retry analysis comment: ${retryComment.id}`);
            return retryComment;
        }

        core.info('No existing retry analysis comment found');
        return null;
    } catch (error) {
        core.warning(`Failed to find existing retry comment: ${error.message}`);
        return null;
    }
}

async function addCommentToPR(github, context, core, runID, runURL, failedJobs, analyzedJobs, retryableJobsCount, priorityCancelled) {
    try {
        // Get workflow run to find the branch
        const { data: workflowRun } = await github.rest.actions.getWorkflowRun({
            owner: context.repo.owner,
            repo: context.repo.repo,
            run_id: runID
        });

        // Find related PR
        const pr = await findRelatedPR(github, context, core, workflowRun);

        if (!pr) {
            core.info('No related PR found, skipping comment');
            return;
        }

        let comment = `## 🤖 Smart Auto-retry Analysis

> **Workflow Run:** [\`${runID}\`](${runURL})

The workflow run has been analyzed for retryable errors using job annotations.

---

### 📊 Analysis Summary

| Metric | Count |
|--------|-------|
| **Total Failed/Cancelled Jobs** | \`${failedJobs.length}\` |
| **Jobs with Retryable Errors** | \`${retryableJobsCount}\` |
| **Jobs with Code/Test Issues** | \`${failedJobs.length - retryableJobsCount}\` |`;

        if (priorityCancelled) {
            comment += `

### ⛔️ Retry Status: **CANCELLED**

> **Reason:** Higher priority request detected - retry has been cancelled to avoid resource conflicts.`;
        } else if (retryableJobsCount > 0) {
            comment += `

### ✅ Retry Status: **AUTOMATIC RETRY INITIATED**

> **${retryableJobsCount} job(s)** have been automatically retried due to infrastructure issues detected in annotations:
> - Runner communication failures
> - Network timeouts
> - Resource exhaustion
> - Other transient infrastructure problems

**📈 Monitor Progress:** [View in Actions](${runURL})`;
        } else {
            comment += `

### ❌ Retry Status: **NO RETRY NEEDED**

> All failures appear to be **code or test related issues** that require manual fixes rather than automatic retries.`;
        }

        comment += `

---

### 🔍 Detailed Job Analysis

${analyzedJobs.map(job => {
            if (job.reason.includes('Analysis failed')) {
                return `#### ❓ **${job.name}**
> **Status:** Analysis failed  
> **Reason:** ${job.reason}`;
            }
            if (job.reason.includes('Cancelled by higher priority')) {
                return `#### ⛔️ **${job.name}**
> **Status:** Cancelled by higher priority request  
> **Reason:** ${job.reason}`;
            }
            if (job.reason.includes('No annotations found')) {
                return `#### ❓ **${job.name}**
> **Status:** No annotations available  
> **Reason:** ${job.reason}`;
            }
            if (job.retryable) {
                return `#### 🔄 **${job.name}**
> **Status:** ✅ **Retryable** (Infrastructure Issue)  
> **Reason:** ${job.reason}  
> **Annotations:** ${job.annotationCount} found`;
            } else {
                return `#### ❌ **${job.name}**
> **Status:** Not retryable (Code/Test Issue)  
> **Reason:** ${job.reason}  
> **Annotations:** ${job.annotationCount} found`;
            }
        }).join('\n\n')}

---

<details>
<summary>🤖 About This Analysis</summary>

This is an **automated analysis and retry** triggered by the smart retry workflow using job annotations. The system analyzes failure patterns to distinguish between:

- **🔄 Infrastructure Issues:** Runner failures, network timeouts, resource exhaustion
- **❌ Code/Test Issues:** Compilation errors, test failures, logic problems

Only infrastructure issues are automatically retried to avoid wasting resources on code problems that need manual fixes.

</details>`;

        // Try to find existing retry comment
        const existingComment = await findExistingRetryComment(github, context, core, pr);

        if (existingComment) {
            // Update existing comment
            await github.rest.issues.updateComment({
                owner: context.repo.owner,
                repo: context.repo.repo,
                comment_id: existingComment.id,
                body: comment
            });
            core.info(`Updated existing smart retry analysis comment on PR #${pr.number}`);
        } else {
            // Create new comment
            await github.rest.issues.createComment({
                owner: context.repo.owner,
                repo: context.repo.repo,
                issue_number: pr.number,
                body: comment
            });
            core.info(`Added new smart retry analysis comment to PR #${pr.number}`);
        }
    } catch (error) {
        core.error(`Failed to add/update comment to PR:`, error.message);
    }
}

module.exports = async ({ github, context, core }) => {
    const runID = process.env.WORKFLOW_RUN_ID;
    const runURL = process.env.WORKFLOW_RUN_URL;

    // Get workflow information and failed jobs
    const { failedJobs, workflowRun } = await getWorkflowInfo(github, context, core, runID);

    if (failedJobs.length === 0) {
        return;
    }

    // Analyze all failed jobs
    const { jobsToRetry, analyzedJobs, priorityCancelled } = await analyzeAllJobs(github, context, core, failedJobs);

    // Handle priority cancellation
    if (priorityCancelled) {
        core.info('Cancelling retry since a higher priority request was made');
        await addCommentToPR(github, context, core, runID, runURL, failedJobs, analyzedJobs, 0, true);
        return;
    }

    // Handle no retryable jobs
    if (jobsToRetry.length === 0) {
        core.info('No jobs found with retryable errors. Skipping retry.');
        await addCommentToPR(github, context, core, runID, runURL, failedJobs, analyzedJobs, 0, false);
        return;
    }

    // Retry failed jobs
    await retryFailedJobs(github, context, core, runID, jobsToRetry);

    // Add comment to PR
    await addCommentToPR(github, context, core, runID, runURL, failedJobs, analyzedJobs, jobsToRetry.length, false);

    core.info('Retry process completed');
};
