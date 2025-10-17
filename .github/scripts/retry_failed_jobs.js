const TITLE = '## 🤖 CI Job Analysis';

function isRetryableError(errorMessage) {
    if (!errorMessage) return false;
    return errorMessage.includes('The self-hosted runner lost communication with the server.') ||
        errorMessage.includes('The operation was canceled.') ||
        errorMessage.includes('Process completed with exit code 143.');
}

function isUserCancelled(errorMessage) {
    if (!errorMessage) return false;
    return errorMessage.includes('The run was canceled by @');
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

        const userCancelled = isUserCancelled(allFailureAnnotationMessages);
        if (userCancelled) {
            core.info(`  ⛔️ Job "${job.name}" is NOT retryable - cancelled by user`);
            return {
                job,
                retryable: false,
                annotationCount: annotations.length,
                reason: 'Cancelled by user',
                priorityCancelled: false
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

    // Get all jobs with pagination
    let allJobs = [];
    let page = 1;
    const perPage = 100; // Use maximum page size

    while (true) {
        const { data: jobsResponse } = await github.rest.actions.listJobsForWorkflowRun({
            owner: context.repo.owner,
            repo: context.repo.repo,
            run_id: runID,
            per_page: perPage,
            page: page
        });

        allJobs = allJobs.concat(jobsResponse.jobs);

        if (jobsResponse.jobs.length < perPage) {
            break;
        }

        page++;
    }

    core.info(`Found ${allJobs.length} jobs in the workflow run`);
    for (const job of allJobs) {
        core.info(`  Job ${job.name} (ID: ${job.id}) status: ${job.status}, conclusion: ${job.conclusion}`);
    }

    const failedJobs = allJobs.filter(job => (job.conclusion === 'failure' || job.conclusion === 'cancelled') && job.name !== 'ready');

    if (failedJobs.length === 0) {
        core.info('No failed jobs found to retry');
        return { failedJobs: [], workflowRun, totalJobs: allJobs.length };
    }

    core.info(`Found ${failedJobs.length} failed jobs to analyze:`);
    return { failedJobs, workflowRun, totalJobs: allJobs.length };
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
            comment.body.includes(TITLE)
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

async function getRetryCountFromAPI(github, context, core, workflowRun) {
    try {
        // Get workflow runs for the same branch/commit
        const { data: workflowRuns } = await github.rest.actions.listWorkflowRuns({
            owner: context.repo.owner,
            repo: context.repo.repo,
            workflow_id: workflowRun.workflow_id,
            branch: workflowRun.head_branch,
            per_page: 100
        });

        // Count runs that were retries (have the same head_sha)
        let retryCount = 0;
        const currentSha = workflowRun.head_sha;

        for (const run of workflowRuns.workflow_runs) {
            if (run.head_sha === currentSha && run.id !== workflowRun.id) {
                // This is a previous run with the same commit SHA
                retryCount++;
            }
        }

        core.info(`Found ${retryCount} previous workflow runs for the same commit`);
        return retryCount;
    } catch (error) {
        core.warning(`Failed to get retry count from API: ${error.message}`);
        return 0;
    }
}

async function addCommentToPR(github, context, core, runID, runURL, jobData, priorityCancelled, maxRetriesReached = false) {
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

        // Try to find existing retry comment
        const existingComment = await findExistingRetryComment(github, context, core, pr);

        // Get current retry count from API
        const currentRetryCount = await getRetryCountFromAPI(github, context, core, workflowRun);
        const newRetryCount = jobData.retryableJobsCount > 0 ? currentRetryCount + 1 : currentRetryCount;

        // Build title with retry count
        const titleSuffix = newRetryCount > 0 ? ` (Retry ${newRetryCount})` : '';

        // Calculate code issues count (exclude priority cancelled)
        const codeIssuesCount = priorityCancelled ? 0 : (jobData.failedJobs.length - jobData.retryableJobsCount);

        let comment;

        if (priorityCancelled) {
            // Simplified comment for priority cancelled workflow
            comment = `${TITLE}${titleSuffix}

> **Workflow:** [\`${runID}\`](${runURL})

### ⛔️ **CANCELLED**
Higher priority request detected - retry cancelled to avoid conflicts.

[View Workflow](${runURL})`;
        } else if (maxRetriesReached) {
            // Comment for when max retries reached
            comment = `${TITLE}${titleSuffix}

> **Workflow:** [\`${runID}\`](${runURL})

### 📊 Summary
- **Total Jobs:** ${jobData.totalJobs}
- **Failed Jobs:** ${jobData.failedJobs.length}
- **Retryable:** ${jobData.retryableJobsCount}
- **Code Issues:** ${codeIssuesCount}

### 🚫 **MAX RETRIES REACHED**
Maximum retry count (3) has been reached. Manual intervention required.

[View Workflow](${runURL})`;

            comment += `

### 🔍 Job Details
${jobData.analyzedJobs.map(job => {
                if (job.reason.includes('Analysis failed')) {
                    return `- ❓ **${job.name}**: Analysis failed`;
                }
                if (job.reason.includes('Cancelled by higher priority')) {
                    return `- ⛔️ **${job.name}**: Cancelled by higher priority`;
                }
                if (job.reason.includes('Cancelled by user')) {
                    return `- 🚫 **${job.name}**: Cancelled by user`;
                }
                if (job.reason.includes('No annotations found')) {
                    return `- ❓ **${job.name}**: No annotations available`;
                }
                if (job.retryable) {
                    return `- 🔄 **${job.name}**: ✅ Retryable (Infrastructure)`;
                } else {
                    return `- ❌ **${job.name}**: Not retryable (Code/Test)`;
                }
            }).join('\n')}

---

<details>
<summary>🤖 About</summary>

Automated analysis using job annotations to distinguish infrastructure issues (auto-retried) from code/test issues (manual fixes needed).
</details>`;
        } else {
            // Full comment for normal analysis
            comment = `${TITLE}${titleSuffix}

> **Workflow:** [\`${runID}\`](${runURL})

### 📊 Summary
- **Total Jobs:** ${jobData.totalJobs}
- **Failed Jobs:** ${jobData.failedJobs.length}
- **Retryable:** ${jobData.retryableJobsCount}
- **Code Issues:** ${codeIssuesCount}`;

            if (jobData.retryableJobsCount > 0) {
                comment += `

### ✅ **AUTO-RETRY INITIATED**
**${jobData.retryableJobsCount} job(s)** retried due to infrastructure issues (runner failures, timeouts, etc.)

[View Progress](${runURL})`;
            } else {
                comment += `

### ❌ **NO RETRY NEEDED**
All failures appear to be code/test issues requiring manual fixes.`;
            }

            comment += `

### 🔍 Job Details
${jobData.analyzedJobs.map(job => {
                if (job.reason.includes('Analysis failed')) {
                    return `- ❓ **${job.name}**: Analysis failed`;
                }
                if (job.reason.includes('Cancelled by higher priority')) {
                    return `- ⛔️ **${job.name}**: Cancelled by higher priority`;
                }
                if (job.reason.includes('Cancelled by user')) {
                    return `- 🚫 **${job.name}**: Cancelled by user`;
                }
                if (job.reason.includes('No annotations found')) {
                    return `- ❓ **${job.name}**: No annotations available`;
                }
                if (job.retryable) {
                    return `- 🔄 **${job.name}**: ✅ Retryable (Infrastructure)`;
                } else {
                    return `- ❌ **${job.name}**: Not retryable (Code/Test)`;
                }
            }).join('\n')}

---

<details>
<summary>🤖 About</summary>

Automated analysis using job annotations to distinguish infrastructure issues (auto-retried) from code/test issues (manual fixes needed).
</details>`;
        }

        if (existingComment) {
            // Update existing comment
            await github.rest.issues.updateComment({
                owner: context.repo.owner,
                repo: context.repo.repo,
                comment_id: existingComment.id,
                body: comment
            });
            core.info(`Updated existing smart retry analysis comment on PR #${pr.number} (Retry ${newRetryCount})`);
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

async function deleteRetryComment(github, context, core, runID) {
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
            core.info('No related PR found, skipping comment deletion');
            return;
        }

        // Try to find existing retry comment
        const existingComment = await findExistingRetryComment(github, context, core, pr);

        if (existingComment) {
            // Delete existing comment
            await github.rest.issues.deleteComment({
                owner: context.repo.owner,
                repo: context.repo.repo,
                comment_id: existingComment.id
            });
            core.info(`Deleted smart retry analysis comment from PR #${pr.number}`);
        } else {
            core.info('No existing retry analysis comment found to delete');
        }
    } catch (error) {
        core.error(`Failed to delete comment from PR:`, error.message);
    }
}

module.exports = async ({ github, context, core }) => {
    const runID = process.env.WORKFLOW_RUN_ID;
    const runURL = process.env.WORKFLOW_RUN_URL;
    const conclusion = process.env.CONCLUSION;

    // Check if workflow succeeded - if so, delete any existing retry comments and exit
    if (conclusion === 'success') {
        core.info('Workflow succeeded - deleting any existing retry analysis comments');
        await deleteRetryComment(github, context, core, runID);
        return;
    }

    // Get workflow information and failed jobs
    const { failedJobs, workflowRun, totalJobs } = await getWorkflowInfo(github, context, core, runID);

    if (failedJobs.length === 0) {
        return;
    }

    // Analyze all failed jobs
    const { jobsToRetry, analyzedJobs, priorityCancelled } = await analyzeAllJobs(github, context, core, failedJobs);

    // Handle priority cancellation
    if (priorityCancelled) {
        core.info('Cancelling retry since a higher priority request was made');
        await addCommentToPR(github, context, core, runID, runURL, { failedJobs, analyzedJobs, totalJobs, retryableJobsCount: 0 }, true);
        return;
    }

    // Check retry count limit (max 3 retries)
    const currentRetryCount = await getRetryCountFromAPI(github, context, core, workflowRun);
    const maxRetries = 3;

    if (currentRetryCount >= maxRetries) {
        core.info(`Maximum retry count (${maxRetries}) reached. Skipping retry.`);
        await addCommentToPR(github, context, core, runID, runURL, { failedJobs, analyzedJobs, totalJobs, retryableJobsCount: 0 }, false, true);
        return;
    }

    // Handle no retryable jobs
    if (jobsToRetry.length === 0) {
        core.info('No jobs found with retryable errors. Skipping retry.');
        await addCommentToPR(github, context, core, runID, runURL, { failedJobs, analyzedJobs, totalJobs, retryableJobsCount: 0 }, false);
        return;
    }

    // Retry failed jobs
    await retryFailedJobs(github, context, core, runID, jobsToRetry);

    // Add comment to PR
    await addCommentToPR(github, context, core, runID, runURL, { failedJobs, analyzedJobs, totalJobs, retryableJobsCount: jobsToRetry.length }, false);

    core.info('Retry process completed');
};
