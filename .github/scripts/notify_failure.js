module.exports = async ({ context, core }) => {
  const {
    CHECK_RUN_ID,
    RELEASE_VERSION,
    REPORT_WEBHOOK,
    SEVERITY = "failure",
    TITLE,
  } = process.env;
  if (!TITLE) {
    core.setFailed("TITLE is not set");
    return;
  }
  if (!REPORT_WEBHOOK) {
    core.setFailed("REPORT_WEBHOOK is not set");
    return;
  }
  if (!CHECK_RUN_ID) {
    core.setFailed("CHECK_RUN_ID is not set");
    return;
  }

  const severity = SEVERITY.toLowerCase();
  const severityConfig = {
    failure: { prefix: "🔥(failure)", jobLinkText: "Failed Job" },
    warning: { prefix: "⚠️", jobLinkText: "Metachaos Job" },
  }[severity];
  if (!severityConfig) {
    core.setFailed(`Unsupported notification severity: ${SEVERITY}`);
    return;
  }

  const repositoryUrl = `${context.serverUrl}/${context.repo.owner}/${context.repo.repo}`;
  const runUrl = `${repositoryUrl}/actions/runs/${context.runId}`;
  const jobUrl = `${runUrl}/job/${CHECK_RUN_ID}`;
  const attemptUrl = `${runUrl}/attempts/${context.runAttempt}`;

  const reportData = {
    msg_type: "post",
    content: {
      post: {
        en_us: {
          title: `${severityConfig.prefix} ${TITLE}`,
          content: [
            ...(RELEASE_VERSION
              ? [[{ tag: "text", text: `Release: ${RELEASE_VERSION}` }]]
              : []),
            [
              {
                tag: "text",
                text: `Run attempt: ${context.runAttempt}`,
              },
            ],
            [
              {
                tag: "a",
                text: severityConfig.jobLinkText,
                href: jobUrl,
              },
              {
                tag: "text",
                text: " | ",
              },
              {
                tag: "a",
                text: "Workflow Attempt",
                href: attemptUrl,
              },
            ],
          ],
        },
      },
    },
  };

  let response;
  try {
    response = await fetch(REPORT_WEBHOOK, {
      method: "POST",
      headers: {
        "Content-Type": "application/json",
      },
      body: JSON.stringify(reportData),
      signal: AbortSignal.timeout(10_000),
    });
  } catch (error) {
    const errorName = error instanceof Error ? error.name : "unknown error";
    core.setFailed(`Failed to send job notification: ${errorName}`);
    return;
  }

  if (!response.ok) {
    core.setFailed(`Job notification webhook returned HTTP ${response.status}`);
    return;
  }

  let result;
  try {
    result = await response.json();
  } catch {
    core.setFailed("Job notification webhook returned invalid JSON");
    return;
  }

  const resultCode = result?.code ?? result?.StatusCode;
  if (resultCode !== undefined && Number(resultCode) !== 0) {
    core.setFailed(
      `Job notification webhook rejected the request: code ${resultCode}`,
    );
  }
};
