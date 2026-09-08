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
  const attemptUrl = `${runUrl}/attempts/${context.runAttempt}`;
  const detailLinks = CHECK_RUN_ID
    ? [
        {
          tag: "a",
          text: severityConfig.jobLinkText,
          href: `${runUrl}/job/${CHECK_RUN_ID}`,
        },
        { tag: "text", text: " | " },
        { tag: "a", text: "Workflow Attempt", href: attemptUrl },
      ]
    : [{ tag: "a", text: "Workflow Attempt", href: attemptUrl }];

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
            detailLinks,
          ],
        },
      },
    },
  };

  await fetch(REPORT_WEBHOOK, {
    method: "POST",
    headers: {
      "Content-Type": "application/json",
    },
    body: JSON.stringify(reportData),
  });
};
