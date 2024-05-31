module.exports = async ({ context, core }) => {
  const { JOBS_STATUS, REPORT_WEBHOOK, VERSION } = process.env;
  if (!REPORT_WEBHOOK) {
    core.setFailed("REPORT_WEBHOOK is not set");
    return;
  }
  if (!VERSION) {
    core.setFailed("VERSION is not set");
    return;
  }
  const reportData = {
    msg_type: "post",
    content: {
      post: {
        en_us: {
          title: `[Release] ${VERSION}`,
          content: [
            [
              {
                tag: "text",
                text: `Build result: ${JOBS_STATUS}`,
              },
            ],
            [
              {
                tag: "a",
                text: "Workflow Details",
                href: `${context.serverUrl}/${context.repo.owner}/${context.repo.repo}/actions/runs/${context.runId}`,
              },
              {
                tag: "text",
                text: " | ",
              },
              {
                tag: "a",
                text: "Release Notes",
                href: `https://github.com/datafuselabs/databend/releases/tag/${VERSION}`,
              },
            ],
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
  if (JOBS_STATUS !== "success") {
    core.setFailed("Release failed");
    return;
  }
};
