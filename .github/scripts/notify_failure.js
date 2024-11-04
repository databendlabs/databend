module.exports = async ({ context, core }) => {
  const { TITLE, REPORT_WEBHOOK } = process.env;
  if (!TITLE) {
    core.setFailed("TITLE is not set");
    return;
  }
  if (!REPORT_WEBHOOK) {
    core.setFailed("REPORT_WEBHOOK is not set");
    return;
  }

  const reportData = {
    msg_type: "post",
    content: {
      post: {
        en_us: {
          title: `🔥(failure) ${TITLE}`,
          content: [
            [
              {
                tag: "a",
                text: "Workflow Details",
                href: `${context.serverUrl}/${context.repo.owner}/${context.repo.repo}/actions/runs/${context.runId}`,
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
};
