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

  let releaseIcon = "🎉";
  let releaseStatus = "success";
  const statuses = JOBS_STATUS.split(",");
  if (statuses.includes("failure")) {
    releaseStatus = "failure";
    icon = "🔥";
  } else if (statuses.includes("skipped")) {
    releaseStatus = "skipped";
    icon = "🚫";
  } else if (statuses.includes("cancelled")) {
    releaseStatus = "cancelled";
    icon = "🚫";
  }

  const reportData = {
    msg_type: "post",
    content: {
      post: {
        en_us: {
          title: `${releaseIcon} [Release] ${VERSION} (${releaseStatus})`,
          content: [
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
  if (releaseStatus !== "success") {
    core.setFailed("Release failed");
    return;
  }
};
