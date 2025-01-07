module.exports = async ({ github, context, core }) => {
  const title = context.payload.pull_request.title;
  const regex = /^(rfc|feat|fix|refactor|ci|docs|chore)(\([a-z0-9-]+\))?:/;
  const m = title.match(regex);
  if (!m) {
    core.setFailed("PR title is not semantic");
    core.setOutput("title", "not-semantic");
    return;
  }
  const prType = m[1];
  // const prScope = m[2];
  // const prSummary = title.substring(m[0].length);
  let labels = [];
  switch (prType) {
    case "rfc":
      labels.push("pr-rfc");
      break;
    case "feat":
      labels.push("pr-feature");
      break;
    case "fix":
      labels.push("pr-bugfix");
      break;
    case "refactor":
      labels.push("pr-refactor");
      break;
    case "ci":
      labels.push("pr-build");
      break;
    case "docs":
      labels.push("pr-doc");
      break;
    case "chore":
      labels.push("pr-chore");
      break;
  }
  if (context.payload.pull_request.base.ref.startsWith("backport/")) {
    labels.push("pr-backport");
  }
  await github.rest.issues.addLabels({
    owner: context.repo.owner,
    repo: context.repo.repo,
    issue_number: context.issue.number,
    labels: labels,
  });
  core.setOutput("title", "semantic");
};
