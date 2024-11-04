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
  let label = "";
  switch (prType) {
    case "rfc":
      label = "pr-rfc";
      break;
    case "feat":
      label = "pr-feature";
      break;
    case "fix":
      label = "pr-bugfix";
      break;
    case "refactor":
      label = "pr-refactor";
      break;
    case "ci":
      label = "pr-build";
      break;
    case "docs":
      label = "pr-doc";
      break;
    case "chore":
      label = "pr-chore";
      break;
  }
  await github.rest.issues.addLabels({
    owner: context.repo.owner,
    repo: context.repo.repo,
    issue_number: context.issue.number,
    labels: [label],
  });
  core.setOutput("title", "semantic");
};
