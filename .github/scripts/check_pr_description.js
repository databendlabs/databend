module.exports = async ({ context, core }) => {
  const body = context.payload.pull_request.body;
  let section = "summary";
  let testsChecked = false;
  let changesChecked = false;
  for (const line of body.split("\n")) {
    if (line.includes("## Tests")) {
      section = "tests";
      core.info("checking section: tests");
      continue;
    } else if (line.includes("## Type of change")) {
      section = "changes";
      core.info("checking section: changes");
      continue;
    }
    if (section === "tests") {
      if (line.startsWith("- [x] ")) {
        testsChecked = true;
        core.info(`tests checked: ${line}`);
        core.setOutput("tests", "checked");
        continue;
      }
    } else if (section === "changes") {
      if (line.startsWith("- [x] ")) {
        changesChecked = true;
        core.info(`type of change checked: ${line}`);
        core.setOutput("changes", "checked");
        continue;
      }
    }
  }
  if (!testsChecked) {
    core.setOutput("tests", "not-checked");
    core.setFailed("Tests are not checked");
  }
  if (!changesChecked) {
    core.setOutput("changes", "not-checked");
    core.setFailed("Type of Changes are not checked");
  }
};
