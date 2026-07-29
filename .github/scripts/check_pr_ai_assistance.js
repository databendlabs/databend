module.exports = async ({ context, core }) => {
  const body = context.payload.pull_request.body || "";
  // Strip HTML comments so template placeholders don't count as content.
  const stripped = body.replace(/<!--[\s\S]*?-->/g, "");

  const problems = [];
  const sectionMatch = stripped.match(
    /^##\s*AI assistance\s*\n([\s\S]*?)(?=\n##\s|$(?![\s\S]))/im,
  );

  if (!sectionMatch) {
    problems.push("the `## AI assistance` section is missing");
  } else {
    const section = sectionMatch[1];

    const usage = section.match(/^-[ \t]*AI usage:[ \t]*(\S.*)$/im);
    if (!usage) {
      problems.push(
        "`AI usage:` is not filled in (write `None` if no AI was used)",
      );
    }

    const human = section.match(/^-[ \t]*Responsible human:.*@[A-Za-z0-9][A-Za-z0-9-]*/im);
    if (!human) {
      problems.push(
        "`Responsible human:` must name a GitHub user, e.g. `@your-github-id`",
      );
    }

    const readBox = section.match(
      /^-\s*\[x\]\s+The responsible human has read every line/im,
    );
    if (!readBox) {
      problems.push(
        'the checkbox "The responsible human has read every line of this diff" is not checked',
      );
    }
  }

  if (problems.length > 0) {
    core.setOutput("ai", "invalid");
    core.setOutput("problems", problems.map((p) => `- ${p}`).join("\n"));
    core.setFailed(`AI assistance section incomplete: ${problems.join("; ")}`);
  } else {
    core.setOutput("ai", "valid");
  }
};
