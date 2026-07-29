module.exports = async ({ github, context, core }) => {
  const body = context.payload.pull_request.body || "";

  const problems = [];
  const sectionMatch = body.match(
    /^##\s*AI assistance\s*\n([\s\S]*?)(?=\n##\s|$(?![\s\S]))/im,
  );

  if (!sectionMatch) {
    problems.push("the `## AI assistance` section is missing");
  } else {
    const section = sectionMatch[1];

    const usageLine = section.match(/^-[ \t]*AI usage:[ \t]*(.*)$/im);
    const usage = usageLine?.[1].trim();
    if (!usage || usage.startsWith("<!--")) {
      problems.push(
        "`AI usage:` is not filled in (write `None` if no AI was used)",
      );
    }

    const humanLine = section.match(/^-[ \t]*Responsible human:[ \t]*(.*)$/im);
    const human = humanLine?.[1].trim();
    const usernameMatch = human?.match(/^@([A-Za-z0-9-]+)$/);
    const username = usernameMatch?.[1];
    const usernameIsValid =
      username &&
      username.length <= 39 &&
      !username.startsWith("-") &&
      !username.endsWith("-") &&
      !username.includes("--") &&
      username.toLowerCase() !== "your-github-id";

    if (!usernameIsValid) {
      problems.push(
        "`Responsible human:` must name a real GitHub user, e.g. `@octocat`",
      );
    } else {
      try {
        const { data: account } = await github.rest.users.getByUsername({
          username,
        });
        if (account.type !== "User") {
          problems.push("`Responsible human:` must refer to a human account");
        }
      } catch (error) {
        if (error.status === 404) {
          problems.push(`Responsible human \`@${username}\` does not exist`);
        } else {
          throw error;
        }
      }
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
