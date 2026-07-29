const removeHtmlComments = (input) => {
  let visible = "";
  let cursor = 0;

  while (cursor < input.length) {
    const commentStart = input.indexOf("<!--", cursor);
    if (commentStart === -1) {
      visible += input.slice(cursor);
      break;
    }

    visible += input.slice(cursor, commentStart);
    const commentEnd = input.indexOf("-->", commentStart + 4);
    if (commentEnd === -1) {
      // GitHub hides an unterminated comment through the end of the body.
      break;
    }
    cursor = commentEnd + 3;
  }

  return visible;
};

module.exports = async ({ github, context, core }) => {
  const body = context.payload.pull_request.body || "";
  const visibleBody = removeHtmlComments(body);

  const problems = [];
  const sectionMatch = visibleBody.match(
    /^##\s*AI assistance\s*\n([\s\S]*?)(?=\n##\s|$(?![\s\S]))/im,
  );

  if (!sectionMatch) {
    problems.push("the `## AI assistance` section is missing");
  } else {
    const section = sectionMatch[1];

    const usageLine = section.match(/^-[ \t]*AI usage:[ \t]*(.*)$/im);
    const usage = usageLine?.[1].trim();
    if (!usage) {
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
