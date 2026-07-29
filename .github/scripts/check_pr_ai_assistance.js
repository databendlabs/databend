const AI_DECLARATION_ENFORCEMENT_START = Date.parse("2026-07-29T14:33:06Z");

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

const removeFencedCodeBlocks = (input) => {
  const lines = input.split("\n");
  const visibleLines = [];
  let fence = null;

  for (const line of lines) {
    const content = line.replace(/^ {0,3}/, "");

    if (!fence) {
      const opener = content.match(/^(`{3,}|~{3,})/);
      if (opener) {
        fence = { marker: opener[1][0], length: opener[1].length };
        visibleLines.push("");
      } else {
        visibleLines.push(line);
      }
      continue;
    }

    let markerLength = 0;
    while (content[markerLength] === fence.marker) {
      markerLength += 1;
    }
    if (
      markerLength >= fence.length &&
      content.slice(markerLength).trim() === ""
    ) {
      fence = null;
    }
    visibleLines.push("");
  }

  return visibleLines.join("\n");
};

const removeRawHtmlCodeBlocks = (input) => {
  let visible = "";
  let cursor = 0;
  const openingTag = /<(pre|code)(?:\s[^>]*)?>/gi;

  while (cursor < input.length) {
    openingTag.lastIndex = cursor;
    const opening = openingTag.exec(input);
    if (!opening) {
      visible += input.slice(cursor);
      break;
    }

    visible += input.slice(cursor, opening.index);
    const tag = opening[1];
    const closingTag = new RegExp(`</${tag}\\s*>`, "gi");
    closingTag.lastIndex = openingTag.lastIndex;
    const closing = closingTag.exec(input);
    const hiddenEnd = closing ? closingTag.lastIndex : input.length;

    // Keep newlines so headings outside the raw code block remain on their
    // original lines, while everything rendered as literal code is ignored.
    visible += input.slice(opening.index, hiddenEnd).replace(/[^\n]/g, "");
    cursor = hiddenEnd;
  }

  return visible;
};

module.exports = async ({ github, context, core }) => {
  const pullRequest = context.payload.pull_request;
  const createdAt = Date.parse(pullRequest.created_at);
  if (
    Number.isFinite(createdAt) &&
    createdAt < AI_DECLARATION_ENFORCEMENT_START
  ) {
    // Do not retroactively block PRs created before the policy was merged.
    core.setOutput("ai", "valid");
    core.setOutput("migration", "legacy-pr");
    return;
  }

  const body = pullRequest.body || "";
  const withoutFences = removeFencedCodeBlocks(body);
  const withoutComments = removeHtmlComments(withoutFences);
  const visibleBody = removeRawHtmlCodeBlocks(withoutComments);

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
      const prAuthor = context.payload.pull_request.user;
      const authorIsBot =
        prAuthor.type === "Bot" || prAuthor.login.toLowerCase().endsWith("[bot]");

      if (
        !authorIsBot &&
        username.toLowerCase() !== prAuthor.login.toLowerCase()
      ) {
        problems.push(
          `Responsible human must match the PR author \`@${prAuthor.login}\``,
        );
      }

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
      /^-[ \t]*\[x\][ \t]+The responsible human has read every line of this diff and can explain each change[ \t]*$/im,
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
