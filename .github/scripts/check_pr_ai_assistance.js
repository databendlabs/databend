const AI_DECLARATION_ENFORCEMENT_START = Date.parse("2026-07-29T14:33:06Z");

const extractVisibleMarkdown = (input) => {
  const visibleLines = [];
  let fence = null;
  let inHtmlComment = false;
  let rawHtmlTag = null;

  for (const originalLine of input.split("\n")) {
    let line = originalLine;

    // Syntax inside a fenced block is literal and must not change other states.
    if (fence) {
      const content = line.replace(/^ {0,3}/, "");
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
      continue;
    }

    // GFM raw <pre>/<code> blocks render Markdown syntax as literal code.
    if (rawHtmlTag) {
      if (new RegExp(`</${rawHtmlTag}\\s*>`, "i").test(line)) {
        rawHtmlTag = null;
      }
      visibleLines.push("");
      continue;
    }

    // A fence-like line inside an HTML comment must not open a fence. Ignore
    // the entire line that closes a multiline comment rather than re-parsing
    // its suffix in the wrong block context.
    if (inHtmlComment) {
      if (line.includes("-->")) {
        inHtmlComment = false;
      }
      visibleLines.push("");
      continue;
    }

    const content = line.replace(/^ {0,3}/, "");
    const fenceOpener = content.match(/^(`{3,}|~{3,})/);
    if (fenceOpener) {
      fence = {
        marker: fenceOpener[1][0],
        length: fenceOpener[1].length,
      };
      visibleLines.push("");
      continue;
    }

    // GFM recognizes these raw blocks as soon as a line starts with <pre or
    // <code followed by whitespace, `>`, or end-of-line. The opening tag may
    // therefore continue on a later line.
    const rawHtmlOpener = content.match(/^<(pre|code)(?=[\s>]|$)/i);
    if (rawHtmlOpener) {
      const tag = rawHtmlOpener[1].toLowerCase();
      if (!new RegExp(`</${tag}\\s*>`, "i").test(content)) {
        rawHtmlTag = tag;
      }
      visibleLines.push("");
      continue;
    }

    // Remove inline comments while preserving visible text around them.
    let visible = "";
    let cursor = 0;
    while (cursor < line.length) {
      const commentStart = line.indexOf("<!--", cursor);
      if (commentStart === -1) {
        visible += line.slice(cursor);
        break;
      }

      visible += line.slice(cursor, commentStart);
      const commentEnd = line.indexOf("-->", commentStart + 4);
      if (commentEnd === -1) {
        inHtmlComment = true;
        break;
      }
      cursor = commentEnd + 3;
    }
    visibleLines.push(visible);
  }

  return visibleLines.join("\n");
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
  const visibleBody = extractVisibleMarkdown(body);

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
