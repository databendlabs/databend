const assert = require("node:assert/strict");
const test = require("node:test");

const checkAiAssistance = require("./check_pr_ai_assistance.js");

const ENFORCEMENT_START = "2026-07-29T14:33:06Z";
const EXACT_ATTESTATION =
  "- [x] The responsible human has read every line of this diff and can explain each change";

const accounts = {
  bohutang: { type: "User" },
  octocat: { type: "User" },
  dependabot: { type: "Bot" },
};

const github = {
  rest: {
    users: {
      getByUsername: async ({ username }) => {
        const account = accounts[username.toLowerCase()];
        if (!account) {
          const error = new Error("Not Found");
          error.status = 404;
          throw error;
        }
        return { data: account };
      },
    },
  },
};

const declaration = ({
  human = "@bohutang",
  attestation = EXACT_ATTESTATION,
  usage = "None",
} = {}) => `## AI assistance

- AI usage: ${usage}
- Responsible human: ${human}
${attestation}`;

const runCheck = async ({
  body,
  createdAt = ENFORCEMENT_START,
  author = { login: "bohutang", type: "User" },
}) => {
  const outputs = {};
  let failure;
  const core = {
    setOutput: (key, value) => {
      outputs[key] = value;
    },
    setFailed: (message) => {
      failure = message;
    },
  };

  await checkAiAssistance({
    github,
    context: {
      payload: {
        pull_request: {
          body,
          created_at: createdAt,
          user: author,
        },
      },
    },
    core,
  });

  return { outputs, failure };
};

const assertResult = async (input, expected) => {
  const result = await runCheck(input);
  assert.equal(result.outputs.ai, expected, result.failure);
  if (expected === "valid") {
    assert.equal(result.failure, undefined);
  } else {
    assert.ok(result.failure);
  }
  return result;
};

test("accepts a visible, complete declaration", async () => {
  await assertResult({ body: declaration() }, "valid");
});

test("does not let comments and fences mutate each other's state", async () => {
  const hiddenByFence = `<!--
\`\`\`
-->
\`\`\`
<!-- x -->
${declaration()}
\`\`\``;
  await assertResult({ body: hiddenByFence }, "invalid");

  const visibleAfterFence = `\`\`\`html
<!--
\`\`\`
${declaration()}`;
  await assertResult({ body: visibleAfterFence }, "valid");

  const visibleAfterComment = `<!--
\`\`\`
-->
${declaration()}`;
  await assertResult({ body: visibleAfterComment }, "valid");
});

test("rejects declarations hidden by Markdown fences", async () => {
  await assertResult(
    { body: `\`\`\`md\n${declaration()}\n\`\`\`` },
    "invalid",
  );
  await assertResult(
    { body: `~~~md\n${declaration()}\n~~~` },
    "invalid",
  );
  await assertResult(
    { body: `\`\`\`\`md\n\`\`\`\n${declaration()}\n\`\`\`\`` },
    "invalid",
  );
});

test("rejects declarations hidden by raw HTML code blocks", async () => {
  await assertResult(
    { body: `<pre>\n${declaration()}\n</pre>` },
    "invalid",
  );
  await assertResult(
    { body: `<pre\nclass="example">\n${declaration()}\n</pre>` },
    "invalid",
  );
  await assertResult(
    { body: `  <code\nclass="example">\n${declaration()}\n</code>` },
    "invalid",
  );
});

test("does not confuse inline or indented code with raw HTML blocks", async () => {
  await assertResult(
    { body: `Summary mentions \`<pre>\` and \`<code>\`.\n${declaration()}` },
    "valid",
  );
  await assertResult(
    { body: `    <pre>\n${declaration()}` },
    "valid",
  );
});

test("requires the exact checked attestation", async () => {
  await assertResult(
    {
      body: declaration({
        attestation: `${EXACT_ATTESTATION} — not this diff`,
      }),
    },
    "invalid",
  );
  await assertResult(
    {
      body: declaration({
        attestation: "- [x] The responsible human has read every line",
      }),
    },
    "invalid",
  );
  await assertResult(
    {
      body: declaration({
        attestation: EXACT_ATTESTATION.replace("[x]", "[ ]"),
      }),
    },
    "invalid",
  );
});

test("binds human-authored PRs to their author", async () => {
  await assertResult(
    { body: declaration({ human: "@octocat" }) },
    "invalid",
  );
  await assertResult(
    { body: declaration({ human: "@BohuTang" }) },
    "valid",
  );
});

test("allows bot-authored PRs to name a human but not another bot", async () => {
  const author = { login: "agent[bot]", type: "Bot" };
  await assertResult({ body: declaration(), author }, "valid");
  await assertResult(
    { body: declaration({ human: "@dependabot" }), author },
    "invalid",
  );
});

test("does not retroactively block PRs created before enforcement", async () => {
  const legacy = await assertResult(
    {
      body: "## Summary\nLegacy PR without a declaration",
      createdAt: "2026-07-29T14:33:05Z",
    },
    "valid",
  );
  assert.equal(legacy.outputs.migration, "legacy-pr");

  await assertResult(
    {
      body: "## Summary\nNew PR without a declaration",
      createdAt: ENFORCEMENT_START,
    },
    "invalid",
  );
});
