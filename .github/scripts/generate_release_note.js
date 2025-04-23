const fs = require("fs");

function escapeHtml(text) {
  const map = {
    "&": "&amp;",
    "<": "&lt;",
    ">": "&gt;",
    '"': "&quot;",
    "'": "&#039;",
  };
  return text.replace(/[&<>"']/g, (m) => map[m]);
}

module.exports = async ({ github, context, core }) => {
  const { VERSION, DATE } = process.env;
  fs.mkdirSync("docs/release-stable", { recursive: true });
  const df = `docs/release-stable/${DATE}_${VERSION}.md`;

  const releases = await github.rest.repos.listReleases({
    owner: "databendlabs",
    repo: "databend",
  });
  const release = releases.data.find((r) => r.name === VERSION);
  if (!release) {
    core.setFailed(`Release ${VERSION} not found`);
    return;
  }

  let body = release.body;
  body = body.split("\n").slice(1).join("\n");
  body = "---\n" + body;
  body = body.replace(/^--$/gm, "---");
  body = body.replace(/^asset:.*$/gm, "");
  body = body.replace(
    /https:\/\/github\.com\/databendlabs\/databend\/pull\/([0-9]+)/g,
    "[#$1](https://github.com/databendlabs/databend/pull/$1)"
  );
  body = escapeHtml(body);

  fs.writeFileSync(df, body);
};
