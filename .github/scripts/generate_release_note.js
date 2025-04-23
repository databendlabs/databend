const fs = require("fs");

module.exports = async ({ github, context, core }) => {
  const { VERSION, DATE } = context.payload.inputs;
  fs.mkdirSync("docs/release-stable", { recursive: true });
  const df = `docs/release-stable/${DATE}_${VERSION}.md`;

  const release = await github.rest.repos.getRelease({
    owner: "databendlabs",
    repo: "databend",
    release_id: VERSION,
  });

  let body = release.data.body;

  body = "---\n" + body;
  body = body.replace(/^--$/gm, "---");
  body = body.replace(/^asset:.*$/gm, "");
  body = body.replace(
    /https:\/\/github\.com\/databendlabs\/databend\/pull\/([0-9]+)/g,
    "[#$1](https://github.com/databendlabs/databend/pull/$1)"
  );

  fs.writeFileSync(df, body);
};
