const fs = require("fs");

module.exports = async ({ github, context, core }) => {
  const { VERSION, DATE } = process.env;
  fs.mkdirSync("docs/release-stable", { recursive: true });
  const df = `docs/release-stable/${DATE}_${VERSION}.md`;

  const releases = await github.rest.repos.listReleases({
    owner: "databendlabs",
    repo: "databend",
  });
  const release = releases.data.find((r) => r.name === VERSION);

  let body = release.body;

  body = "---\n" + body;
  body = body.replace(/^--$/gm, "---");
  body = body.replace(/^asset:.*$/gm, "");
  body = body.replace(
    /https:\/\/github\.com\/databendlabs\/databend\/pull\/([0-9]+)/g,
    "[#$1](https://github.com/databendlabs/databend/pull/$1)"
  );

  fs.writeFileSync(df, body);
};
