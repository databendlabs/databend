module.exports = async ({ github, context, core }) => {
  const { STABLE, TAG } = process.env;
  if (context.ref.startsWith("refs/tags/")) {
    let tag = context.ref.replace("refs/tags/", "");
    core.setOutput("tag", tag);
    core.info(`Tag event triggered by ${tag}.`);
    return;
  }
  if (STABLE == "true") {
    if (TAG) {
      // trigger stable release by workflow_dispatch with a tag
      let result = /v(\d+)\.(\d+)\.(\d+)-nightly/g.exec(TAG);
      if (result === null) {
        core.setFailed(`The tag ${TAG} to stablize is invalid, ignoring`);
        return;
      }
      let major = result[1];
      let minor = result[2];
      let patch = result[3];
      let stable_tag = `v${major}.${minor}.${patch}`;
      core.setOutput("tag", stable_tag);
      let ref = await github.rest.git.getRef({
        owner: context.repo.owner,
        repo: context.repo.repo,
        ref: `tags/${TAG}`,
      });
      core.setOutput("sha", ref.data.object.sha);
      core.info(
        `Stable release ${stable_tag} from ${TAG} (${ref.data.object.sha})`
      );
    } else {
      core.setFailed("Stable release must be triggered with a nightly tag");
    }
  } else {
    if (TAG) {
      core.setOutput("tag", TAG);
      core.info(`Release create manually with tag ${TAG}`);
    } else {
      let releases = await github.rest.repos.listReleases({
        owner: context.repo.owner,
        repo: context.repo.repo,
        per_page: 1,
      });
      let tag = releases.data[0].tag_name;
      let result = /v(\d+)\.(\d+)\.(\d+)/g.exec(tag);
      if (result === null) {
        core.setFailed(`The previous tag ${tag} is invalid, ignoring`);
        return;
      }
      let major = result[1];
      let minor = result[2];
      let patch = (parseInt(result[3]) + 1).toString();
      let next_tag = `v${major}.${minor}.${patch}-nightly`;
      core.setOutput("tag", next_tag);
      core.setOutput("sha", context.sha);
      core.info(`Nightly release ${next_tag} from ${tag} (${context.sha})`);
    }
  }
};
