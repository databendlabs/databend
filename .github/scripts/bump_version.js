module.exports = async ({ github, context, core }) => {
  const knownEvents = ["schedule", "workflow_dispatch"];
  if (!knownEvents.includes(context.eventName)) {
    core.setFailed(`Triggerd by unknown event: ${context.eventName}`);
    return;
  }

  const { TYPE, TAG } = process.env;

  const RE_TAG_STABLE = /^v(\d+)\.(\d+)\.(\d+)$/;
  const RE_TAG_NIGHTLY = /^v(\d+)\.(\d+)\.(\d+)-nightly$/;
  const RE_TAG_PATCH = /^v(\d+)\.(\d+)\.(\d+)-p(\d+)$/;

  switch (TYPE) {
    case "":
    case "nightly": {
      core.setOutput("sha", context.sha);
      core.info(`Nightly release triggered by (${context.sha})`);

      let previous = null;
      const releases = await github.rest.repos.listReleases({
        owner: context.repo.owner,
        repo: context.repo.repo,
      });
      for (const release of releases.data) {
        const ret = RE_TAG_NIGHTLY.exec(release.tag_name);
        if (ret) {
          previous = release.tag_name;
          break;
        }
      }
      core.setOutput("previous", previous);
      core.info(`Nightly release with previous release: ${previous}`);

      if (TAG) {
        core.setOutput("tag", TAG);
        core.info(`Release create manually with tag ${TAG}`);
        return;
      }
      const result = RE_TAG_NIGHTLY.exec(previous);
      if (!result) {
        core.setFailed(`The previous tag ${previous} is invalid.`);
        return;
      }
      const major = result[1];
      const minor = result[2];
      const patch = (parseInt(result[3]) + 1).toString();
      const nextTag = `v${major}.${minor}.${patch}-nightly`;
      core.setOutput("tag", nextTag);
      core.info(`Release create new nightly ${nextTag}`);
      return;
    }

    case "stable": {
      core.setOutput("sha", context.sha);
      if (!TAG) {
        core.setFailed("Stable release must be triggered with a nightly tag");
        return;
      }
      core.info(`Stable release triggered by ${TAG} (${context.sha})`);
      const result = RE_TAG_NIGHTLY.exec(TAG);
      if (!result) {
        core.setFailed(`The tag ${TAG} is invalid, ignoring`);
        return;
      }
      const major = result[1];
      const minor = result[2];
      const patch = result[3];
      const nextTag = `v${major}.${minor}.${patch}`;
      core.setOutput("tag", nextTag);
      core.info(`Stable release ${nextTag} from ${TAG}`);

      let previous = null;
      let page = 1;
      while (true) {
        const releases = await github.rest.repos.listReleases({
          owner: context.repo.owner,
          repo: context.repo.repo,
          page,
        });
        if (releases.data.length === 0) {
          break;
        }
        page++;
        for (const release of releases.data) {
          const ret = RE_TAG_STABLE.exec(release.tag_name);
          if (ret) {
            previous = release.tag_name;
            break;
          }
        }
      }
      if (!previous) {
        core.setFailed(`No previous stable release found, ignoring`);
        return;
      }
      core.setOutput("previous", previous);
      core.info(`Stable release with previous release: ${previous}`);
    }

    case "patch": {
      if (!TAG) {
        core.setFailed("Patch release must be triggered with a stable tag");
        return;
      }
      core.info(`Patch release triggered by ${TAG}`);
      const result = RE_TAG_STABLE.exec(TAG);
      if (!result) {
        core.setFailed(`The tag ${TAG} is invalid, ignoring`);
        return;
      }

      const branch = await github.rest.repos.getBranch({
        owner: context.repo.owner,
        repo: context.repo.repo,
        branch: `backport/${TAG}`,
      });
      core.setOutput("sha", branch.data.commit.sha);
      core.info(
        `Patch release triggered by ${TAG} (${branch.data.commit.sha})`
      );

      let pv = 1;
      let previous = null;
      let page = 1;
      while (true) {
        const releases = await github.rest.repos.listReleases({
          owner: context.repo.owner,
          repo: context.repo.repo,
          page,
        });
        if (releases.data.length === 0) {
          break;
        }
        page++;
        for (const release of releases.data) {
          if (!release.tag_name.startsWith(TAG)) {
            continue;
          }
          if (release.tag_name === TAG) {
            previous = release.tag_name;
            break;
          }
          const ret = RE_TAG_PATCH.exec(release.tag_name);
          if (!ret) {
            core.warning(`Ignore previous release ${release.tag_name}`);
            continue;
          }
          const lastPV = parseInt(ret[4]);
          if (lastPV) {
            pv = lastPV + 1;
            previous = release.tag_name;
            break;
          }
        }
      }
      if (!previous) {
        core.setFailed(`No previous stable release found, ignoring`);
        return;
      }
      core.setOutput("previous", previous);
      core.info(`Patch release with previous release: ${previous}`);

      const major = result[1];
      const minor = result[2];
      const patch = result[3];
      const nextTag = `v${major}.${minor}.${patch}-p${pv}`;
      core.setOutput("tag", nextTag);
      core.info(`Patch release ${nextTag} from ${TAG}`);
      return;
    }

    default: {
      core.setFailed(`Unknown release type: ${TYPE}`);
      return;
    }
  }
};
