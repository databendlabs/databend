module.exports = async ({ github, context, core }) => {
  const knownEvents = ["schedule", "workflow_dispatch"];
  if (!knownEvents.includes(context.eventName)) {
    core.setFailed(`Triggerd by unknown event: ${context.eventName}`);
    return;
  }

  const { TYPE, TAG, SHA } = process.env;

  if (!TYPE) {
    core.setOutput("type", "nightly");
  } else {
    core.setOutput("type", TYPE);
  }

  const RE_TAG_STABLE = /^v(\d+)\.(\d+)\.(\d+)$/;
  const RE_TAG_NIGHTLY = /^v(\d+)\.(\d+)\.(\d+)-nightly$/;
  const RE_TAG_PATCH = /^v(\d+)\.(\d+)\.(\d+)-p(\d+)$/;

  async function getPreviousNightlyRelease(github, context) {
    const releases = await github.rest.repos.listReleases({
      owner: context.repo.owner,
      repo: context.repo.repo,
    });
    for (const release of releases.data) {
      const ret = RE_TAG_NIGHTLY.exec(release.tag_name);
      if (ret) {
        return release.tag_name;
      }
    }
  }

  function getNextNightlyRelease(previous) {
    const nightly = RE_TAG_NIGHTLY.exec(previous);
    if (nightly) {
      const major = nightly[1];
      const minor = nightly[2];
      const patch = parseInt(nightly[3]) + 1;
      return `v${major}.${minor}.${patch}-nightly`;
    }
  }

  async function getPreviousStableRelease(github, context) {
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
          return release.tag_name;
        }
      }
    }
  }

  function getNextStableRelease() {
    const nightly = RE_TAG_NIGHTLY.exec(TAG);
    if (nightly) {
      const major = nightly[1];
      const minor = nightly[2];
      const patch = nightly[3];
      return `v${major}.${minor}.${patch}`;
    }
  }

  async function getPreviousPatchRelease(github, context) {
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
          // no previous patch release, use the previous stable release
          return release.tag_name;
        }
        const ret = RE_TAG_PATCH.exec(release.tag_name);
        if (!ret) {
          core.warning(`Ignore invalid patch release ${release.tag_name}`);
          continue;
        }
        return release.tag_name;
      }
    }
  }

  function getNextPatchRelease(previous) {
    const stable = RE_TAG_STABLE.exec(previous);
    if (stable) {
      const major = stable[1];
      const minor = stable[2];
      const patch = stable[3];
      return `v${major}.${minor}.${patch}-p1`;
    }
    const version = RE_TAG_PATCH.exec(previous);
    if (version) {
      const major = version[1];
      const minor = version[2];
      const patch = version[3];
      const pv = parseInt(version[4]) + 1;
      return `v${major}.${minor}.${patch}-p${pv}`;
    }
  }

  // Determine the SHA to use: custom SHA if provided, otherwise context.sha
  const targetSha = SHA || context.sha;

  switch (TYPE) {
    case "":
    case "nightly": {
      core.setOutput("sha", targetSha);
      core.info(`Nightly release triggered by (${targetSha})${SHA ? " [custom SHA]" : ""}`);
      if (SHA) {
        core.info(`Using custom SHA: ${SHA}`);
      }

      const previous = await getPreviousNightlyRelease(github, context);
      if (!previous) {
        core.setFailed(`No previous nightly release found, ignoring`);
        return;
      }
      core.setOutput("previous", previous);
      core.info(`Nightly release with previous release: ${previous}`);

      if (TAG) {
        core.setOutput("tag", TAG);
        core.info(`Release create manually with tag ${TAG}`);
        return;
      }
      const nextTag = getNextNightlyRelease(previous);
      if (!nextTag) {
        core.setFailed(`No next nightly release from ${previous}`);
        return;
      }
      core.setOutput("tag", nextTag);
      core.info(`Release create new nightly ${nextTag}`);
      return;
    }

    case "stable": {
      core.setOutput("sha", targetSha);
      if (!TAG) {
        core.setFailed("Stable release must be triggered with a nightly tag");
        return;
      }
      core.info(`Stable release triggered by ${TAG} (${targetSha})${SHA ? " [custom SHA]" : ""}`);
      if (SHA) {
        core.info(`Using custom SHA: ${SHA}`);
      }
      const nextTag = getNextStableRelease();
      if (!nextTag) {
        core.setFailed(`No stable release from ${TAG}`);
        return;
      }
      core.setOutput("tag", nextTag);
      core.info(`Stable release ${nextTag} from ${TAG}`);

      const previous = await getPreviousStableRelease(github, context);
      if (!previous) {
        core.setFailed(`No previous stable release found, ignoring`);
        return;
      }
      core.setOutput("previous", previous);
      core.info(`Stable release with previous release: ${previous}`);
      return;
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

      let patchSha;
      if (SHA) {
        // Use custom SHA if provided
        patchSha = SHA;
        core.info(`Using custom SHA: ${SHA}`);
      } else {
        // Otherwise use the backport branch SHA
        const branch = await github.rest.repos.getBranch({
          owner: context.repo.owner,
          repo: context.repo.repo,
          branch: `backport/${TAG}`,
        });
        patchSha = branch.data.commit.sha;
      }
      core.setOutput("sha", patchSha);
      core.info(
        `Patch release triggered by ${TAG} (${patchSha})${SHA ? " [custom SHA]" : ""}`
      );

      const previous = await getPreviousPatchRelease(github, context);
      if (!previous) {
        core.setFailed(`No previous patch release found, ignoring`);
        return;
      }
      core.setOutput("previous", previous);
      core.info(`Patch release with previous release: ${previous}`);

      const nextTag = getNextPatchRelease(previous);
      if (!nextTag) {
        core.setFailed(`No next patch release from ${previous}`);
        return;
      }
      core.setOutput("tag", nextTag);
      core.info(`Patch release ${nextTag} from ${previous}`);
      return;
    }

    default: {
      core.setFailed(`Unknown release type: ${TYPE}`);
      return;
    }
  }
};
