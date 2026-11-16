// Copyright 2021 Datafuse Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Git metadata for build scripts, read through the `git` CLI.
//!
//! We used to link `gix` into the build scripts, which costs ~85 CPU-seconds (125 crates) at the
//! start of every clean build. Shelling out to `git` is what `vergen-gitcl` does as well.

use std::collections::BTreeMap;
use std::path::Path;
use std::process::Command;

use anyhow::Context;
use anyhow::Result;
use anyhow::bail;

/// Run `git <args>` in `dir` and return its trimmed stdout.
fn git(dir: &Path, args: &[&str]) -> Result<String> {
    let output = Command::new("git")
        .current_dir(dir)
        .args(args)
        .output()
        .with_context(|| format!("failed to run `git {}`", args.join(" ")))?;
    if !output.status.success() {
        bail!(
            "`git {}` failed: {}",
            args.join(" "),
            String::from_utf8_lossy(&output.stderr).trim()
        );
    }
    Ok(String::from_utf8(output.stdout)?.trim().to_string())
}

/// Top-level directory of the git work tree containing `dir`.
pub fn discover(dir: &Path) -> Result<std::path::PathBuf> {
    Ok(git(dir, &["rev-parse", "--show-toplevel"])?.into())
}

/// Name of the nearest tag (annotated or lightweight) reachable from HEAD.
pub fn get_latest_tag(repo: &Path) -> Result<String> {
    git(repo, &["describe", "--tags", "--abbrev=0", "HEAD"])
        .context("Did not find a valid tag for HEAD")
}

/// Comma separated, sorted, de-duplicated author names of all commits reachable from HEAD.
/// Authors are keyed by email; the name of the most recent commit for an email wins.
pub fn get_commit_authors(repo: &Path) -> Result<String> {
    let log = git(repo, &["log", "--format=%ae%x00%an", "HEAD"])?;

    let mut authors: BTreeMap<&str, &str> = BTreeMap::new();
    for line in log.lines() {
        if let Some((email, name)) = line.split_once('\0') {
            authors.entry(email).or_insert(name);
        }
    }

    let names = authors
        .into_values()
        .collect::<std::collections::BTreeSet<&str>>()
        .into_iter()
        .collect::<Vec<&str>>()
        .join(", ");
    Ok(names)
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::*;

    #[test]
    fn test_git_metadata() -> Result<()> {
        let repo = discover(Path::new(env!("CARGO_MANIFEST_DIR")))?;
        // A shallow clone may have no tag; only check that the command works when one exists.
        if let Ok(tag) = get_latest_tag(&repo) {
            assert!(!tag.is_empty());
        }
        let authors = get_commit_authors(&repo)?;
        assert!(!authors.is_empty());
        let names: Vec<&str> = authors.split(", ").collect();
        let mut sorted = names.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(names, sorted);
        Ok(())
    }
}
