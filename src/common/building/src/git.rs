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

use std::collections::BTreeMap;

use anyhow::Context;
use anyhow::Result;
use gix::commit::describe::SelectRef;
use gix::Repository;

// Get the latest tag:
// git describe --tags --abbrev=0
// v0.6.99-nightly
pub fn get_latest_tag(repo: &Repository) -> Result<String> {
    let hc = repo.head_commit()?;

    let desc = hc
        .describe()
        .names(SelectRef::AllTags)
        .try_resolve()?
        .with_context(|| {
            format!(
                "Did not find a single candidate ref for naming id '{}'",
                hc.id
            )
        })?;
    let tag = desc
        .format()?
        .name
        .with_context(|| format!("Did not find a valid tag for '{}'", hc.id))?;
    Ok(tag.to_string())
}

// Get commit author list
// git shortlog HEAD -sne | awk '{$1=""; sub(" ", "    \""); print }' | awk -F'<' '!x[$1]++' | awk -F'<' '!x[$2]++' | awk -F'<' '{gsub(/ +$/, "\",", $1); print $1}' | sort | xargs
// use email to uniq authors
pub fn get_commit_authors(repo: &Repository) -> Result<String> {
    let mut authors: BTreeMap<String, String> = BTreeMap::new();

    let revwalk = repo
        .rev_walk(Some(repo.head_id()?.detach()))
        .all()?
        .filter_map(Result::ok);
    for oid in revwalk {
        let cm = oid.object()?;
        let author = cm.author()?;
        let email = author.email.to_string();
        if authors.contains_key(&email) {
            continue;
        }
        let name = author.name.to_string();
        authors.insert(email, name);
    }

    let result = authors
        .values()
        .map(|name| (name, 1))
        .collect::<BTreeMap<&String, u8>>()
        .keys()
        .map(|name| name.as_str())
        .collect::<Vec<&str>>()
        .join(", ");

    Ok(result)
}
