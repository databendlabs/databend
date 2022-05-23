// Copyright 2021 Datafuse Labs.
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

use anyhow::Result;
use git2::DescribeFormatOptions;
use git2::DescribeOptions;
use git2::Repository;

// Get the latest tag:
// git describe --tags --abbrev=0
// v0.6.99-nightly
pub fn get_latest_tag(repo: &Repository) -> Result<String> {
    let mut opts = DescribeOptions::new();
    let mut format_options = DescribeFormatOptions::new();
    let describe = repo.describe(opts.describe_tags())?;
    let tag = describe.format(Some(format_options.abbreviated_size(0)))?;
    Ok(tag)
}

// Get commit author list
// git shortlog HEAD -sne | awk '{$1=""; sub(" ", "    \""); print }' | awk -F'<' '!x[$1]++' | awk -F'<' '!x[$2]++' | awk -F'<' '{gsub(/ +$/, "\",", $1); print $1}' | sort | xargs
// use email to uniq authors
pub fn get_commit_authors(repo: &Repository) -> Result<String> {
    let mut authors: BTreeMap<String, String> = BTreeMap::new();

    let mut revwalk = repo.revwalk()?;
    revwalk.push_head()?;
    for oid in revwalk {
        let commit = repo.find_commit(oid?)?;
        let author = commit.author();
        match author.email() {
            None => continue,
            Some(email) => {
                if authors.contains_key(email) {
                    continue;
                }
                match author.name() {
                    None => continue,
                    Some(name) => {
                        authors.insert(email.to_string(), name.to_string());
                    }
                }
            }
        }
    }

    let result = authors
        .iter()
        .map(|(_, name)| (name, 1))
        .collect::<BTreeMap<&String, u8>>()
        .iter()
        .map(|(name, _)| name.as_str())
        .collect::<Vec<&str>>()
        .join(", ");

    Ok(result)
}
