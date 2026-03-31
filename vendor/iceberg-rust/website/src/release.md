<!--
  ~ Licensed to the Apache Software Foundation (ASF) under one
  ~ or more contributor license agreements.  See the NOTICE file
  ~ distributed with this work for additional information
  ~ regarding copyright ownership.  The ASF licenses this file
  ~ to you under the Apache License, Version 2.0 (the
  ~ "License"); you may not use this file except in compliance
  ~ with the License.  You may obtain a copy of the License at
  ~
  ~   http://www.apache.org/licenses/LICENSE-2.0
  ~
  ~ Unless required by applicable law or agreed to in writing,
  ~ software distributed under the License is distributed on an
  ~ "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  ~ KIND, either express or implied.  See the License for the
  ~ specific language governing permissions and limitations
  ~ under the License.
-->

This document mainly introduces how the release manager releases a new version in accordance with the Apache requirements.

## Introduction

`Source Release` is the key point which Apache values, and is also necessary for an ASF release.

Please remember that publishing software has legal consequences.

This guide complements the foundation-wide policies and guides:

- [Release Policy](https://www.apache.org/legal/release-policy.html)
- [Release Distribution Policy](https://infra.apache.org/release-distribution)
- [Release Creation Process](https://infra.apache.org/release-publishing.html)

## Some Terminology of release

In the context of our release, we use several terms to describe different stages of the release process.

Here's an explanation of these terms:

- `iceberg_version`: the version of Iceberg to be released, like `0.2.0`.
- `rc_version`: the minor version for voting round, like `rc.1`.
- `release_version`: the version of release candidate, like `0.2.0-rc.1`.

## Preparation

<div class="warning">

This section is the requirements for individuals who are new to the role of release manager.

</div>

Refer to [Setup GPG Key](reference/setup_gpg.md) to make sure the GPG key has been set up.

## Start a tracking issue about the next release

Start a tracking issue on GitHub for the upcoming release to track all tasks that need to be completed.

Title:

```
Tracking issues of Iceberg Rust ${iceberg_version} Release
```

Content:

```markdown
This issue is used to track tasks of the iceberg rust ${iceberg_version} release.

## Tasks

### Blockers

> Blockers are the tasks that must be completed before the release.

### Build Release

#### GitHub Side

- [ ] Bump version in project
- [ ] Update docs
- [ ] Generate dependencies list
- [ ] Push release candidate tag to GitHub

#### ASF Side

- [ ] Create an ASF Release
- [ ] Upload artifacts to the SVN dist repo

### Voting

- [ ] Start VOTE at iceberg community

### Official Release

- [ ] Push the release git tag
- [ ] Publish artifacts to SVN RELEASE branch
- [ ] Change Iceberg Rust Website download link
- [ ] Send the announcement

For details of each step, please refer to: https://rust.iceberg.apache.org/release
```

## GitHub Side

### Bump version in project

Bump all components' version in the project to the new iceberg version.
Please note that this version is the exact version of the release, not the release candidate version.

- rust core: bump version in `Cargo.toml`

### Update docs

- Update `CHANGELOG.md` by Drafting a new release [note on Github Releases](https://github.com/apache/iceberg-rust/releases/new)

### Generate dependencies list

Download and setup `cargo-deny`. You can refer to [cargo-deny](https://embarkstudios.github.io/cargo-deny/cli/index.html).

Run `python3 ./scripts/dependencies.py generate` to update the dependencies list of every package.

Run `python3 ./scripts/dependencies.py check` to verify the updated dependencies' license.

### Push release candidate tag

After bump version PR gets merged, we can create a GitHub release for the release candidate:

- Create a tag at `main` branch on the `Bump Version` / `Patch up version` commit: `git tag -s "v${release_version}"`, please correctly check out the corresponding commit instead of directly tagging on the main branch.
- Push tags to GitHub: `git push origin "v${release_version}"`.

## ASF Side

If any step in the ASF Release process fails and requires code changes,
we will abandon that version and prepare for the next one.
Our release page will only display ASF releases instead of GitHub Releases.

### Create an ASF Release

After GitHub Release has been created, we can start to create ASF Release.

- Checkout to released tag. (e.g. `git checkout v0.2.0-rc.1`, tag is created in the previous step)
- Use the release script to create a new release: `ICEBERG_VERSION=<iceberg_version> ICEBERG_VERSION_RC=<rc_version> ./scripts/release.sh`(e.g. `ICEBERG_VERSION=0.2.0 ICEBERG_VERSION_RC=rc.1 ./scripts/release.sh`)
    - This script will do the following things:
        - Create a new branch named by `release-${release_version}` from the tag
        - Generate the release candidate artifacts under `dist`, including:
            - `apache-iceberg-rust-${release_version}.tar.gz`
            - `apache-iceberg-rust-${release_version}.tar.gz.asc`
            - `apache-iceberg-rust-${release_version}.tar.gz.sha512`
        - Check the header of the source code. This step needs docker to run.

This script will create a new release under `dist`.

For example:

```shell
> tree dist
dist
├── apache-iceberg-rust-0.2.0.tar.gz
├── apache-iceberg-rust-0.2.0.tar.gz.asc
└── apache-iceberg-rust-0.2.0.tar.gz.sha512
```

### Upload artifacts to the SVN dist repo

SVN is required for this step.

The svn repository of the dev branch is: <https://dist.apache.org/repos/dist/dev/iceberg/>

First, checkout Iceberg to local directory:

```shell
# As this step will copy all the versions, it will take some time. If the network is broken, please use svn cleanup to delete the lock before re-execute it.
svn co https://dist.apache.org/repos/dist/dev/iceberg/ /tmp/iceberg-dist-dev
```

Then, upload the artifacts:

> The `${release_version}` here should be like `0.2.0-rc.1`

```shell
# create a directory named by version
mkdir /tmp/iceberg-dist-dev/apache-iceberg-rust-${release_version}/

# copy source code and signature package to the versioned directory
cp ./dist/* /tmp/iceberg-dist-dev/apache-iceberg-rust-${release_version}/

# change dir to the svn folder
cd /tmp/iceberg-dist-dev/

# check svn status
svn status

# add to svn
svn add apache-iceberg-rust-${release_version}

# commit to SVN remote server
svn commit -m "Prepare for iceberg-rust ${release_version}"
```

Visit <https://dist.apache.org/repos/dist/dev/iceberg/> to make sure the artifacts are uploaded correctly.

### Rescue

If you accidentally published wrong or unexpected artifacts, like wrong signature files, wrong sha256 files,
please cancel the release for the current `release_version`,
_increase th RC counting_ and re-initiate a release with the new `release_version`.
And remember to delete the wrong artifacts from the SVN dist repo.

## Voting

Iceberg Community Vote should send email to: <dev@iceberg.apache.org>:

Title:

```
[VOTE] Release Apache Iceberg Rust ${iceberg_version} ${rc_version}
```

Content:

```
Hello, Apache Iceberg Rust Community,

This is a call for a vote to release Apache Iceberg rust version ${iceberg_version}.

The tag to be voted on is v${release_version}.

The release candidate:

https://dist.apache.org/repos/dist/dev/iceberg/apache-iceberg-rust-${release_version}/

Keys to verify the release candidate:

https://downloads.apache.org/iceberg/KEYS

Git tag for the release:

https://github.com/apache/iceberg-rust/releases/tag/v${release_version}

Please download, verify, and test.

The VOTE will be open for at least 72 hours and until the necessary
number of votes are reached.

[ ] +1 approve
[ ] +0 no opinion
[ ] -1 disapprove with the reason

To learn more about Apache Iceberg, please see https://rust.iceberg.apache.org/

Checklist for reference:

[ ] Download links are valid.
[ ] Checksums and signatures.
[ ] LICENSE/NOTICE files exist
[ ] No unexpected binary files
[ ] All source files have ASF headers
[ ] Can compile from source

More details please refer to https://rust.iceberg.apache.org/release.html#how-to-verify-a-release.

Thanks

${name}
```

Example: <https://lists.apache.org/thread/c211gqq2yl15jbxqk4rcnq1bdqltjm5l>

After at least 3 `+1` binding vote (from Iceberg PMC member), claim the vote result:

Title:

```
[RESULT][VOTE] Release Apache Iceberg Rust ${iceberg_version} ${rc_version}
```

Content:

```
Hello, Apache Iceberg Rust Community,

The vote to release Apache Iceberg Rust ${release_version} has passed.

The vote PASSED with 3 +1 binding and 1 +1 non-binding votes, no +0 or -1 votes:

Binding votes:

- xxx
- yyy
- zzz

Non-Binding votes:

- aaa

Vote thread: ${vote_thread_url}

Thanks

${name}
```

Example: <https://lists.apache.org/thread/xk5myl10mztcfotn59oo59s4ckvojds6>

## How to verify a release

### Validating a source release

A release contains links to following things:

* A source tarball
* A signature(.asc)
* A checksum(.sha512)

After downloading them, here are the instructions on how to verify them.

* Import keys:

  ```bash
  curl https://downloads.apache.org/iceberg/KEYS -o KEYS
  gpg --import KEYS
  ```
* Verify the `.asc` file:
  
  ```bash
  gpg --verify apache-iceberg-rust-*.tar.gz.asc
  ```
  Expects: `gpg: Good signature from ...`
* Verify the checksums: 
  
  ```bash
  shasum -a 512 -c apache-iceberg-rust-*.tar.gz.sha512
  ```
  Expects: `"apache-iceberg-rust-...tar.gz: OK"`
* Verify build and test:
  
  ```bash
  tar -xzf apache-iceberg-rust-*.tar.gz
  cd apache-iceberg-rust-*/
  make build && make test
  ```
* Verify license headers: 
  
  ```bash
  docker run -it --rm -v $(pwd):/github/workspace apache/skywalking-eyes header check
  ```
  Expects: `INFO Totally checked _ files, valid: _, invalid: 0, ignored: _, fixed: 0 `

## Official Release

### Push the release git tag

```shell
# Checkout the tags that passed VOTE
git checkout "v${release_version}"
# Tag with the iceberg version
git tag -s "v${iceberg_version}"
# Push tags to github to trigger releases
git push origin "v${iceberg_version}"
```

### Publish artifacts to SVN RELEASE branch

```shell
svn mv https://dist.apache.org/repos/dist/dev/iceberg/apache-iceberg-rust-${release_version} https://dist.apache.org/repos/dist/release/iceberg/apache-iceberg-rust-${iceberg_version} -m "Release Apache Iceberg Rust ${iceberg_version}"
```

### Change Iceberg Rust Website download link

Update the download link in `website/src/download.md` to the new release version.

### Create a GitHub Release

- Click [here](https://github.com/apache/iceberg-rust/releases/new) to create a new release.
- Pick the git tag of this release version from the dropdown menu.
- Make sure the branch target is `main`.
- Generate the release note by clicking the `Generate release notes` button.
- Add the release note from every component's `upgrade.md` if there are breaking changes before the content generated by GitHub. Check them carefully.
- Publish the release.

### Send the announcement

Send the release announcement to `dev@iceberg.apache.org` and CC `announce@apache.org`.

Instead of adding breaking changes, let's include the new features as "notable changes" in the announcement.

Title:

```
[ANNOUNCE] Release Apache Iceberg Rust ${iceberg_version}
```

Content:

```
Hi all,

The Apache Iceberg Rust community is pleased to announce
that Apache Iceberg Rust ${iceberg_version} has been released!

Iceberg is a data access layer that allows users to easily and efficiently
retrieve data from various storage services in a unified way.

The notable changes since ${iceberg_version} include:
1. xxxxx
2. yyyyyy
3. zzzzzz

Please refer to the change log for the complete list of changes:
https://github.com/apache/iceberg-rust/releases/tag/v${iceberg_version}

Apache Iceberg Rust website: https://rust.iceberg.apache.org/

Download Links: https://rust.iceberg.apache.org/download

From official ASF distribution: https://dist.apache.org/repos/dist/release/iceberg/apache-iceberg-rust-${release_version}/

Iceberg Resources:
- Issue: https://github.com/apache/iceberg-rust/issues
- Mailing list: dev@iceberg.apache.org

Thanks
On behalf of Apache Iceberg Community
```

Example: <https://lists.apache.org/thread/oy77n55brvk72tnlb2bjzfs9nz3cfd0s>
