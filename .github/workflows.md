# Databend Workflow

## Release

Databend make new releases via workflow [Release](./workflows/release.yml).

There are three ways to trigger a new release:

- Push a new tag
- On Schedule (every day at 00:00 UTC+8)
- Manually triggered

If new tags pushed, we use this the new tag name as release name. Otherwise, we will generate a new nightly tag.

For example:

- If current latest release is `v0.7.0`, we will generate a nightly tag `v0.7.1-nightly`
- If current latest release is `v0.7.10-nightly`, we will generate a new tag `v0.7.11-nightly`

For every release, we will:

- Create a new release on GitHub.
- Build binaries and pack them on Linux and MacOS.
- Upload built packages to GitHub Releases, AWS S3 bucket and Docker Registry.

We are adopting github native release notes generation which controlled by [release.yml](./release.yml).
