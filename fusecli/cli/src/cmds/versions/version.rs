// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use sha2::Digest;
use sha2::Sha256;
use structopt::StructOpt;

use crate::error::Result;

#[derive(StructOpt, Debug)]
pub struct VersionCommand {}

impl VersionCommand {
    pub async fn execute(&self) -> Result<()> {
        let build_semver = option_env!("VERGEN_BUILD_SEMVER");
        let git_sha = option_env!("VERGEN_GIT_SHA_SHORT");
        let timestamp = option_env!("VERGEN_BUILD_TIMESTAMP");
        let (ver, git, ts) = match (build_semver, git_sha, timestamp) {
            (Some(ver), Some(git), Some(ts)) => (ver, git, ts),
            _ => ("", "", ""),
        };

        self.print_with_width("Datafuse CLI", ver, 20);
        if let Some(sha) = self.cli_sha() {
            self.print_with_width("Datafuse CLI SHA256", &sha, 20);
        }
        self.print_with_width("Git commit", git, 20);
        self.print_with_width("Build date", ts, 20);

        Ok(())
    }

    fn cli_sha(&self) -> Option<String> {
        let path = std::env::current_exe().ok()?;
        let cli_bin = std::fs::read(path).ok()?;
        let mut hasher = Sha256::default();
        hasher.update(cli_bin);
        let bin_sha256 = hasher.finalize();
        Some(format!("{:x}", &bin_sha256))
    }

    fn print_with_width(&self, name: &str, version: &str, width: usize) {
        println!("{:width$} : {}", name, version, width = width);
    }
}
