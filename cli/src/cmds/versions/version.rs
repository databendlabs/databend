// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use sha2::Digest;
use sha2::Sha256;
use sysinfo::SystemExt;

use crate::cmds::command::Command;
use crate::cmds::Writer;
use crate::error::Result;

#[derive(Clone)]
pub struct VersionCommand {}

impl VersionCommand {
    pub fn create() -> Self {
        VersionCommand {}
    }

    fn cli_sha_info(&self) -> Option<String> {
        let path = std::env::current_exe().ok()?;
        let cli_bin = std::fs::read(path).ok()?;
        let mut hasher = Sha256::default();
        hasher.update(cli_bin);
        let bin_sha256 = hasher.finalize();
        Some(format!("{:x}", &bin_sha256))
    }

    fn os_info(&self) -> Option<String> {
        let sys = sysinfo::System::new_all();

        let info = format!(
            "{} {} (kernel {})",
            sys.host_name()?,
            sys.os_version()?,
            sys.kernel_version()?,
        );

        Some(info)
    }
}

impl Command for VersionCommand {
    fn name(&self) -> &str {
        "version"
    }

    fn about(&self) -> &str {
        "Datafuse CLI version"
    }

    fn is(&self, s: &str) -> bool {
        self.name() == s
    }

    fn exec(&self, writer: &mut Writer, _args: String) -> Result<()> {
        let build_semver = option_env!("VERGEN_BUILD_SEMVER");
        let git_sha = option_env!("VERGEN_GIT_SHA_SHORT");
        let timestamp = option_env!("VERGEN_BUILD_TIMESTAMP");
        let (ver, git, ts) = match (build_semver, git_sha, timestamp) {
            (Some(ver), Some(git), Some(ts)) => (ver, git, ts),
            _ => ("", "", ""),
        };

        writer.writeln_width("Datafuse CLI", ver);
        if let Some(sha) = self.cli_sha_info() {
            writer.writeln_width("Datafuse CLI SHA256", &sha);
        }
        writer.writeln_width("Git commit", git);
        writer.writeln_width("Build date", ts);

        if let Some(os) = self.os_info() {
            writer.writeln_width("OS version", &os);
        }

        Ok(())
    }
}
