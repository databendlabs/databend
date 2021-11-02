// Copyright 2020 Datafuse Labs.
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

use std::sync::Arc;

use async_trait::async_trait;
use clap::App;
use clap::ArgMatches;
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

    pub fn default() -> Self {
        VersionCommand::create()
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

    pub async fn exec(&self, writer: &mut Writer, _args: String) -> Result<()> {
        self.exec_matches(writer, None).await
    }
}

#[async_trait]
impl Command for VersionCommand {
    fn name(&self) -> &str {
        "version"
    }

    fn clap(&self) -> App<'static> {
        App::new("version").about("Version info for local cli and remote cluster")
    }

    fn about(&self) -> &'static str {
        "Databend CLI version"
    }

    fn subcommands(&self) -> Vec<Arc<dyn Command>> {
        vec![]
    }

    fn is(&self, s: &str) -> bool {
        self.name() == s
    }

    async fn exec_matches(&self, writer: &mut Writer, _args: Option<&ArgMatches>) -> Result<()> {
        let build_semver = option_env!("VERGEN_BUILD_SEMVER");
        let git_sha = option_env!("VERGEN_GIT_SHA_SHORT");
        let timestamp = option_env!("VERGEN_BUILD_TIMESTAMP");
        let (ver, git, ts) = match (build_semver, git_sha, timestamp) {
            (Some(ver), Some(git), Some(ts)) => (ver, git, ts),
            _ => ("", "", ""),
        };

        writer.writeln_width("Databend CLI", ver);
        if let Some(sha) = self.cli_sha_info() {
            writer.writeln_width("Databend CLI SHA256", &sha);
        }
        writer.writeln_width("Git commit", git);
        writer.writeln_width("Build date", ts);

        if let Some(os) = self.os_info() {
            writer.writeln_width("OS version", &os);
        }

        Ok(())
    }
}
