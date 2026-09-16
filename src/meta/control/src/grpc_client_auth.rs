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

use std::fs;
use std::path::Path;
use std::path::PathBuf;

use anyhow::Context;
use anyhow::bail;
use clap::Args;
use databend_meta::raft_config::Secret;
use serde::Deserialize;
use serde::Serialize;

const DEFAULT_USERNAME: &str = "root";

/// Credentials used by administrative gRPC clients.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GrpcClientAuth {
    username: String,
    password: Secret,
}

impl Default for GrpcClientAuth {
    fn default() -> Self {
        Self {
            username: DEFAULT_USERNAME.to_string(),
            password: Secret::new(""),
        }
    }
}

impl GrpcClientAuth {
    /// Returns the username sent during the gRPC handshake.
    pub fn username(&self) -> &str {
        &self.username
    }

    /// Exposes the password for the gRPC handshake.
    pub fn expose_password(&self) -> &str {
        self.password.expose()
    }
}

/// Command-line inputs for administrative gRPC credentials.
///
/// The default `root` user and empty password retain compatibility with old
/// servers and authenticated servers running in permissive mode.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq, Args)]
#[serde(default)]
pub struct GrpcClientAuthArgs {
    /// Username sent during the gRPC handshake.
    #[clap(
        long = "user",
        default_value = DEFAULT_USERNAME,
        global = true
    )]
    username: String,

    /// File containing the password sent during the gRPC handshake.
    #[clap(long = "password-file", value_name = "PATH", global = true)]
    password_file: Option<PathBuf>,
}

impl Default for GrpcClientAuthArgs {
    fn default() -> Self {
        Self {
            username: DEFAULT_USERNAME.to_string(),
            password_file: None,
        }
    }
}

impl GrpcClientAuthArgs {
    /// Loads and validates the effective administrative gRPC credentials.
    pub fn load(&self) -> anyhow::Result<GrpcClientAuth> {
        if self.username.is_empty() {
            bail!("gRPC authentication username must not be empty");
        }

        let password = self.load_password()?;
        Ok(GrpcClientAuth {
            username: self.username.clone(),
            password,
        })
    }

    fn load_password(&self) -> anyhow::Result<Secret> {
        let Some(path) = &self.password_file else {
            return Ok(Secret::new(""));
        };
        let password = read_password_file(path)?;
        if password.is_empty() {
            bail!("gRPC authentication password must not be empty");
        }
        Ok(Secret::new(password))
    }
}

fn read_password_file(path: &Path) -> anyhow::Result<String> {
    let mut password = fs::read_to_string(path)
        .with_context(|| format!("failed to read gRPC authentication password from {path:?}"))?;
    if password.ends_with('\n') {
        password.pop();
        if password.ends_with('\r') {
            password.pop();
        }
    }
    Ok(password)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::GrpcClientAuthArgs;

    #[test]
    fn test_load_password_from_file() -> anyhow::Result<()> {
        let temp_dir = tempdir()?;
        let password_file = temp_dir.path().join("password");
        fs::write(&password_file, "secret\r\n")?;
        let args = GrpcClientAuthArgs {
            username: "metactl".to_string(),
            password_file: Some(password_file),
        };

        let auth = args.load()?;

        let username = auth.username();
        let password = auth.expose_password();
        assert_eq!(username, "metactl");
        assert_eq!(password, "secret");
        Ok(())
    }

    #[test]
    fn test_default_keeps_legacy_credentials() -> anyhow::Result<()> {
        let args = GrpcClientAuthArgs::default();

        let auth = args.load()?;

        let username = auth.username();
        let password = auth.expose_password();
        assert_eq!(username, "root");
        assert_eq!(password, "");
        Ok(())
    }

    #[test]
    fn test_reject_empty_password_file() -> anyhow::Result<()> {
        let temp_dir = tempdir()?;
        let password_file = temp_dir.path().join("password");
        fs::write(&password_file, "")?;
        let args = GrpcClientAuthArgs {
            password_file: Some(password_file),
            ..Default::default()
        };

        let result = args.load();
        let error = result.unwrap_err();
        let message = error.to_string();

        assert_eq!(message, "gRPC authentication password must not be empty");
        Ok(())
    }

    #[test]
    fn test_auth_debug_redacts_password() -> anyhow::Result<()> {
        let password = "secret-that-must-not-leak";
        let temp_dir = tempdir()?;
        let password_file = temp_dir.path().join("password");
        fs::write(&password_file, password)?;
        let args = GrpcClientAuthArgs {
            password_file: Some(password_file),
            ..Default::default()
        };

        let auth = args.load()?;

        let debug = format!("{auth:?}");
        let leaked = debug.contains(password);
        let redacted = debug.contains("***");

        assert!(!leaked);
        assert!(redacted);
        Ok(())
    }
}
