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

use std::fmt::Debug;
use std::fmt::Display;
use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

use databend_common_base::base::GlobalInstance;
use databend_common_exception::ErrorCode;
use databend_common_meta_app::storage::StorageParams;
use serde::Deserialize;
use serde::Serialize;
use tokio::sync::RwLock;

use crate::auth::RefreshableToken;
use crate::auth::TokenFile;

/// Config for storage backend.
///
/// # TODO(xuanwo)
///
/// In the future, we will use the following storage config layout:
///
/// ```toml
/// [storage]
///
/// [storage.data]
/// type = "s3"
///
/// [storage.temporary]
/// type = "s3"
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct StorageConfig {
    pub num_cpus: u64,
    pub allow_insecure: bool,
    /// Runtime policy used to validate user-controlled external storage
    /// endpoints before query nodes connect to them.
    pub endpoint_url_policy: EndpointUrlPolicyConfig,
    pub params: StorageParams,
    /// Global switches that affect the ambient credential chain behavior.
    ///
    /// Notes:
    /// - These are runtime-only controls and are not persisted in meta.
    /// - They apply to all storage operators created in this process.
    pub disable_config_load: bool,
    pub disable_instance_profile: bool,
    /// Controls whether stage paths containing parent directory components
    /// (`../`) are allowed.
    pub stage_path_traversal_policy: StagePathTraversalPolicy,
}

/// Policy controlling stage paths that contain parent directory traversal
/// components (`../`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum StagePathTraversalPolicy {
    /// Reject traversal paths for both read and write operations.
    #[default]
    Disable,
    /// Allow traversal paths for both read and write operations.
    Enable,
    /// Existing traversal paths can be read but new ones cannot be written.
    ReadOnly,
}

impl StagePathTraversalPolicy {
    pub fn allows_read(self) -> bool {
        matches!(
            self,
            StagePathTraversalPolicy::Enable | StagePathTraversalPolicy::ReadOnly
        )
    }

    pub fn allows_write(self) -> bool {
        matches!(self, StagePathTraversalPolicy::Enable)
    }
}

impl FromStr for StagePathTraversalPolicy {
    type Err = ErrorCode;

    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s.to_ascii_lowercase().as_str() {
            "disable" => Ok(StagePathTraversalPolicy::Disable),
            "enable" => Ok(StagePathTraversalPolicy::Enable),
            "readonly" => Ok(StagePathTraversalPolicy::ReadOnly),
            _ => Err(ErrorCode::InvalidConfig(format!(
                "invalid stage_path_traversal_policy: {:?}, valid values are: disable, enable, readonly",
                s
            ))),
        }
    }
}

impl Display for StagePathTraversalPolicy {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StagePathTraversalPolicy::Disable => write!(f, "disable"),
            StagePathTraversalPolicy::Enable => write!(f, "enable"),
            StagePathTraversalPolicy::ReadOnly => write!(f, "readonly"),
        }
    }
}

/// Endpoint target validation mode for external storage URLs.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum EndpointUrlPolicy {
    /// Keep backward compatibility: allow endpoints unless they match an
    /// explicit blocklist or protected internal socket.
    #[default]
    Permissive,
    /// Reject high-risk targets such as loopback, private, link-local,
    /// multicast, unspecified and metadata addresses unless explicitly allowed.
    Strict,
    /// Reject all endpoints unless they match the configured allowlist
    /// (`allowed_hosts` or `allowed_cidrs`). This is the most restrictive
    /// mode: even public addresses are denied by default.
    Allowlist,
}

/// Runtime egress policy for external storage endpoint URLs.
///
/// This config is process-local and is applied by query nodes before creating
/// storage clients and before sending storage HTTP requests. It is intentionally
/// separate from `allow_insecure`, which only controls whether non-HTTPS storage
/// endpoints are allowed.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct EndpointUrlPolicyConfig {
    /// Validation mode. `Permissive` preserves compatibility, while `Strict`
    /// rejects high-risk resolved targets by default.
    pub policy: EndpointUrlPolicy,
    /// Hostnames that are allowed even when their resolved IPs are normally
    /// rejected by strict mode. Supports exact hosts and subdomain-only
    /// wildcard patterns such as `*.example.com`.
    pub allowed_hosts: Vec<String>,
    /// IP ranges that are allowed even when they are normally rejected by
    /// strict mode. Entries may be IP addresses or CIDR blocks.
    pub allowed_cidrs: Vec<String>,
    /// Hostnames that are always rejected. Supports exact hosts and
    /// subdomain-only wildcard patterns such as `*.example.com`.
    pub blocked_hosts: Vec<String>,
    /// IP ranges that are always rejected. Entries may be IP addresses or CIDR
    /// blocks.
    pub blocked_cidrs: Vec<String>,
    /// Internal service sockets that storage endpoints must never access.
    ///
    /// Query initializes this from configured databend-meta endpoints. Wildcard
    /// binds such as `0.0.0.0:9191` and `[::]:9191` protect local and private
    /// IPs (those matched by `builtin_blocked_ip_reason`) on the same port.
    /// Public IPs bound on those ports are not covered. IPv6 socket addresses
    /// must be bracketed.
    ///
    /// This field is runtime-only: it is populated at process init from meta
    /// endpoint config and is not persisted or round-tripped through
    /// serialization.
    pub protected_sockets: Vec<String>,
}

/// Runtime trust scope for storage endpoint URL egress checks.
///
/// This is intentionally not persisted in `StorageParams`: the same storage
/// config can be trusted when it comes from server config, and external when it
/// comes from user SQL.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum EndpointPolicyScope {
    /// Server-controlled storage configuration. Endpoint egress policy is not
    /// applied.
    #[default]
    Trusted,
    /// User-controlled external storage configuration. Endpoint egress policy
    /// is applied before requests are sent.
    External,
}

/// Runtime-only switches for ambient credential chain behavior.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct CredentialChainConfig {
    pub disable_config_load: bool,
    pub disable_instance_profile: bool,
}

impl CredentialChainConfig {
    pub fn init(cfg: CredentialChainConfig) -> databend_common_exception::Result<()> {
        GlobalInstance::set(cfg);
        Ok(())
    }

    pub fn try_get() -> Option<CredentialChainConfig> {
        GlobalInstance::try_get()
    }
}

// TODO: This config should be moved out of common-storage crate.
#[derive(Clone)]
pub struct ShareTableConfig {
    pub share_endpoint_address: Option<String>,
    pub share_endpoint_token: RefreshableToken,
}

impl ShareTableConfig {
    pub fn init(
        share_endpoint_address: &str,
        token_file: &str,
        default_token: String,
    ) -> databend_common_exception::Result<()> {
        GlobalInstance::set(Self::try_create(
            share_endpoint_address,
            token_file,
            default_token,
        )?);

        Ok(())
    }

    pub fn try_create(
        share_endpoint_address: &str,
        token_file: &str,
        default_token: String,
    ) -> databend_common_exception::Result<ShareTableConfig> {
        let share_endpoint_address = if share_endpoint_address.is_empty() {
            None
        } else {
            Some(share_endpoint_address.to_owned())
        };
        let share_endpoint_token = if token_file.is_empty() {
            RefreshableToken::Direct(default_token)
        } else {
            let s = String::from(token_file);
            let f = TokenFile::new(Path::new(&s))?;
            RefreshableToken::File(Arc::new(RwLock::new(f)))
        };
        Ok(ShareTableConfig {
            share_endpoint_address,
            share_endpoint_token,
        })
    }

    pub fn share_endpoint_address() -> Option<String> {
        ShareTableConfig::instance().share_endpoint_address
    }

    pub fn share_endpoint_token() -> RefreshableToken {
        ShareTableConfig::instance().share_endpoint_token
    }

    pub fn instance() -> ShareTableConfig {
        GlobalInstance::get()
    }
}
