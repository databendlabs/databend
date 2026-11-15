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

use databend_common_base::base::escape_for_key;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::KeyWithTenant;
use databend_common_meta_app::principal::SettingIdent;
use databend_common_meta_app::principal::TenantUserIdent;
use databend_common_meta_app::tenant::Tenant;
use databend_meta_client::kvapi::StructKey;

use crate::warehouse::WAREHOUSE_API_KEY_PREFIX;

/// A meta-service prefix that can be served by [`MetaReadCache`].
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CachedPrefix {
    /// `__fd_users/<tenant>/`
    User,
    /// `__fd_settings/<tenant>/`
    Setting,
    /// `__fd_clusters_v6/<tenant>/`: online nodes and cluster membership.
    Cluster,
}

impl CachedPrefix {
    pub fn parse(name: &str) -> Result<Self> {
        match name.trim().to_ascii_lowercase().as_str() {
            "user" => Ok(Self::User),
            "setting" => Ok(Self::Setting),
            "cluster" => Ok(Self::Cluster),
            other => Err(ErrorCode::InvalidConfig(format!(
                "unknown meta_read_cache prefix '{other}', expect one of: user, setting, cluster"
            ))),
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Setting => "setting",
            Self::Cluster => "cluster",
        }
    }

    /// Whether "absent" may be answered from memory; see `PrefixCache`.
    pub(super) fn serve_absent(&self) -> bool {
        // No global setting means every setting has its default: as safe to serve slightly stale as
        // a changed setting. An unknown user or an empty node list falls back instead.
        matches!(self, Self::Setting)
    }

    /// The cached key range of `tenant`, with a trailing `/`.
    pub(super) fn dir(&self, tenant: &Tenant) -> Result<String> {
        let dir = match self {
            Self::User => {
                TenantUserIdent::new_user_host(tenant.clone(), "dummy", "dummy").tenant_prefix()
            }
            Self::Setting => SettingIdent::new(tenant.clone(), "").to_string_key(),
            Self::Cluster => format!(
                "{}/{}/",
                WAREHOUSE_API_KEY_PREFIX,
                escape_for_key(tenant.tenant_name())?
            ),
        };
        Ok(if dir.ends_with('/') { dir } else { dir + "/" })
    }
}
