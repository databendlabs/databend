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

use databend_common_meta_app::KeyWithTenant;
use databend_common_meta_app::principal::SettingIdent;
use databend_common_meta_app::principal::TenantUserIdent;
use databend_common_meta_app::tenant::Tenant;
use databend_meta_client::kvapi::StructKey;

/// A meta-service key range whose reads may be cached. This is the only place of the cache that
/// knows what is stored in a range; add a range here to cache another kind of data.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum CachedRange {
    /// `__fd_users/<tenant>/`
    User,
    /// `__fd_settings/<tenant>/`
    Setting,
}

impl CachedRange {
    pub(crate) const ALL: [CachedRange; 2] = [CachedRange::User, CachedRange::Setting];

    pub(crate) fn name(&self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Setting => "setting",
        }
    }

    /// Whether "absent" may be kept in memory.
    pub(crate) fn keep_absent(&self) -> bool {
        // No global setting means every setting has its default: as safe to keep as a value. An
        // unknown user is read again, so that a user just created elsewhere is found at once.
        matches!(self, Self::Setting)
    }

    /// The key range of `tenant`, with a trailing `/`.
    pub(crate) fn dir(&self, tenant: &Tenant) -> String {
        let dir = match self {
            Self::User => {
                TenantUserIdent::new_user_host(tenant.clone(), "dummy", "dummy").tenant_prefix()
            }
            Self::Setting => SettingIdent::new(tenant.clone(), "").to_string_key(),
        };
        if dir.ends_with('/') { dir } else { dir + "/" }
    }
}
