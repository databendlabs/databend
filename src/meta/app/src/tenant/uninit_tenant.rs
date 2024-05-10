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

use std::fmt::Display;

use databend_common_meta_types::NonEmptyString;

use crate::app_error::TenantIsEmpty;
use crate::tenant::Tenant;

/// Tenant that is not yet  initialized with per-tenant config.
/// Can not be used as a meta-service key.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct UninitTenant {
    pub tenant: NonEmptyString,
}

impl UninitTenant {
    pub fn new_or_err(tenant: impl ToString, ctx: impl Display) -> Result<Self, TenantIsEmpty> {
        let non_empty =
            NonEmptyString::new(tenant.to_string()).map_err(|_e| TenantIsEmpty::new(ctx))?;

        let t = Self { tenant: non_empty };

        Ok(t)
    }

    /// Initialize the tenant with the given config.
    // TODO: implement config
    pub fn initialize(self, _config: ()) -> Tenant {
        Tenant {
            tenant: self.tenant.as_str().to_string(),
        }
    }
}
