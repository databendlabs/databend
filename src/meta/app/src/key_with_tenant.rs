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

//! Define behaviors of a `kvapi::Key` that contains a tenant.

use databend_meta_kvapi::kvapi;

use crate::tenant::Tenant;

/// Define behaviors of a `kvapi::Key` that contains a tenant.
pub trait KeyWithTenant {
    /// Return the tenant this key belongs to.
    fn tenant(&self) -> &Tenant;

    /// Return the name of the embedded tenant.
    fn tenant_name(&self) -> &str {
        self.tenant().tenant_name()
    }

    /// Return a encoded key prefix for listing keys of this kind that belong to the tenant.
    ///
    /// It is in form of `<__PREFIX>/<tenant>/`.
    /// The trailing `/` is important for exclude tenants with prefix same as this tenant.
    // TODO: test tenant_prefix with tenant config
    fn tenant_prefix(&self) -> String
    where Self: kvapi::Key {
        kvapi::KeyBuilder::new_prefixed(Self::PREFIX)
            .push_str(self.tenant().tenant_name())
            // Add trailing "/"
            .push_raw("")
            .done()
    }
}
