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

use crate::tenant_key::ident::TIdent;
use crate::tenant_key::raw::TIdentRaw;

pub type RowAccessPolicyNameIdent = TIdent<Resource>;
pub type RowAccessPolicyNameIdentRaw = TIdentRaw<Resource>;

pub use kvapi_impl::Resource;

impl RowAccessPolicyNameIdent {
    pub fn row_access_name(&self) -> &str {
        self.name()
    }
}

impl RowAccessPolicyNameIdentRaw {
    pub fn row_access_name(&self) -> &str {
        self.name()
    }
}

mod kvapi_impl {

    use crate::row_access_policy::RowAccessPolicyId;
    use crate::tenant_key::resource::TenantResource;

    pub struct Resource;
    impl TenantResource for Resource {
        const PREFIX: &'static str = "__fd_row_access_policy";
        const TYPE: &'static str = "RowAccessPolicyNameIdent";
        const HAS_TENANT: bool = true;
        type ValueType = RowAccessPolicyId;
    }
}

#[cfg(test)]
mod tests {

    use databend_meta_client::kvapi::testing::assert_round_trip;

    use crate::row_access_policy::RowAccessPolicyNameIdent;
    use crate::tenant::Tenant;

    #[test]
    fn test_ident() {
        let tenant = Tenant::new_literal("tenant1");
        let ident = RowAccessPolicyNameIdent::new(tenant, "test");
        assert_round_trip(ident, "__fd_row_access_policy/tenant1/test");
    }
}
