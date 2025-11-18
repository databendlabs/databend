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

use crate::row_access_policy::RowAccessPolicyId;
use crate::tenant_key::ident::TIdent;
use crate::tenant_key::raw::TIdentRaw;

/// Tenantless key mapping a row access policy id back to its name for reverse lookups.
pub type RowAccessPolicyIdToNameIdent = TIdent<Resource, RowAccessPolicyId>;
/// Raw form of [`RowAccessPolicyIdToNameIdent`] used for serde/protobuf.
pub type RowAccessPolicyIdToNameIdentRaw = TIdentRaw<Resource, RowAccessPolicyId>;

pub use kvapi_impl::Resource;

impl RowAccessPolicyIdToNameIdent {
    pub fn row_access_policy_id(&self) -> RowAccessPolicyId {
        *self.name()
    }
}

impl RowAccessPolicyIdToNameIdentRaw {
    pub fn row_access_policy_id(&self) -> RowAccessPolicyId {
        *self.name()
    }
}

mod kvapi_impl {

    use databend_common_meta_kvapi::kvapi;

    use crate::row_access_policy::row_access_policy_id_to_name_ident::RowAccessPolicyIdToNameIdent;
    use crate::row_access_policy::RowAccessPolicyNameIdentRaw;
    use crate::tenant_key::resource::TenantResource;

    pub struct Resource;
    impl TenantResource for Resource {
        const PREFIX: &'static str = "__fd_row_access_policy_id_to_name";
        const TYPE: &'static str = "RowAccessPolicyIdToNameIdent";
        const HAS_TENANT: bool = true;
        type ValueType = RowAccessPolicyNameIdentRaw;
    }

    impl kvapi::Value for RowAccessPolicyNameIdentRaw {
        type KeyType = RowAccessPolicyIdToNameIdent;
        fn dependency_keys(&self, _key: &Self::KeyType) -> impl IntoIterator<Item = String> {
            []
        }
    }
}
