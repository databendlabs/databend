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

/// RowAccess Policy can be applied to tables, If drop a row access policy
/// should get all __fd_row_access_policy_apply_table_id/tenant/<policy_id> key
/// and remove table's bind.
pub type RowAccessPolicyTableIdIdent = TIdent<Resource, RowAccessPolicyId>;
pub type RowAccessPolicyTableIdIdentRaw = TIdentRaw<Resource, RowAccessPolicyId>;

pub use kvapi_impl::Resource;

mod kvapi_impl {

    use databend_common_meta_kvapi::kvapi;

    use crate::row_access_policy::RowAccessPolicyTableId;
    use crate::row_access_policy::RowAccessPolicyTableIdIdent;
    use crate::tenant_key::resource::TenantResource;

    pub struct Resource;
    impl TenantResource for Resource {
        const PREFIX: &'static str = "__fd_row_access_policy_apply_table_id";
        const TYPE: &'static str = "RowAccessPolicyTableIdListIdent";
        const HAS_TENANT: bool = true;
        type ValueType = RowAccessPolicyTableId;
    }

    impl kvapi::Value for RowAccessPolicyTableId {
        type KeyType = RowAccessPolicyTableIdIdent;
        fn dependency_keys(&self, _key: &Self::KeyType) -> impl IntoIterator<Item = String> {
            []
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_common_meta_kvapi::kvapi::Key;

    use crate::row_access_policy::RowAccessPolicyId;
    use crate::row_access_policy::RowAccessPolicyIdIdent;
    use crate::row_access_policy::RowAccessPolicyTableIdIdent;
    use crate::tenant::Tenant;

    #[test]
    fn test_ident() {
        let tenant = Tenant::new_literal("tenant1");
        let id = RowAccessPolicyId::new(20);
        let id_ident = RowAccessPolicyIdIdent::new_generic(tenant, id);
        let ident = RowAccessPolicyTableIdIdent::new_from(id_ident);
        assert_eq!(
            "__fd_row_access_policy_apply_table_id/tenant1/20",
            ident.to_string_key()
        );

        let got = RowAccessPolicyTableIdIdent::from_str_key(&ident.to_string_key()).unwrap();
        assert_eq!(ident, got);
    }
}
