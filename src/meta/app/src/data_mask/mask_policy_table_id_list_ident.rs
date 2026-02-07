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

pub type MaskPolicyTableIdListIdent = TIdent<Resource>;
pub type MaskPolicyTableIdListIdentRaw = TIdentRaw<Resource>;

pub use kvapi_impl::Resource;

mod kvapi_impl {

    use databend_meta_kvapi::kvapi;

    use crate::data_mask::MaskPolicyTableIdListIdent;
    use crate::data_mask::MaskpolicyTableIdList;
    use crate::tenant_key::resource::TenantResource;

    pub struct Resource;
    impl TenantResource for Resource {
        const PREFIX: &'static str = "__fd_datamask_id_list";
        const TYPE: &'static str = "MaskPolicyTableIdListIdent";
        const HAS_TENANT: bool = true;
        type ValueType = MaskpolicyTableIdList;
    }

    impl kvapi::Value for MaskpolicyTableIdList {
        type KeyType = MaskPolicyTableIdListIdent;
        fn dependency_keys(&self, _key: &Self::KeyType) -> impl IntoIterator<Item = String> {
            []
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_meta_kvapi::kvapi::Key;

    use crate::data_mask::mask_policy_table_id_list_ident::MaskPolicyTableIdListIdent;
    use crate::tenant::Tenant;

    #[test]
    fn test_ident() {
        let tenant = Tenant::new_literal("tenant1");
        let ident = MaskPolicyTableIdListIdent::new(tenant.clone(), "test");
        assert_eq!("__fd_datamask_id_list/tenant1/test", ident.to_string_key());

        let got = MaskPolicyTableIdListIdent::from_str_key(&ident.to_string_key()).unwrap();
        assert_eq!(ident, got);
    }
}
