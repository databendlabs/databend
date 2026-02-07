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

use databend_meta_kvapi::kvapi::KeyBuilder;
use databend_meta_kvapi::kvapi::KeyCodec;
use databend_meta_kvapi::kvapi::KeyError;
use databend_meta_kvapi::kvapi::KeyParser;

use crate::tenant_key::ident::TIdent;
use crate::tenant_key::raw::TIdentRaw;

#[derive(Clone, PartialEq, Debug)]
pub struct MaskPolicyIdTableId {
    pub policy_id: u64,
    pub table_id: u64,
}

impl KeyCodec for MaskPolicyIdTableId {
    fn encode_key(&self, b: KeyBuilder) -> KeyBuilder {
        b.push_u64(self.policy_id).push_u64(self.table_id)
    }

    fn decode_key(parser: &mut KeyParser) -> Result<Self, KeyError>
    where Self: Sized {
        let policy_id = parser.next_u64()?;
        let table_id = parser.next_u64()?;
        Ok(Self {
            policy_id,
            table_id,
        })
    }
}

/// Mask Policy can be applied to tables. When dropping a masking policy,
/// should get all __fd_mask_policy_apply_table_id/tenant/<policy_id>/<table_id>
/// and remove the table's reference.
pub type MaskPolicyTableIdIdent = TIdent<Resource, MaskPolicyIdTableId>;
pub type MaskPolicyTableIdIdentRaw = TIdentRaw<Resource, MaskPolicyIdTableId>;

pub use kvapi_impl::Resource;

mod kvapi_impl {

    use databend_meta_kvapi::kvapi;

    use crate::data_mask::MaskPolicyTableId;
    use crate::data_mask::mask_policy_policy_table_id_ident::MaskPolicyTableIdIdent;
    use crate::tenant_key::resource::TenantResource;

    pub struct Resource;
    impl TenantResource for Resource {
        const PREFIX: &'static str = "__fd_mask_policy_apply_table_id";
        const TYPE: &'static str = "MaskPolicyTableIdIdent";
        const HAS_TENANT: bool = true;
        type ValueType = MaskPolicyTableId;
    }

    impl kvapi::Value for MaskPolicyTableId {
        type KeyType = MaskPolicyTableIdIdent;
        fn dependency_keys(&self, _key: &Self::KeyType) -> impl IntoIterator<Item = String> {
            []
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_meta_kvapi::kvapi::Key;

    use crate::data_mask::mask_policy_policy_table_id_ident::MaskPolicyIdTableId;
    use crate::data_mask::mask_policy_policy_table_id_ident::MaskPolicyTableIdIdent;
    use crate::tenant::Tenant;

    #[test]
    fn test_ident() {
        let tenant = Tenant::new_literal("tenant1");
        let id = MaskPolicyIdTableId {
            policy_id: 20,
            table_id: 10,
        };
        let ident = MaskPolicyTableIdIdent::new_generic(tenant.clone(), id);
        assert_eq!(
            "__fd_mask_policy_apply_table_id/tenant1/20/10",
            ident.to_string_key()
        );

        let got = MaskPolicyTableIdIdent::from_str_key(&ident.to_string_key()).unwrap();
        assert_eq!(ident, got);
    }
}
