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

use crate::data_mask::DataMaskId;
use crate::tenant_key::ident::TIdent;
use crate::tenant_key::raw::TIdentRaw;

/// Tenantless key mapping a masking policy id back to its name.
/// This enables reverse lookup for SHOW GRANTS and ownership listings.
pub type DataMaskIdToNameIdent = TIdent<Resource, DataMaskId>;
/// Raw form of [`DataMaskIdToNameIdent`] used for serde/protobuf.
pub type DataMaskIdToNameIdentRaw = TIdentRaw<Resource, DataMaskId>;

pub use kvapi_impl::Resource;

impl DataMaskIdToNameIdent {
    pub fn data_mask_id(&self) -> DataMaskId {
        *self.name()
    }
}

impl DataMaskIdToNameIdentRaw {
    pub fn data_mask_id(&self) -> DataMaskId {
        *self.name()
    }
}

mod kvapi_impl {

    use databend_meta_kvapi::kvapi;

    use crate::data_mask::data_mask_id_to_name_ident::DataMaskIdToNameIdent;
    use crate::data_mask::data_mask_name_ident::DataMaskNameIdentRaw;
    use crate::tenant_key::resource::TenantResource;

    pub struct Resource;
    impl TenantResource for Resource {
        const PREFIX: &'static str = "__fd_datamask_id_to_name";
        const TYPE: &'static str = "DataMaskIdToNameIdent";
        const HAS_TENANT: bool = true;
        type ValueType = DataMaskNameIdentRaw;
    }

    impl kvapi::Value for DataMaskNameIdentRaw {
        type KeyType = DataMaskIdToNameIdent;
        fn dependency_keys(&self, _key: &Self::KeyType) -> impl IntoIterator<Item = String> {
            []
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_meta_kvapi::kvapi::Key;

    use crate::data_mask::DataMaskId;
    use crate::data_mask::DataMaskIdToNameIdent;
    use crate::tenant::Tenant;

    #[test]
    fn test_data_mask_id_ident() {
        let tenant = Tenant::new_literal("dummy");
        let ident = DataMaskIdToNameIdent::new_generic(tenant, DataMaskId::new(3));

        let key = ident.to_string_key();
        assert_eq!(key, "__fd_datamask_id_to_name/dummy/3");

        assert_eq!(ident, DataMaskIdToNameIdent::from_str_key(&key).unwrap());
    }
}
