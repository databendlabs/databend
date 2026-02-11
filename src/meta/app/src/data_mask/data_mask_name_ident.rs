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

pub type DataMaskNameIdent = TIdent<Resource>;
pub type DataMaskNameIdentRaw = TIdentRaw<Resource>;

pub use kvapi_impl::Resource;

impl DataMaskNameIdent {
    pub fn data_mask_name(&self) -> &str {
        self.name()
    }
}

impl DataMaskNameIdentRaw {
    pub fn data_mask_name(&self) -> &str {
        self.name()
    }
}

mod kvapi_impl {

    use databend_meta_kvapi::kvapi;
    use databend_meta_kvapi::kvapi::Key;

    use crate::KeyWithTenant;
    use crate::data_mask::DataMaskId;
    use crate::data_mask::DataMaskNameIdent;
    use crate::tenant_key::resource::TenantResource;

    pub struct Resource;
    impl TenantResource for Resource {
        const PREFIX: &'static str = "__fd_datamask";
        const TYPE: &'static str = "DataMaskNameIdent";
        const HAS_TENANT: bool = true;
        type ValueType = DataMaskId;
    }

    impl kvapi::Value for DataMaskId {
        type KeyType = DataMaskNameIdent;

        fn dependency_keys(&self, key: &Self::KeyType) -> impl IntoIterator<Item = String> {
            [self.into_t_ident(key.tenant()).to_string_key()]
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_meta_kvapi::kvapi::Key;

    use crate::data_mask::DataMaskNameIdent;
    use crate::tenant::Tenant;

    #[test]
    fn test_ident() {
        let tenant = Tenant::new_literal("tenant1");
        let ident = DataMaskNameIdent::new(tenant.clone(), "test");
        assert_eq!("__fd_datamask/tenant1/test", ident.to_string_key());

        let got = DataMaskNameIdent::from_str_key(&ident.to_string_key()).unwrap();
        assert_eq!(ident, got);
    }
}
