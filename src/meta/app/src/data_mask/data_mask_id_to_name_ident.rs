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

pub type DataMaskIdToNameIdent = TIdent<Resource, DataMaskId>;
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

    use databend_common_meta_kvapi::kvapi;

    use crate::data_mask::data_mask_id_to_name_ident::DataMaskIdToNameIdent;
    use crate::data_mask::data_mask_name_ident::DataMaskNameIdentRaw;
    use crate::tenant_key::resource::TenantResource;

    pub struct Resource;
    impl TenantResource for Resource {
        const PREFIX: &'static str = "__fd_datamask_id_to_name";
        const TYPE: &'static str = "DataMaskIdToNameIdent";
        const HAS_TENANT: bool = false;
        type ValueType = DataMaskNameIdentRaw;
    }

    impl kvapi::Value for DataMaskNameIdentRaw {
        type KeyType = DataMaskIdToNameIdent;
        fn dependency_keys(&self, _key: &Self::KeyType) -> impl IntoIterator<Item = String> {
            []
        }
    }
}
