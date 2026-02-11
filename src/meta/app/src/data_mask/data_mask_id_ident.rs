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

pub type DataMaskId = DataId<Resource>;

pub type DataMaskIdIdent = TIdent<Resource, DataMaskId>;
pub type DataMaskIdIdentRaw = TIdentRaw<Resource, DataMaskId>;

pub use kvapi_impl::Resource;

use crate::data_id::DataId;
use crate::tenant::ToTenant;

impl DataMaskIdIdent {
    pub fn new(tenant: impl ToTenant, data_mask_id: u64) -> Self {
        Self::new_generic(tenant, DataMaskId::new(data_mask_id))
    }

    pub fn data_mask_id(&self) -> DataMaskId {
        *self.name()
    }
}

impl DataMaskIdIdentRaw {
    pub fn data_mask_id(&self) -> DataMaskId {
        *self.name()
    }
}

mod kvapi_impl {

    use databend_meta_kvapi::kvapi;

    use crate::data_mask::DataMaskIdIdent;
    use crate::data_mask::DatamaskMeta;
    use crate::tenant_key::resource::TenantResource;

    pub struct Resource;
    impl TenantResource for Resource {
        const PREFIX: &'static str = "__fd_datamask_by_id";
        const TYPE: &'static str = "DataMaskIdIdent";
        const HAS_TENANT: bool = false;
        type ValueType = DatamaskMeta;
    }

    impl kvapi::Value for DatamaskMeta {
        type KeyType = DataMaskIdIdent;
        fn dependency_keys(&self, _key: &Self::KeyType) -> impl IntoIterator<Item = String> {
            []
        }
    }

    // // Use these error types to replace usage of ErrorCode if possible.
    // impl From<ExistError<Resource>> for ErrorCode {
    // impl From<UnknownError<Resource>> for ErrorCode {
}

#[cfg(test)]
mod tests {
    use databend_meta_kvapi::kvapi::Key;

    use super::DataMaskIdIdent;
    use crate::data_mask::DataMaskId;
    use crate::tenant::Tenant;

    #[test]
    fn test_data_mask_id_ident() {
        let tenant = Tenant::new_literal("dummy");
        let ident = DataMaskIdIdent::new_generic(tenant, DataMaskId::new(3));

        let key = ident.to_string_key();
        assert_eq!(key, "__fd_datamask_by_id/3");

        assert_eq!(ident, DataMaskIdIdent::from_str_key(&key).unwrap());
    }

    #[test]
    fn test_data_mask_id_ident_with_key_space() {
        // TODO(xp): implement this test
        // let tenant = Tenant::new_literal("test");
        // let ident = DataMaskIdIdent::new(tenant, 3);
        //
        // let key = ident.to_string_key();
        // assert_eq!(key, "__fd_background_job_by_id/3");
        //
        // assert_eq!(ident, DataMaskIdIdent::from_str_key(&key).unwrap());
    }
}
