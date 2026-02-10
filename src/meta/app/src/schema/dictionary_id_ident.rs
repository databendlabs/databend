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

use crate::data_id::DataId;
use crate::tenant::ToTenant;
use crate::tenant_key::ident::TIdent;

pub type DictionaryId = DataId<DictionaryIdRsc>;

pub type DictionaryIdIdent = TIdent<DictionaryIdRsc, DictionaryId>;

pub use kvapi_impl::DictionaryIdRsc;

impl DictionaryIdIdent {
    pub fn new(tenant: impl ToTenant, catalog_id: u64) -> Self {
        Self::new_generic(tenant, DictionaryId::new(catalog_id))
    }

    pub fn catalog_id(&self) -> DictionaryId {
        *self.name()
    }
}

mod kvapi_impl {

    use databend_meta_kvapi::kvapi;

    use crate::schema::DictionaryMeta;
    use crate::schema::dictionary_id_ident::DictionaryIdIdent;
    use crate::tenant_key::resource::TenantResource;

    pub struct DictionaryIdRsc;
    impl TenantResource for DictionaryIdRsc {
        const PREFIX: &'static str = "__fd_dictionary_by_id";
        const TYPE: &'static str = "DictionaryId";
        const HAS_TENANT: bool = false;
        type ValueType = DictionaryMeta;
    }

    impl kvapi::Value for DictionaryMeta {
        type KeyType = DictionaryIdIdent;

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

    use super::DictionaryId;
    use super::DictionaryIdIdent;
    use crate::tenant::Tenant;

    #[test]
    fn test_dictionary_id_ident() {
        let tenant = Tenant::new_literal("dummy");
        let ident = DictionaryIdIdent::new_generic(tenant, DictionaryId::new(3));

        let key = ident.to_string_key();
        assert_eq!(key, "__fd_dictionary_by_id/3");
        assert_eq!(ident, DictionaryIdIdent::from_str_key(&key).unwrap());
    }

    #[test]
    fn test_dictionary_id_ident_with_key_space() {
        // TODO(xp): implement this test
        // let tenant = Tenant::new_literal("test");
        // let ident = DictionaryIdIdent::new(tenant, 3);
        //
        // let key = ident.to_string_key();
        // assert_eq!(key, "__fd_catalog_by_id/3");
        //
        // assert_eq!(ident, DictionaryIdIdent::from_str_key(&key).unwrap());
    }
}
