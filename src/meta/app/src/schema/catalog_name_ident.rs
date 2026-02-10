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

/// The identifier of a catalog,
/// which is used as a key and does not support other codec method such as serde.
pub type CatalogNameIdent = TIdent<CatalogNameRsc>;
pub type CatalogNameIdentRaw = TIdentRaw<CatalogNameRsc>;

pub use kvapi_impl::CatalogNameRsc;

use crate::tenant_key::raw::TIdentRaw;

mod kvapi_impl {

    use databend_meta_kvapi::kvapi;
    use databend_meta_kvapi::kvapi::Key;

    use crate::KeyWithTenant;
    use crate::schema::CatalogNameIdent;
    use crate::schema::catalog_id_ident::CatalogId;
    use crate::tenant_key::resource::TenantResource;

    pub struct CatalogNameRsc;
    impl TenantResource for CatalogNameRsc {
        const PREFIX: &'static str = "__fd_catalog";
        const TYPE: &'static str = "CatalogNameIdent";
        const HAS_TENANT: bool = true;
        type ValueType = CatalogId;
    }

    impl kvapi::Value for CatalogId {
        type KeyType = CatalogNameIdent;
        fn dependency_keys(&self, key: &Self::KeyType) -> impl IntoIterator<Item = String> {
            [self.into_t_ident(key.tenant()).to_string_key()]
        }
    }

    // // Use these error types to replace usage of ErrorCode if possible.
    // impl From<ExistError<Resource>> for ErrorCode {
    // impl From<UnknownError<Resource>> for ErrorCode {
}

#[cfg(test)]
mod tests {
    use databend_meta_kvapi::kvapi::Key;

    use super::CatalogNameIdent;
    use crate::tenant::Tenant;

    #[test]
    fn test_catalog_name_ident() {
        let tenant = Tenant::new_literal("test");
        let ident = CatalogNameIdent::new(tenant, "test1");

        let key = ident.to_string_key();
        assert_eq!(key, "__fd_catalog/test/test1");

        assert_eq!(ident, CatalogNameIdent::from_str_key(&key).unwrap());
    }
}
