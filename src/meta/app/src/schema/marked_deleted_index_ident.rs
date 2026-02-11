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

pub type MarkedDeletedIndexIdIdent = TIdent<MarkedDeletedIndexResource, MarkedDeletedIndexId>;

pub use kvapi_impl::MarkedDeletedIndexResource;

use super::marked_deleted_index_id::MarkedDeletedIndexId;

mod kvapi_impl {

    use databend_meta_kvapi::kvapi;

    use crate::schema::MarkedDeletedIndexMeta;
    use crate::schema::marked_deleted_index_ident::MarkedDeletedIndexIdIdent;
    use crate::tenant_key::resource::TenantResource;

    /// The meta-service key for storing id of dropped but not vacuumed index
    pub struct MarkedDeletedIndexResource;
    impl TenantResource for MarkedDeletedIndexResource {
        const PREFIX: &'static str = "__fd_marked_deleted_index";
        const TYPE: &'static str = "MarkedDeletedIndexIdent";
        const HAS_TENANT: bool = false;
        type ValueType = MarkedDeletedIndexMeta;
    }

    impl kvapi::Value for MarkedDeletedIndexMeta {
        type KeyType = MarkedDeletedIndexIdIdent;

        fn dependency_keys(&self, _key: &Self::KeyType) -> impl IntoIterator<Item = String> {
            []
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_meta_kvapi::kvapi::Key;

    use super::MarkedDeletedIndexIdIdent;
    use crate::schema::marked_deleted_index_ident::MarkedDeletedIndexId;
    use crate::tenant::Tenant;

    #[test]
    fn test_index_id_ident() {
        let tenant = Tenant::new_literal("dummy");
        let ident = MarkedDeletedIndexIdIdent::new_generic(tenant, MarkedDeletedIndexId::new(3, 4));

        let key = ident.to_string_key();
        assert_eq!(key, "__fd_marked_deleted_index/3/4");

        assert_eq!(
            ident,
            MarkedDeletedIndexIdIdent::from_str_key(&key).unwrap()
        );
    }
}
