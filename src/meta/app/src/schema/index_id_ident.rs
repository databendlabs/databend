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
use crate::tenant_key::ident::TIdent;
use crate::tenant_key::raw::TIdentRaw;

pub type IndexId = DataId<IndexIdResource>;

pub type IndexIdIdent = TIdent<IndexIdResource, IndexId>;
pub type IndexIdIdentRaw = TIdentRaw<IndexIdResource, IndexId>;

pub use kvapi_impl::IndexIdResource;

mod kvapi_impl {

    use databend_meta_kvapi::kvapi;

    use crate::schema::IndexMeta;
    use crate::schema::index_id_ident::IndexIdIdent;
    use crate::tenant_key::resource::TenantResource;

    pub struct IndexIdResource;
    impl TenantResource for IndexIdResource {
        const PREFIX: &'static str = "__fd_index_by_id";
        const TYPE: &'static str = "IndexIdIdent";
        const HAS_TENANT: bool = false;
        type ValueType = IndexMeta;
    }

    impl kvapi::Value for IndexMeta {
        type KeyType = IndexIdIdent;

        // todo!("IndexId being parent of IndexIdToName can be described with dependency_keys")
        fn dependency_keys(&self, _key: &Self::KeyType) -> impl IntoIterator<Item = String> {
            []
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_meta_kvapi::kvapi::Key;

    use super::IndexId;
    use super::IndexIdIdent;
    use crate::tenant::Tenant;

    #[test]
    fn test_index_id_ident() {
        let tenant = Tenant::new_literal("dummy");
        let ident = IndexIdIdent::new_generic(tenant, IndexId::new(3));

        let key = ident.to_string_key();
        assert_eq!(key, "__fd_index_by_id/3");

        assert_eq!(ident, IndexIdIdent::from_str_key(&key).unwrap());
    }

    #[test]
    fn test_background_job_id_ident_with_key_space() {
        // TODO(xp): implement this test
        // let tenant = Tenant::new_literal("test");
        // let ident = IndexIdIdent::new(tenant, 3);
        //
        // let key = ident.to_string_key();
        // assert_eq!(key, "__fd_catalog_by_id/3");
        //
        // assert_eq!(ident, IndexIdIdent::from_str_key(&key).unwrap());
    }
}
