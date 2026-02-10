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

/// Index name as meta-service key
pub type IndexNameIdent = TIdent<IndexName>;

/// Index name as value.
pub type IndexNameIdentRaw = TIdentRaw<IndexName>;

pub use kvapi_impl::IndexName;

impl TIdent<IndexName> {
    pub fn index_name(&self) -> &str {
        self.name()
    }
}

impl TIdentRaw<IndexName> {
    pub fn index_name(&self) -> &str {
        self.name()
    }
}

mod kvapi_impl {
    use databend_meta_kvapi::kvapi;
    use databend_meta_kvapi::kvapi::Key;

    use crate::KeyWithTenant;
    use crate::schema::IndexNameIdent;
    use crate::schema::index_id_ident::IndexId;
    use crate::tenant_key::resource::TenantResource;

    pub struct IndexName;
    impl TenantResource for IndexName {
        const PREFIX: &'static str = "__fd_index";
        const HAS_TENANT: bool = true;
        type ValueType = IndexId;
    }

    impl kvapi::Value for IndexId {
        type KeyType = IndexNameIdent;
        /// IndexId is id of the two level `name->id,id->value` mapping
        fn dependency_keys(&self, key: &Self::KeyType) -> impl IntoIterator<Item = String> {
            [self.into_t_ident(key.tenant()).to_string_key()]
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_meta_kvapi::kvapi::Key;

    use super::IndexNameIdent;
    use crate::tenant::Tenant;

    #[test]
    fn test_ident() {
        let tenant = Tenant::new_literal("test");
        let ident = IndexNameIdent::new(tenant, "test1");

        let key = ident.to_string_key();
        assert_eq!(key, "__fd_index/test/test1");

        assert_eq!(ident, IndexNameIdent::from_str_key(&key).unwrap());
    }
}
