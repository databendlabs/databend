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

pub type TagId = DataId<Resource>;
pub type TagIdIdent = TIdent<Resource, TagId>;
pub type TagIdIdentRaw = TIdentRaw<Resource, TagId>;

pub use kvapi_impl::Resource;

mod kvapi_impl {
    use databend_common_meta_kvapi::kvapi;

    use crate::schema::TagIdIdent;
    use crate::schema::TagMeta;
    use crate::tenant_key::resource::TenantResource;

    pub struct Resource;
    impl TenantResource for Resource {
        const PREFIX: &'static str = "__fd_tag_by_id";
        const TYPE: &'static str = "TagIdIdent";
        const HAS_TENANT: bool = false;
        type ValueType = TagMeta;
    }

    impl kvapi::Value for TagMeta {
        type KeyType = TagIdIdent;

        fn dependency_keys(&self, _key: &Self::KeyType) -> impl IntoIterator<Item = String> {
            []
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_common_meta_kvapi::kvapi::Key;

    use super::TagId;
    use super::TagIdIdent;
    use crate::tenant::Tenant;

    #[test]
    fn test_tag_id_ident_roundtrip() {
        let tenant = Tenant::new_literal("t");
        let ident = TagIdIdent::new_generic(tenant, TagId::new(42));
        let key = ident.to_string_key();
        assert_eq!("__fd_tag_by_id/42", key);
        assert_eq!(ident, TagIdIdent::from_str_key(&key).unwrap());
    }
}
