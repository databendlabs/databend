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

use super::id_ident::TagId;
use crate::tenant_key::ident::TIdent;
use crate::tenant_key::raw::TIdentRaw;

/// Reverse lookup from tag ID to tag name.
///
/// Key format: `__fd_tag_id_to_name/<tenant>/<tag_id>` -> `TagNameIdentRaw`
///
/// This enables efficient tag name lookup by ID without scanning all tags.
pub type TagIdToNameIdent = TIdent<Resource, TagId>;
pub type TagIdToNameIdentRaw = TIdentRaw<Resource, TagId>;

pub use kvapi_impl::Resource;

mod kvapi_impl {
    use databend_meta_kvapi::kvapi;

    use super::super::name_ident::TagNameIdentRaw;
    use crate::schema::TagIdToNameIdent;
    use crate::tenant_key::resource::TenantResource;

    pub struct Resource;
    impl TenantResource for Resource {
        const PREFIX: &'static str = "__fd_tag_id_to_name";
        const TYPE: &'static str = "TagIdToNameIdent";
        const HAS_TENANT: bool = true;
        type ValueType = TagNameIdentRaw;
    }

    impl kvapi::Value for TagNameIdentRaw {
        type KeyType = TagIdToNameIdent;

        fn dependency_keys(&self, _key: &Self::KeyType) -> impl IntoIterator<Item = String> {
            []
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_meta_kvapi::kvapi::Key;

    use super::TagId;
    use super::TagIdToNameIdent;
    use crate::tenant::Tenant;

    #[test]
    fn test_tag_id_to_name_ident() {
        let tenant = Tenant::new_literal("t");
        let ident = TagIdToNameIdent::new_generic(tenant, TagId::new(42));
        let key = ident.to_string_key();
        assert_eq!("__fd_tag_id_to_name/t/42", key);
        assert_eq!(ident, TagIdToNameIdent::from_str_key(&key).unwrap());
    }
}
