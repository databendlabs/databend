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

/// Tag name as meta-service key.
pub type TagNameIdent = TIdent<Resource>;

/// Tag name stored as value.
pub type TagNameIdentRaw = TIdentRaw<Resource>;

pub use kvapi_impl::Resource;

impl TagNameIdent {
    pub fn tag_name(&self) -> &str {
        self.name()
    }
}

impl TagNameIdentRaw {
    pub fn tag_name(&self) -> &str {
        self.name()
    }
}

mod kvapi_impl {
    use databend_meta_kvapi::kvapi;
    use databend_meta_kvapi::kvapi::Key;

    use super::super::id_ident::TagId;
    use crate::key_with_tenant::KeyWithTenant;
    use crate::schema::TagNameIdent;
    use crate::tenant_key::resource::TenantResource;

    pub struct Resource;
    impl TenantResource for Resource {
        const PREFIX: &'static str = "__fd_tag";
        const TYPE: &'static str = "TagNameIdent";
        const HAS_TENANT: bool = true;
        type ValueType = TagId;
    }

    impl kvapi::Value for TagId {
        type KeyType = TagNameIdent;

        fn dependency_keys(&self, key: &Self::KeyType) -> impl IntoIterator<Item = String> {
            [self.into_t_ident(key.tenant()).to_string_key()]
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_meta_kvapi::kvapi::Key;

    use super::TagNameIdent;
    use crate::tenant::Tenant;

    #[test]
    fn test_tag_name_ident() {
        let tenant = Tenant::new_literal("tenant_a");
        let ident = TagNameIdent::new(tenant, "pii_flag");

        let key = ident.to_string_key();
        assert_eq!("__fd_tag/tenant_a/pii_flag", key);
        assert_eq!(ident, TagNameIdent::from_str_key(&key).unwrap());
    }
}
