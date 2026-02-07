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

pub type LeastVisibleTimeIdent = TIdent<LeastVisibleTimeRsc, u64>;

pub use kvapi_impl::LeastVisibleTimeRsc;

impl LeastVisibleTimeIdent {
    pub fn table_id(&self) -> u64 {
        *self.name()
    }
}

mod kvapi_impl {
    use databend_meta_kvapi::kvapi;

    use crate::schema::LeastVisibleTime;
    use crate::schema::least_visible_time_ident::LeastVisibleTimeIdent;
    use crate::tenant_key::resource::TenantResource;

    pub struct LeastVisibleTimeRsc;
    impl TenantResource for LeastVisibleTimeRsc {
        const PREFIX: &'static str = "__fd_table_lvt";
        const HAS_TENANT: bool = false;
        type ValueType = LeastVisibleTime;
    }

    // TODO: kvapi::Key::parent() for LeastVisibleTimeIdent should be the table id
    impl kvapi::Value for LeastVisibleTime {
        type KeyType = LeastVisibleTimeIdent;
        /// IndexId is id of the two level `name->id,id->value` mapping
        fn dependency_keys(&self, _key: &Self::KeyType) -> impl IntoIterator<Item = String> {
            []
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_meta_kvapi::kvapi::Key;

    use super::LeastVisibleTimeIdent;
    use crate::tenant::Tenant;

    #[test]
    fn test_ident() {
        let tenant = Tenant::new_literal("dummy");
        let ident = LeastVisibleTimeIdent::new(tenant, 3);

        let key = ident.to_string_key();
        assert_eq!(key, "__fd_table_lvt/3");

        assert_eq!(ident, LeastVisibleTimeIdent::from_str_key(&key).unwrap());
    }
}
