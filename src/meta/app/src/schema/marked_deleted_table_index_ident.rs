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

pub type MarkedDeletedTableIndexIdIdent =
    TIdent<MarkedDeletedTableIndexResource, MarkedDeletedTableIndexId>;

pub use kvapi_impl::MarkedDeletedTableIndexResource;

use super::marked_deleted_table_index_id::MarkedDeletedTableIndexId;

mod kvapi_impl {
    use crate::schema::MarkedDeletedIndexMeta;
    use crate::tenant_key::resource::TenantResource;

    /// The meta-service key for storing id of dropped but not vacuumed table index
    pub struct MarkedDeletedTableIndexResource;
    impl TenantResource for MarkedDeletedTableIndexResource {
        const PREFIX: &'static str = "__fd_marked_deleted_table_index";
        const TYPE: &'static str = "MarkedDeletedTableIndexIdent";
        const HAS_TENANT: bool = false;
        type ValueType = MarkedDeletedIndexMeta;
    }
}

#[cfg(test)]
mod tests {
    use databend_meta_kvapi::kvapi::Key;

    use super::MarkedDeletedTableIndexIdIdent;
    use crate::schema::marked_deleted_table_index_id::MarkedDeletedTableIndexId;
    use crate::tenant::Tenant;

    #[test]
    fn test_table_index_id_ident() {
        let tenant = Tenant::new_literal("dummy");
        let ident = MarkedDeletedTableIndexIdIdent::new_generic(
            tenant,
            MarkedDeletedTableIndexId::new(3, "test".to_owned(), "1".to_owned()),
        );

        let key = ident.to_string_key();
        assert_eq!(key, "__fd_marked_deleted_table_index/3/test/1");

        assert_eq!(
            ident,
            MarkedDeletedTableIndexIdIdent::from_str_key(&key).unwrap()
        );
    }
}
