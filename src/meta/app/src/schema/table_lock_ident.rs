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

use databend_meta_kvapi::kvapi;

use crate::tenant_key::ident::TIdent;
use crate::tenant_key::raw::TIdentRaw;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableIdRevision {
    table_id: u64,
    revision: u64,
}

impl kvapi::KeyCodec for TableIdRevision {
    fn encode_key(&self, b: kvapi::KeyBuilder) -> kvapi::KeyBuilder {
        b.push_u64(self.table_id).push_u64(self.revision)
    }

    fn decode_key(p: &mut kvapi::KeyParser) -> Result<Self, kvapi::KeyError> {
        let table_id = p.next_u64()?;
        let revision = p.next_u64()?;

        Ok(Self { table_id, revision })
    }
}

/// The identifier of a Table lock,
/// which is used as a key and does not support other codec method such as serde.
pub type TableLockIdent = TIdent<Resource, TableIdRevision>;
pub type TableLockIdentRaw = TIdentRaw<Resource, TableIdRevision>;

pub use kvapi_impl::Resource;

use crate::tenant::ToTenant;

impl TableLockIdent {
    pub fn new(tenant: impl ToTenant, table_id: u64, revision: u64) -> Self {
        Self::new_generic(tenant, TableIdRevision { table_id, revision })
    }

    pub fn table_id(&self) -> u64 {
        self.name().table_id
    }

    pub fn revision(&self) -> u64 {
        self.name().revision
    }
}

mod kvapi_impl {

    use databend_meta_kvapi::kvapi;

    use crate::schema::LockMeta;
    use crate::schema::TableLockIdent;
    use crate::tenant_key::resource::TenantResource;

    pub struct Resource;
    impl TenantResource for Resource {
        const PREFIX: &'static str = "__fd_table_lock";
        const TYPE: &'static str = "TableLockIdent";
        const HAS_TENANT: bool = true;
        type ValueType = LockMeta;
    }

    impl kvapi::Value for LockMeta {
        type KeyType = TableLockIdent;
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

    use super::TableLockIdent;
    use crate::tenant::Tenant;

    #[test]
    fn test_catalog_name_ident() {
        let tenant = Tenant::new_literal("test");
        let ident = TableLockIdent::new(tenant, 5, 6);

        let key = ident.to_string_key();
        assert_eq!(key, "__fd_table_lock/test/5/6");

        assert_eq!(ident, TableLockIdent::from_str_key(&key).unwrap());
    }
}
