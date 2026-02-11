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

/// The key of the list of database ids that have been used in history by a database name.
pub type DatabaseIdHistoryIdent = TIdent<DatabaseIdHistoryRsc>;
pub type DatabaseIdHistoryIdentRaw = TIdentRaw<DatabaseIdHistoryRsc>;

pub use kvapi_impl::DatabaseIdHistoryRsc;

use crate::tenant_key::raw::TIdentRaw;

impl DatabaseIdHistoryIdent {
    pub fn database_name(&self) -> &str {
        self.name().as_str()
    }
}

impl DatabaseIdHistoryIdentRaw {
    pub fn database_name(&self) -> &str {
        self.name().as_str()
    }
}

mod kvapi_impl {

    use databend_meta_kvapi::kvapi;
    use databend_meta_kvapi::kvapi::Key;

    use crate::schema::DatabaseId;
    use crate::schema::DatabaseIdHistoryIdent;
    use crate::schema::DbIdList;
    use crate::tenant_key::resource::TenantResource;

    pub struct DatabaseIdHistoryRsc;
    impl TenantResource for DatabaseIdHistoryRsc {
        const PREFIX: &'static str = "__fd_db_id_list";
        const TYPE: &'static str = "DatabaseIdHistoryIdent";
        const HAS_TENANT: bool = true;
        type ValueType = DbIdList;
    }

    impl kvapi::Value for DbIdList {
        type KeyType = DatabaseIdHistoryIdent;
        fn dependency_keys(&self, _key: &Self::KeyType) -> impl IntoIterator<Item = String> {
            self.id_list
                .iter()
                .map(|id| DatabaseId::new(*id).to_string_key())
        }
    }

    // // Use these error types to replace usage of ErrorCode if possible.
    // impl From<ExistError<Resource>> for ErrorCode {
    // impl From<UnknownError<Resource>> for ErrorCode {
}

#[cfg(test)]
mod tests {
    use databend_meta_kvapi::kvapi::Key;

    use super::DatabaseIdHistoryIdent;
    use crate::tenant::Tenant;

    #[test]
    fn test_database_id_history_ident() {
        let tenant = Tenant::new_literal("test");
        let ident = DatabaseIdHistoryIdent::new(tenant, "3");

        let key = ident.to_string_key();
        assert_eq!(key, "__fd_db_id_list/test/3");

        assert_eq!(ident, DatabaseIdHistoryIdent::from_str_key(&key).unwrap());
    }
}
