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

pub type DatabaseNameIdent = TIdent<DatabaseNameRsc>;
pub type DatabaseNameIdentRaw = TIdentRaw<DatabaseNameRsc>;

pub use kvapi_impl::DatabaseNameRsc;

impl DatabaseNameIdent {
    pub fn database_name(&self) -> &str {
        self.name()
    }
}

impl DatabaseNameIdentRaw {
    pub fn database_name(&self) -> &str {
        self.name()
    }
}

mod kvapi_impl {

    use databend_meta_kvapi::kvapi;
    use databend_meta_kvapi::kvapi::Key;

    use crate::primitive::Id;
    use crate::schema::DatabaseId;
    use crate::schema::database_name_ident::DatabaseNameIdent;
    use crate::tenant_key::resource::TenantResource;

    pub struct DatabaseNameRsc;
    impl TenantResource for DatabaseNameRsc {
        const PREFIX: &'static str = "__fd_database";
        const TYPE: &'static str = "DatabaseNameIdent";
        const HAS_TENANT: bool = true;
        type ValueType = Id<DatabaseId>;
    }

    impl kvapi::Value for Id<DatabaseId> {
        type KeyType = DatabaseNameIdent;
        fn dependency_keys(&self, _key: &Self::KeyType) -> impl IntoIterator<Item = String> {
            [self.inner().to_string_key()]
        }
    }

    // // Use these error types to replace usage of ErrorCode if possible.
    // impl From<ExistError<Resource>> for ErrorCode {
    // impl From<UnknownError<Resource>> for ErrorCode {
}

#[cfg(test)]
mod tests {
    use databend_meta_kvapi::kvapi::Key;

    use super::DatabaseNameIdent;
    use crate::tenant::Tenant;

    #[test]
    fn test_ident() {
        let tenant = Tenant::new_literal("test");
        let ident = DatabaseNameIdent::new(tenant, "test1");

        let key = ident.to_string_key();
        assert_eq!(key, "__fd_database/test/test1");

        assert_eq!(ident, DatabaseNameIdent::from_str_key(&key).unwrap());
    }
}
