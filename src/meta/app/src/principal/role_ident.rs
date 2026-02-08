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

pub type RoleIdent = TIdent<Resource>;

/// Share name as value.
pub type RoleIdentRaw = TIdentRaw<Resource>;

pub use kvapi_impl::Resource;

impl RoleIdent {
    pub fn role_name(&self) -> &str {
        self.name()
    }
}

impl RoleIdentRaw {
    pub fn role_name(&self) -> &str {
        self.name()
    }
}

mod kvapi_impl {

    use databend_meta_kvapi::kvapi;

    use crate::principal::RoleIdent;
    use crate::principal::RoleInfo;
    use crate::tenant_key::resource::TenantResource;

    pub struct Resource;
    impl TenantResource for Resource {
        const PREFIX: &'static str = "__fd_roles";
        const TYPE: &'static str = "RoleIdent";
        const HAS_TENANT: bool = true;
        type ValueType = RoleInfo;
    }

    impl kvapi::Value for RoleInfo {
        type KeyType = RoleIdent;

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

    use super::RoleIdent;
    use crate::tenant::Tenant;

    #[test]
    fn test_ident() {
        let tenant = Tenant::new_literal("test");
        let ident = RoleIdent::new(tenant, "test1");

        let key = ident.to_string_key();
        assert_eq!(key, "__fd_roles/test/test1");

        assert_eq!(ident, RoleIdent::from_str_key(&key).unwrap());
    }
}
