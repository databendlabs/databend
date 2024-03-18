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

use crate::tenant_key::TIdent;

/// Defines the meta-service key for password policy.
pub type PasswordPolicyIdent = TIdent<kvapi_impl::Resource>;

mod kvapi_impl {
    use std::fmt::Display;

    use databend_common_exception::ErrorCode;
    use databend_common_meta_kvapi::kvapi;

    use crate::principal::PasswordPolicy;
    use crate::tenant::Tenant;
    use crate::tenant_key::TenantResource;

    pub struct Resource;

    impl TenantResource for Resource {
        const PREFIX: &'static str = "__fd_password_policies";
        type ValueType = PasswordPolicy;

        type UnknownError = ErrorCode;

        fn error_unknown<D: Display>(
            _tenant: &Tenant,
            _name: &str,
            _ctx: impl FnOnce() -> D,
        ) -> Self::UnknownError {
            todo!()
        }

        type ExistError = ErrorCode;

        fn error_exist<D: Display>(
            _tenant: &Tenant,
            _name: &str,
            _ctx: impl FnOnce() -> D,
        ) -> Self::ExistError {
            todo!()
        }
    }

    impl kvapi::Value for PasswordPolicy {
        fn dependency_keys(&self) -> impl IntoIterator<Item = String> {
            []
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_common_meta_kvapi::kvapi::Key;

    use crate::principal::password_policy_ident::PasswordPolicyIdent;
    use crate::tenant::Tenant;
    #[test]
    fn test_password_policy_ident() {
        let tenant = Tenant::new("test");
        let ident = PasswordPolicyIdent::new(tenant.clone(), "test2");

        assert_eq!(ident.to_string_key(), "__fd_password_policies/test/test2");
        assert_eq!(
            ident,
            PasswordPolicyIdent::from_str_key("__fd_password_policies/test/test2").unwrap()
        );
    }
}
