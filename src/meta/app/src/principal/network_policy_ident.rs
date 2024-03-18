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

/// Defines the meta-service key for network policy.
pub type NetworkPolicyIdent = TIdent<Resource>;

pub use kvapi_impl::Resource;

mod kvapi_impl {
    use std::fmt::Display;

    use databend_common_exception::ErrorCode;
    use databend_common_meta_kvapi::kvapi;

    use crate::principal::NetworkPolicy;
    use crate::tenant::Tenant;
    use crate::tenant_key::TenantResource;

    pub struct Resource;
    impl TenantResource for Resource {
        const PREFIX: &'static str = "__fd_network_policies";
        type ValueType = NetworkPolicy;
        type UnknownError = ErrorCode;

        fn error_unknown<D: Display>(
            _tenant: &Tenant,
            name: &str,
            ctx: impl FnOnce() -> D,
        ) -> Self::UnknownError {
            ErrorCode::UnknownNetworkPolicy(format!("Unknown network policy '{name}': {}", ctx()))
        }

        type ExistError = ErrorCode;

        fn error_exist<D: Display>(
            _tenant: &Tenant,
            name: &str,
            ctx: impl FnOnce() -> D,
        ) -> Self::ExistError {
            ErrorCode::NetworkPolicyAlreadyExists(format!(
                "Network policy '{name}' already exists: {}",
                ctx()
            ))
        }
    }

    impl kvapi::Value for NetworkPolicy {
        fn dependency_keys(&self) -> impl IntoIterator<Item = String> {
            []
        }
    }

    impl kvapi::ValueWithName for NetworkPolicy {
        fn name(&self) -> &str {
            &self.name
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_common_meta_kvapi::kvapi::Key;

    use crate::principal::network_policy_ident::NetworkPolicyIdent;
    use crate::tenant::Tenant;

    #[test]
    fn test_network_policy_ident() {
        let tenant = Tenant::new("test".to_string());
        let ident = NetworkPolicyIdent::new(tenant, "test1");

        let key = ident.to_string_key();
        assert_eq!(key, "__fd_network_policies/test/test1");

        assert_eq!(ident, NetworkPolicyIdent::from_str_key(&key).unwrap());
    }
}
