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

use crate::tenant::Tenant;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct PasswordPolicyIdent {
    tenant: Tenant,
    name: String,
}

impl PasswordPolicyIdent {
    pub fn new(tenant: Tenant, name: impl ToString) -> Self {
        Self {
            tenant,
            name: name.to_string(),
        }
    }
}

mod kvapi_key_impl {
    use databend_common_meta_kvapi::kvapi;
    use databend_common_meta_kvapi::kvapi::KeyError;

    use crate::principal::password_policy_ident::PasswordPolicyIdent;
    use crate::principal::PasswordPolicy;
    use crate::tenant::Tenant;
    use crate::KeyWithTenant;

    impl kvapi::Key for PasswordPolicyIdent {
        const PREFIX: &'static str = "__fd_password_policies";
        type ValueType = PasswordPolicy;

        fn parent(&self) -> Option<String> {
            Some(self.tenant.to_string_key())
        }

        fn encode_key(&self, b: kvapi::KeyBuilder) -> kvapi::KeyBuilder {
            b.push_str(self.tenant_name()).push_str(&self.name)
        }

        fn decode_key(p: &mut kvapi::KeyParser) -> Result<Self, KeyError> {
            let tenant = p.next_nonempty()?;
            let name = p.next_str()?;

            Ok(PasswordPolicyIdent::new(Tenant::new_nonempty(tenant), name))
        }
    }

    impl KeyWithTenant for PasswordPolicyIdent {
        fn tenant(&self) -> &Tenant {
            &self.tenant
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
