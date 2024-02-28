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

/// The identifier of a role.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct RoleIdent {
    tenant: Tenant,
    role_name: String,
}

impl RoleIdent {
    pub fn new(tenant: Tenant, role_name: impl ToString) -> RoleIdent {
        RoleIdent {
            tenant,
            role_name: role_name.to_string(),
        }
    }

    pub fn role_name(&self) -> &str {
        &self.role_name
    }
}

mod kvapi_key_impl {
    use databend_common_meta_kvapi::kvapi;

    use crate::principal::role_ident::RoleIdent;
    use crate::principal::RoleInfo;
    use crate::tenant::Tenant;
    use crate::KeyWithTenant;

    impl kvapi::Key for RoleIdent {
        const PREFIX: &'static str = "__fd_roles";
        type ValueType = RoleInfo;

        fn parent(&self) -> Option<String> {
            Some(self.tenant.to_string_key())
        }

        fn to_string_key(&self) -> String {
            kvapi::KeyBuilder::new_prefixed(Self::PREFIX)
                .push_str(self.tenant.name())
                .push_str(&self.role_name)
                .done()
        }

        fn from_str_key(s: &str) -> Result<Self, kvapi::KeyError> {
            let mut p = kvapi::KeyParser::new_prefixed(s, Self::PREFIX)?;
            let tenant = p.next_str()?;
            let role_name = p.next_str()?;
            p.done()?;

            Ok(RoleIdent::new(Tenant::new(tenant), role_name))
        }
    }

    impl kvapi::Value for RoleInfo {
        fn dependency_keys(&self) -> impl IntoIterator<Item = String> {
            []
        }
    }

    impl KeyWithTenant for RoleIdent {
        fn tenant(&self) -> &Tenant {
            &self.tenant
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_common_meta_kvapi::kvapi::Key;

    use crate::principal::role_ident::RoleIdent;
    use crate::tenant::Tenant;
    use crate::KeyWithTenant;

    #[test]
    fn test_role_ident_tenant_prefix() {
        let r = RoleIdent::new(Tenant::new("tenant"), "role");
        assert_eq!("__fd_roles/tenant/", r.tenant_prefix());
    }

    #[test]
    fn test_role_ident_key() {
        let r = RoleIdent::new(Tenant::new("tenant"), "role");
        assert_eq!("__fd_roles/tenant/role", r.to_string_key());
        assert_eq!(Ok(r), RoleIdent::from_str_key("__fd_roles/tenant/role"));
    }
}
