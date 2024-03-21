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

use std::fmt;

use databend_common_meta_kvapi::kvapi;

use crate::tenant::Tenant;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Eq, PartialEq, Hash, Default)]
pub struct UserIdentity {
    pub username: String,
    pub hostname: String,
}

impl UserIdentity {
    const ESCAPE_CHARS: [u8; 2] = [b'\'', b'@'];

    pub fn new(name: &str, host: &str) -> Self {
        Self {
            username: name.to_string(),
            hostname: host.to_string(),
        }
    }

    /// Format into form `'username'@'hostname'`.
    pub fn format(&self) -> String {
        format!(
            "'{}'@'{}'",
            kvapi::KeyBuilder::escape_specified(&self.username, &Self::ESCAPE_CHARS),
            kvapi::KeyBuilder::escape_specified(&self.hostname, &Self::ESCAPE_CHARS),
        )
    }

    pub fn parse(s: &str) -> Result<Self, kvapi::KeyError> {
        let parts = s.splitn(2, '@').collect::<Vec<&str>>();
        if parts.len() != 2 {
            return Err(kvapi::KeyError::WrongNumberOfSegments {
                expect: 2,
                got: s.to_string(),
            });
        }

        // trim single quotes
        let username =
            kvapi::KeyParser::unescape_specified(parts[0].trim_matches('\''), &Self::ESCAPE_CHARS)?;
        let hostname =
            kvapi::KeyParser::unescape_specified(parts[1].trim_matches('\''), &Self::ESCAPE_CHARS)?;

        Ok(UserIdentity { username, hostname })
    }
}

impl fmt::Display for UserIdentity {
    fn fmt(&self, f: &mut fmt::Formatter) -> std::result::Result<(), fmt::Error> {
        write!(f, "'{}'@'{}'", self.username, self.hostname)
    }
}

/// A user identity belonging to a tenant.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct TenantUserIdent {
    pub tenant: Tenant,
    pub user: UserIdentity,
}

impl TenantUserIdent {
    pub fn new(tenant: Tenant, user: UserIdentity) -> Self {
        Self { tenant, user }
    }

    pub fn new_user_host(tenant: Tenant, user: &str, host: &str) -> Self {
        Self {
            tenant,
            user: UserIdentity::new(user, host),
        }
    }
}

mod kvapi_key_impl {
    use databend_common_meta_kvapi::kvapi;
    use databend_common_meta_kvapi::kvapi::KeyError;

    use crate::principal::user_identity::TenantUserIdent;
    use crate::principal::UserIdentity;
    use crate::principal::UserInfo;
    use crate::tenant::Tenant;
    use crate::KeyWithTenant;

    impl kvapi::Key for TenantUserIdent {
        const PREFIX: &'static str = "__fd_users";
        type ValueType = UserInfo;

        fn parent(&self) -> Option<String> {
            Some(self.tenant.to_string_key())
        }

        fn to_string_key(&self) -> String {
            kvapi::KeyBuilder::new_prefixed(Self::PREFIX)
                .push_str(self.tenant_name())
                .push_str(&self.user.format())
                .done()
        }

        fn from_str_key(s: &str) -> Result<Self, KeyError> {
            let mut p = kvapi::KeyParser::new_prefixed(s, Self::PREFIX)?;
            let tenant = p.next_nonempty()?;
            let user_str = p.next_str()?;
            p.done()?;

            let user = UserIdentity::parse(&user_str)?;
            Ok(TenantUserIdent {
                tenant: Tenant::new_nonempty(tenant),
                user,
            })
        }
    }

    impl KeyWithTenant for TenantUserIdent {
        fn tenant(&self) -> &Tenant {
            &self.tenant
        }
    }

    impl kvapi::Value for UserInfo {
        fn dependency_keys(&self) -> impl IntoIterator<Item = String> {
            []
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_common_meta_kvapi::kvapi::Key;
    use databend_common_meta_types::NonEmptyString;

    use crate::principal::user_identity::TenantUserIdent;
    use crate::principal::UserIdentity;
    use crate::tenant::Tenant;

    fn test_format_parse(user: &str, host: &str, expect: &str) {
        let tenant = Tenant::new_nonempty(NonEmptyString::new("test_tenant").unwrap());
        let user_ident = UserIdentity::new(user, host);
        let tenant_user_ident = TenantUserIdent {
            tenant,
            user: user_ident,
        };

        let key = tenant_user_ident.to_string_key();
        assert_eq!(key, expect, "'{user}' '{host}' '{expect}'");

        let tenant_user_ident_parsed = TenantUserIdent::from_str_key(&key).unwrap();
        assert_eq!(
            tenant_user_ident, tenant_user_ident_parsed,
            "'{user}' '{host}' '{expect}'"
        );
    }

    #[test]
    fn test_tenant_user_ident_as_kvapi_key() {
        test_format_parse("user", "%", "__fd_users/test_tenant/%27user%27%40%27%25%27");
        test_format_parse(
            "user'",
            "%",
            "__fd_users/test_tenant/%27user%2527%27%40%27%25%27",
        );

        // With correct encoding the following two pair should not be encoded into the same string.

        test_format_parse(
            "u'@'h",
            "h",
            "__fd_users/test_tenant/%27u%2527%2540%2527h%27%40%27h%27",
        );
        test_format_parse(
            "u",
            "h'@'h",
            "__fd_users/test_tenant/%27u%27%40%27h%2527%2540%2527h%27",
        );
    }
}
