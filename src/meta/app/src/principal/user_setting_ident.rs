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

/// Define the meta-service key for a user setting.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SettingIdent {
    pub tenant: Tenant,
    pub name: String,
}

impl SettingIdent {
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

    use crate::principal::user_setting_ident::SettingIdent;
    use crate::principal::UserSetting;
    use crate::tenant::Tenant;
    use crate::KeyWithTenant;

    impl kvapi::Key for SettingIdent {
        const PREFIX: &'static str = "__fd_settings";
        type ValueType = UserSetting;

        fn parent(&self) -> Option<String> {
            Some(self.tenant.to_string_key())
        }

        fn encode_key(&self, b: kvapi::KeyBuilder) -> kvapi::KeyBuilder {
            b.push_str(self.tenant_name()).push_str(&self.name)
        }

        fn decode_key(p: &mut kvapi::KeyParser) -> Result<Self, KeyError> {
            let tenant = p.next_nonempty()?;
            let name = p.next_str()?;

            Ok(SettingIdent::new(Tenant::new_nonempty(tenant), name))
        }
    }

    impl KeyWithTenant for SettingIdent {
        fn tenant(&self) -> &Tenant {
            &self.tenant
        }
    }

    impl kvapi::Value for UserSetting {
        fn dependency_keys(&self) -> impl IntoIterator<Item = String> {
            []
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_common_meta_kvapi::kvapi::Key;

    use crate::principal::user_setting_ident::SettingIdent;
    use crate::tenant::Tenant;

    #[test]
    fn test_setting_ident() {
        let tenant = Tenant::new("tenant1");
        let ident = SettingIdent::new(tenant.clone(), "test");
        assert_eq!(tenant, ident.tenant);
        assert_eq!("test", ident.name);
        assert_eq!("__fd_settings/tenant1/test", ident.to_string_key());

        let got = SettingIdent::from_str_key(&ident.to_string_key()).unwrap();
        assert_eq!(ident, got);
    }
}
