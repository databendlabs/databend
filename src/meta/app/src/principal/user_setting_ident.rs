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

/// Define the meta-service key for a user setting.
pub type SettingIdent = TIdent<Resource>;

pub use kvapi_impl::Resource;

mod kvapi_impl {

    use databend_meta_kvapi::kvapi;

    use crate::principal::SettingIdent;
    use crate::principal::UserSetting;
    use crate::tenant_key::resource::TenantResource;

    pub struct Resource;
    impl TenantResource for Resource {
        const PREFIX: &'static str = "__fd_settings";
        const TYPE: &'static str = "SettingIdent";
        const HAS_TENANT: bool = true;
        type ValueType = UserSetting;
    }

    impl kvapi::Value for UserSetting {
        type KeyType = SettingIdent;
        fn dependency_keys(&self, _key: &Self::KeyType) -> impl IntoIterator<Item = String> {
            []
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_meta_kvapi::kvapi::Key;

    use crate::principal::user_setting_ident::SettingIdent;
    use crate::tenant::Tenant;

    #[test]
    fn test_setting_ident() {
        let tenant = Tenant::new_literal("tenant1");
        let ident = SettingIdent::new(tenant.clone(), "test");
        assert_eq!("__fd_settings/tenant1/test", ident.to_string_key());

        let got = SettingIdent::from_str_key(&ident.to_string_key()).unwrap();
        assert_eq!(ident, got);
    }
}
