// Copyright 2024 Datafuse Labs
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

pub type VacuumRetentionIdent = TIdent<VacuumRetentionRsc, ()>;

pub use kvapi_impl::VacuumRetentionRsc;

impl VacuumRetentionIdent {
    pub fn new_global(tenant: impl crate::tenant::ToTenant) -> Self {
        TIdent::new_generic(tenant, ())
    }
}

mod kvapi_impl {
    use databend_common_meta_kvapi::kvapi;

    use crate::schema::vacuum_retention::VacuumRetention;
    use crate::schema::vacuum_retention_ident::VacuumRetentionIdent;
    use crate::tenant_key::resource::TenantResource;

    pub struct VacuumRetentionRsc;

    impl TenantResource for VacuumRetentionRsc {
        const PREFIX: &'static str = "__fd_vacuum_retention_ts";
        const HAS_TENANT: bool = true;
        type ValueType = VacuumRetention;
    }

    impl kvapi::Value for VacuumRetention {
        type KeyType = VacuumRetentionIdent;

        fn dependency_keys(&self, _key: &Self::KeyType) -> impl IntoIterator<Item = String> {
            []
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_common_meta_kvapi::kvapi::Key;

    use super::VacuumRetentionIdent;
    use crate::tenant::Tenant;

    #[test]
    fn test_ident() {
        let tenant = Tenant::new_literal("dummy");
        let ident = VacuumRetentionIdent::new_global(tenant);

        let key = ident.to_string_key();
        assert_eq!(key, "__fd_vacuum_retention_ts/dummy");

        assert_eq!(ident, VacuumRetentionIdent::from_str_key(&key).unwrap());
    }
}
