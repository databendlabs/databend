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

/// QuotaIdent is a unique identifier for a quota.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TenantQuotaIdent {
    pub tenant: Tenant,
}

impl TenantQuotaIdent {
    pub fn new(tenant: Tenant) -> Self {
        Self { tenant }
    }
}

mod kvapi_key_impl {
    use databend_common_meta_kvapi::kvapi;
    use databend_common_meta_kvapi::kvapi::KeyError;

    use crate::tenant::Tenant;
    use crate::tenant::TenantQuota;
    use crate::tenant::TenantQuotaIdent;
    use crate::KeyWithTenant;

    impl kvapi::Key for TenantQuotaIdent {
        const PREFIX: &'static str = "__fd_quotas";
        type ValueType = TenantQuota;

        fn parent(&self) -> Option<String> {
            Some(self.tenant.to_string_key())
        }

        fn encode_key(&self, b: kvapi::KeyBuilder) -> kvapi::KeyBuilder {
            b.push_str(self.tenant_name())
        }

        fn decode_key(p: &mut kvapi::KeyParser) -> Result<Self, KeyError> {
            let tenant = p.next_nonempty()?;
            Ok(TenantQuotaIdent::new(Tenant::new_nonempty(tenant)))
        }
    }

    impl KeyWithTenant for TenantQuotaIdent {
        fn tenant(&self) -> &Tenant {
            &self.tenant
        }
    }

    impl kvapi::Value for TenantQuota {
        fn dependency_keys(&self) -> impl IntoIterator<Item = String> {
            []
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_common_meta_kvapi::kvapi::Key;

    use super::*;

    #[test]
    fn test_quota_ident() {
        let tenant = Tenant::new_literal("test");
        let ident = TenantQuotaIdent::new(tenant.clone());

        let key = ident.to_string_key();
        assert_eq!(key, "__fd_quotas/test");

        assert_eq!(ident, TenantQuotaIdent::from_str_key(&key).unwrap());
    }
}
