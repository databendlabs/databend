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

pub type RowAccessPolicyId = DataId<Resource>;

pub type RowAccessPolicyIdIdent = TIdent<Resource, RowAccessPolicyId>;
pub type RowAccessPolicyIdIdentRaw = TIdentRaw<Resource, RowAccessPolicyId>;

pub use kvapi_impl::Resource;

use crate::data_id::DataId;
use crate::tenant::ToTenant;

impl RowAccessPolicyIdIdent {
    pub fn new(tenant: impl ToTenant, row_access_id: u64) -> Self {
        Self::new_generic(tenant, RowAccessPolicyId::new(row_access_id))
    }

    pub fn row_access_policy_id(&self) -> RowAccessPolicyId {
        *self.name()
    }
}

impl RowAccessPolicyIdIdentRaw {
    pub fn row_access_policy_id(&self) -> RowAccessPolicyId {
        *self.name()
    }
}

mod kvapi_impl {

    use databend_meta_kvapi::kvapi;

    use crate::row_access_policy::RowAccessPolicyIdIdent;
    use crate::row_access_policy::RowAccessPolicyMeta;
    use crate::tenant_key::resource::TenantResource;

    pub struct Resource;
    impl TenantResource for Resource {
        const PREFIX: &'static str = "__fd_row_access_policy_by_id";
        const TYPE: &'static str = "RowAccessPolicyIdIdent";
        const HAS_TENANT: bool = true;
        type ValueType = RowAccessPolicyMeta;
    }

    impl kvapi::Value for RowAccessPolicyMeta {
        type KeyType = RowAccessPolicyIdIdent;
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

    use super::RowAccessPolicyIdIdent;
    use crate::row_access_policy::RowAccessPolicyId;
    use crate::tenant::Tenant;

    #[test]
    fn test_row_access_id_ident() {
        let tenant = Tenant::new_literal("dummy");
        let ident = RowAccessPolicyIdIdent::new_generic(tenant, RowAccessPolicyId::new(3));

        let key = ident.to_string_key();
        assert_eq!(key, "__fd_row_access_policy_by_id/dummy/3");

        assert_eq!(ident, RowAccessPolicyIdIdent::from_str_key(&key).unwrap());
    }

    #[test]
    fn test_row_access_id_ident_with_key_space() {
        // TODO(xp): implement this test
        // let tenant = Tenant::new_literal("test");
        // let ident = RowAccessPolicyIdIdent::new(tenant, 3);
        //
        // let key = ident.to_string_key();
        // assert_eq!(key, "__fd_background_job_by_id/3");
        //
        // assert_eq!(ident, RowAccessPolicyIdIdent::from_str_key(&key).unwrap());
    }
}
