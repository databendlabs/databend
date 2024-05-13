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

/// The share consuming key describes that the `tenant`, who is a consumer of a shared object,
/// which is created by another tenant and is identified by `share_id`.
pub type ShareConsumerIdent = TIdent<Resource, u64>;
pub type ShareConsumerIdentRaw = TIdentRaw<Resource, u64>;

pub use kvapi_impl::Resource;

use crate::tenant_key::raw::TIdentRaw;

impl ShareConsumerIdent {
    pub fn share_id(&self) -> u64 {
        *self.name()
    }
}

impl ShareConsumerIdentRaw {
    pub fn share_id(&self) -> u64 {
        *self.name()
    }
}

mod kvapi_impl {

    use databend_common_meta_kvapi::kvapi;

    use crate::share::ShareAccountMeta;
    use crate::tenant_key::resource::TenantResource;

    pub struct Resource;
    impl TenantResource for Resource {
        const PREFIX: &'static str = "__fd_share_account_id";
        const TYPE: &'static str = "ShareConsumerIdent";
        const HAS_TENANT: bool = true;
        type ValueType = ShareAccountMeta;
    }

    impl kvapi::Value for ShareAccountMeta {
        fn dependency_keys(&self) -> impl IntoIterator<Item = String> {
            []
        }
    }

    // // Use these error types to replace usage of ErrorCode if possible.
    // impl From<ExistError<Resource>> for ErrorCode {
    // impl From<UnknownError<Resource>> for ErrorCode {
}

#[cfg(test)]
mod tests {
    use databend_common_meta_kvapi::kvapi::Key;

    use super::ShareConsumerIdent;
    use crate::tenant::Tenant;

    #[test]
    fn test_share_consumer_ident() {
        let tenant = Tenant::new_literal("test");
        let ident = ShareConsumerIdent::new(tenant, 3);

        let key = ident.to_string_key();
        assert_eq!(key, "__fd_share_account_id/test/3");

        assert_eq!(ident, ShareConsumerIdent::from_str_key(&key).unwrap());
    }
}
