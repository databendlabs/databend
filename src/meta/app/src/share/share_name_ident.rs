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

use databend_common_exception::Result;

use crate::tenant::Tenant;
use crate::tenant_key::ident::TIdent;
use crate::tenant_key::raw::TIdentRaw;

pub type ShareNameIdent = TIdent<Resource>;

/// Share name as value.
pub type ShareNameIdentRaw = TIdentRaw<Resource>;

use anyerror::func_name;
use databend_common_exception::ErrorCode;
pub use kvapi_impl::Resource;

impl TIdent<Resource> {
    pub fn share_name(&self) -> &str {
        self.name()
    }
}

impl TIdentRaw<Resource> {
    pub fn share_name(&self) -> &str {
        self.name()
    }
}

impl TryFrom<databend_common_ast::ast::ShareNameIdent> for ShareNameIdent {
    type Error = ErrorCode;

    fn try_from(ident: databend_common_ast::ast::ShareNameIdent) -> Result<Self> {
        let tenant = Tenant::new_or_err(ident.tenant.name, func_name!())?;
        Ok(ShareNameIdent::new(tenant, ident.share))
    }
}

mod kvapi_impl {

    use databend_common_meta_kvapi::kvapi;
    use databend_common_meta_kvapi::kvapi::Key;

    use crate::share::ShareId;
    use crate::tenant_key::resource::TenantResource;

    pub struct Resource;
    impl TenantResource for Resource {
        const PREFIX: &'static str = "__fd_share";
        const TYPE: &'static str = "ShareNameIdent";
        const HAS_TENANT: bool = true;
        type ValueType = ShareId;
    }

    impl kvapi::Value for ShareId {
        type KeyType = super::ShareNameIdent;
        /// ShareId is id of the two level `name->id,id->value` mapping
        fn dependency_keys(&self, _key: &Self::KeyType) -> impl IntoIterator<Item = String> {
            [self.to_string_key()]
        }
    }

    // // Use these error types to replace usage of ErrorCode if possible.
    // impl From<ExistError<Resource>> for ErrorCode {
    // impl From<UnknownError<Resource>> for ErrorCode {
}

#[cfg(test)]
mod tests {
    use databend_common_meta_kvapi::kvapi::Key;

    use super::ShareNameIdent;
    use crate::tenant::Tenant;

    #[test]
    fn test_share_name_ident() {
        let tenant = Tenant::new_literal("test");
        let ident = ShareNameIdent::new(tenant, "test1");

        let key = ident.to_string_key();
        assert_eq!(key, "__fd_share/test/test1");

        assert_eq!(ident, ShareNameIdent::from_str_key(&key).unwrap());
    }
}
