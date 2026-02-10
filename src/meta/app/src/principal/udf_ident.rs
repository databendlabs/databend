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

pub type UdfIdent = TIdent<Resource>;

/// Share name as value.
pub type UdfIdentRaw = TIdentRaw<Resource>;

pub use kvapi_impl::Resource;

impl UdfIdent {
    pub fn udf_name(&self) -> &str {
        self.name()
    }
}

impl UdfIdentRaw {
    pub fn udf_name(&self) -> &str {
        self.name()
    }
}

mod kvapi_impl {

    use databend_meta_kvapi::kvapi;

    use crate::principal::UdfIdent;
    use crate::principal::UserDefinedFunction;
    use crate::tenant_key::resource::TenantResource;

    pub struct Resource;
    impl TenantResource for Resource {
        const PREFIX: &'static str = "__fd_udfs";
        const TYPE: &'static str = "UdfIdent";
        const HAS_TENANT: bool = true;
        type ValueType = UserDefinedFunction;
    }

    impl kvapi::Value for UserDefinedFunction {
        type KeyType = UdfIdent;

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

    use super::UdfIdent;
    use crate::tenant::Tenant;

    #[test]
    fn test_ident() {
        let tenant = Tenant::new_literal("test");
        let ident = UdfIdent::new(tenant, "test1");

        let key = ident.to_string_key();
        assert_eq!(key, "__fd_udfs/test/test1");

        assert_eq!(ident, UdfIdent::from_str_key(&key).unwrap());
    }
}
