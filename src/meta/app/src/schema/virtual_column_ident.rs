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

/// The virtual column name ident the meta-service key. The `u64` is table_id.
pub type VirtualColumnIdent = TIdent<Resource, u64>;
pub type VirtualColumnIdentRaw = TIdentRaw<Resource, u64>;

pub use kvapi_impl::Resource;

use crate::tenant_key::raw::TIdentRaw;

impl VirtualColumnIdent {
    pub fn table_id(&self) -> u64 {
        *self.name()
    }
}

impl VirtualColumnIdentRaw {
    pub fn table_id(&self) -> u64 {
        *self.name()
    }
}

mod kvapi_impl {

    use databend_common_meta_kvapi::kvapi;

    use crate::schema::VirtualColumnMeta;
    use crate::tenant_key::resource::TenantResource;

    pub struct Resource;
    impl TenantResource for Resource {
        const PREFIX: &'static str = "__fd_virtual_column";
        const TYPE: &'static str = "VirtualColumnIdent";
        const HAS_TENANT: bool = true;
        type ValueType = VirtualColumnMeta;
    }

    impl kvapi::Value for VirtualColumnMeta {
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

    use super::VirtualColumnIdent;
    use crate::tenant::Tenant;

    #[test]
    fn test_virtual_column_name_ident() {
        let tenant = Tenant::new_literal("test");
        let ident = VirtualColumnIdent::new(tenant, 3);

        let key = ident.to_string_key();
        assert_eq!(key, "__fd_virtual_column/test/3");

        assert_eq!(ident, VirtualColumnIdent::from_str_key(&key).unwrap());
    }
}
