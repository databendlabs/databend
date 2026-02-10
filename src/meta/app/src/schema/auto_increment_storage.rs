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

pub use kvapi_impl::AutoIncrementStorageRsc;
pub use kvapi_impl::AutoIncrementStorageValue;

use crate::principal::AutoIncrementKey;
use crate::tenant_key::ident::TIdent;

/// Defines the meta-service key for sequence.
pub type AutoIncrementStorageIdent = TIdent<AutoIncrementStorageRsc, AutoIncrementKey>;

mod kvapi_impl {

    use databend_meta_kvapi::kvapi;

    use crate::primitive::Id;
    use crate::tenant_key::resource::TenantResource;

    /// Defines the storage for autoincrement generator.
    ///
    /// [`AutoIncrementStorageRsc`] like ['SequenceStorageRsc'] just stores the metadata of autoincrement but not the counter.
    /// This key stores the counter for the autoincrement name.
    pub struct AutoIncrementStorageRsc;
    impl TenantResource for AutoIncrementStorageRsc {
        const PREFIX: &'static str = "__fd_autoincrement_storage";
        const HAS_TENANT: bool = true;
        type ValueType = Id<AutoIncrementStorageValue>;
    }

    impl kvapi::Value for Id<AutoIncrementStorageValue> {
        type KeyType = super::AutoIncrementStorageIdent;
        fn dependency_keys(&self, _key: &Self::KeyType) -> impl IntoIterator<Item = String> {
            []
        }
    }

    #[derive(
        Debug,
        Clone,
        Copy,
        Default,
        PartialEq,
        Eq,
        derive_more::From,
        derive_more::Deref,
        derive_more::DerefMut,
    )]
    pub struct AutoIncrementStorageValue(pub u64);
}

#[cfg(test)]
mod tests {
    use databend_meta_kvapi::kvapi::Key;

    use crate::principal::AutoIncrementKey;
    use crate::schema::auto_increment_storage::AutoIncrementStorageIdent;
    use crate::tenant::Tenant;

    #[test]
    fn test_auto_increment_storage_ident() {
        let tenant = Tenant::new_literal("dummy");
        let key = AutoIncrementKey::new(0, 3);
        let ident = AutoIncrementStorageIdent::new_generic(tenant, key);

        let key = ident.to_string_key();
        assert_eq!(key, "__fd_autoincrement_storage/dummy/0/3");

        assert_eq!(
            ident,
            AutoIncrementStorageIdent::from_str_key(&key).unwrap()
        );
    }
}
