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

pub use kvapi_impl::SequenceStorageRsc;
pub use kvapi_impl::SequenceStorageValue;

use crate::tenant_key::ident::TIdent;

/// Defines the meta-service key for sequence.
pub type SequenceStorageIdent = TIdent<SequenceStorageRsc>;

mod kvapi_impl {

    use databend_meta_kvapi::kvapi;

    use crate::primitive::Id;
    use crate::tenant_key::resource::TenantResource;

    /// Defines the storage for sequence generator.
    ///
    /// [`SequenceRsc`] just stores the metadata of sequence but not the counter.
    /// This key stores the counter for the sequence number.
    pub struct SequenceStorageRsc;
    impl TenantResource for SequenceStorageRsc {
        const PREFIX: &'static str = "__fd_sequence_storage";
        const HAS_TENANT: bool = true;
        type ValueType = Id<SequenceStorageValue>;
    }

    impl kvapi::Value for Id<SequenceStorageValue> {
        type KeyType = super::SequenceStorageIdent;
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
    pub struct SequenceStorageValue(pub u64);
}

#[cfg(test)]
mod tests {
    use databend_meta_kvapi::kvapi::Key;

    use crate::schema::sequence_storage::SequenceStorageIdent;
    use crate::tenant::Tenant;

    #[test]
    fn test_sequence_storage_ident() {
        let tenant = Tenant::new_literal("dummy");
        let ident = SequenceStorageIdent::new_generic(tenant, "3".to_string());

        let key = ident.to_string_key();
        assert_eq!(key, "__fd_sequence_storage/dummy/3");

        assert_eq!(ident, SequenceStorageIdent::from_str_key(&key).unwrap());
    }
}
