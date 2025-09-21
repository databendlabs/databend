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

use std::ops::Deref;

pub use kvapi_impl::AutoIncrementRsc;

use crate::principal::AutoIncrementKey;
use crate::schema::sequence_storage::SequenceStorageIdent;
use crate::schema::SequenceMeta;
use crate::tenant_key::ident::TIdent;
use crate::KeyWithTenant;

/// AutoIncrementIdent is the Ident of Sequence in AutoIncrement,
/// which is used to distinguish SequenceIdent and manage resource recycling and show meta separately.
pub type AutoIncrementIdent = TIdent<AutoIncrementRsc, AutoIncrementKey>;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AutoIncrementMeta(pub SequenceMeta);

impl Deref for AutoIncrementMeta {
    type Target = SequenceMeta;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl AutoIncrementIdent {
    pub fn to_storage_ident(&self) -> SequenceStorageIdent {
        SequenceStorageIdent::new_generic(self.tenant(), self.name().to_string())
    }
}

mod kvapi_impl {

    use databend_common_meta_kvapi::kvapi;

    use crate::schema::auto_incremnt::AutoIncrementMeta;
    use crate::tenant_key::resource::TenantResource;

    pub struct AutoIncrementRsc;
    impl TenantResource for AutoIncrementRsc {
        const PREFIX: &'static str = "__fd_auto_increment";
        const HAS_TENANT: bool = true;
        type ValueType = AutoIncrementMeta;
    }

    impl kvapi::Value for AutoIncrementMeta {
        type KeyType = super::AutoIncrementIdent;
        fn dependency_keys(&self, _key: &Self::KeyType) -> impl IntoIterator<Item = String> {
            []
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_common_meta_kvapi::kvapi::Key;

    use crate::principal::AutoIncrementKey;
    use crate::schema::AutoIncrementIdent;
    use crate::tenant::Tenant;

    #[test]
    fn test_auto_increment_ident() {
        let tenant = Tenant::new_literal("dummy");
        let key = AutoIncrementKey::new(2, 3);
        let ident = AutoIncrementIdent::new_generic(tenant, key);

        let key = ident.to_string_key();
        assert_eq!(key, "__fd_auto_increment/dummy/2/3");

        assert_eq!(ident, AutoIncrementIdent::from_str_key(&key).unwrap());
    }
}
