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

use super::DictionaryIdentity;
use crate::tenant::ToTenant;
use crate::tenant_key::ident::TIdent;
use crate::KeyWithTenant;

/// A dictionary identity belonging to a tenant.
pub type TenantDictionaryIdent = TIdent<Resource, DictionaryIdentity>;
pub type TenantDictionaryIdentRaw = TIdent<Resource, DictionaryIdentity>;

pub use kvapi_impl::Resource;

impl TenantDictionaryIdent {
    pub fn new(tenant: impl ToTenant, dictionary: DictionaryIdentity) -> Self {
        Self::new_generic(tenant, dictionary)
    }

    pub fn dict_name(&self) -> String {
        self.name().dict_name.clone()
    }

    pub fn db_id(&self) -> u64 {
        self.name().db_id
    }

    pub fn tenant_name(&self) -> &str {
        self.tenant().tenant_name()
    }
}

mod kvapi_impl {

    use databend_common_meta_kvapi::kvapi;

    use crate::schema::DictionaryId;
    use crate::tenant_key::resource::TenantResource;

    pub struct Resource;
    impl TenantResource for Resource {
        const PREFIX: &'static str = "__fd_dictionaries";
        const TYPE: &'static str = "TenantDictionaryIdent";
        const HAS_TENANT: bool = true;
        type ValueType = DictionaryId;
    }

    impl kvapi::Value for DictionaryId {
        fn dependency_keys(&self) -> impl IntoIterator<Item = String> {
            []
        }
    }
}
