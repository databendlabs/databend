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

/// Define the meta-service key for a stage.
pub type StageIdent = TIdent<Resource>;
pub type StageIdentRaw = TIdentRaw<Resource>;

pub use kvapi_impl::Resource;

impl StageIdent {
    pub fn stage_name(&self) -> &str {
        self.name()
    }
}

impl StageIdentRaw {
    pub fn stage_name(&self) -> &str {
        self.name()
    }
}

mod kvapi_impl {

    use databend_meta_kvapi::kvapi;

    use super::StageIdent;
    use crate::principal::StageInfo;
    use crate::tenant_key::resource::TenantResource;

    pub struct Resource;
    impl TenantResource for Resource {
        const PREFIX: &'static str = "__fd_stages";
        const TYPE: &'static str = "StageIdent";
        const HAS_TENANT: bool = true;
        type ValueType = StageInfo;
    }

    impl kvapi::Value for StageInfo {
        type KeyType = StageIdent;
        fn dependency_keys(&self, _key: &Self::KeyType) -> impl IntoIterator<Item = String> {
            []
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_meta_kvapi::kvapi::Key;

    use crate::principal::user_stage_ident::StageIdent;
    use crate::tenant::Tenant;

    #[test]
    fn test_stage_ident() {
        let tenant = Tenant::new_literal("test");
        let stage = StageIdent::new(tenant, "stage");

        let key = stage.to_string_key();
        assert_eq!(key, "__fd_stages/test/stage");
        assert_eq!(stage, StageIdent::from_str_key(&key).unwrap());
    }
}
