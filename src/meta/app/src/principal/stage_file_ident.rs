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

use crate::principal::StageFilePath;
use crate::principal::StageIdent;
use crate::tenant_key::ident::TIdent;
use crate::tenant_key::raw::TIdentRaw;

pub type StageFileIdent = TIdent<Resource, StageFilePath>;
pub type StageFileIdentRaw = TIdentRaw<Resource, StageFilePath>;

pub use kvapi_impl::Resource;

use crate::KeyWithTenant;

impl StageFileIdent {
    pub fn new(stage: StageIdent, path: impl ToString) -> Self {
        Self::new_generic(stage.tenant(), StageFilePath::new(stage.stage_name(), path))
    }

    pub fn stage_file_path(&self) -> &StageFilePath {
        self.name()
    }

    pub fn stage_name(&self) -> &str {
        self.stage_file_path().stage_name()
    }

    pub fn path(&self) -> &str {
        self.stage_file_path().path()
    }
}

mod kvapi_impl {

    use databend_meta_kvapi::kvapi;

    use super::StageFileIdent;
    use crate::principal::StageFile;
    use crate::tenant_key::resource::TenantResource;

    pub struct Resource;
    impl TenantResource for Resource {
        const PREFIX: &'static str = "__fd_stage_files";
        const TYPE: &'static str = "StageFileIdent";
        const HAS_TENANT: bool = true;
        type ValueType = StageFile;
    }

    impl kvapi::Value for StageFile {
        type KeyType = StageFileIdent;
        fn dependency_keys(&self, _key: &Self::KeyType) -> impl IntoIterator<Item = String> {
            []
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_meta_kvapi::kvapi::Key;

    use crate::principal::StageIdent;
    use crate::principal::stage_file_ident::StageFileIdent;
    use crate::tenant::Tenant;

    #[test]
    fn test_stage_file_ident() {
        let tenant = Tenant::new_literal("tenant1");
        let stage = StageIdent::new(tenant, "stage1");
        let sfi = StageFileIdent::new(stage, "file1");

        let key = sfi.to_string_key();
        assert_eq!("__fd_stage_files/tenant1/stage1/file1", key,);
        assert_eq!(sfi, StageFileIdent::from_str_key(&key).unwrap());
    }

    #[test]
    fn test_stage_file_ident_with_key_space() {
        // TODO(xp):
    }
}
