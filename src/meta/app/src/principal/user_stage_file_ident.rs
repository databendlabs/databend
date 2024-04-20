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

use crate::principal::StageIdent;

/// Define the meta-service key for a file belonging to a stage.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct StageFileIdent {
    pub stage: StageIdent,
    pub path: String,
}

impl StageFileIdent {
    pub fn new(stage: StageIdent, path: impl ToString) -> Self {
        Self {
            stage,
            path: path.to_string(),
        }
    }
}

mod kvapi_key_impl {
    use databend_common_meta_kvapi::kvapi;
    use databend_common_meta_kvapi::kvapi::KeyBuilder;
    use databend_common_meta_kvapi::kvapi::KeyError;
    use databend_common_meta_kvapi::kvapi::KeyParser;

    use crate::principal::user_stage_file_ident::StageFileIdent;
    use crate::principal::StageFile;
    use crate::principal::StageIdent;
    use crate::tenant::Tenant;
    use crate::KeyWithTenant;

    impl kvapi::KeyCodec for StageFileIdent {
        fn encode_key(&self, b: KeyBuilder) -> KeyBuilder {
            b.push_str(self.stage.tenant_name())
                .push_str(self.stage.name())
                .push_str(&self.path)
        }

        fn decode_key(p: &mut KeyParser) -> Result<Self, KeyError> {
            let stage = StageIdent::decode_key(p)?;
            let path = p.next_str()?;
            Ok(StageFileIdent::new(stage, path))
        }
    }

    impl kvapi::Key for StageFileIdent {
        const PREFIX: &'static str = "__fd_stage_files";
        type ValueType = StageFile;

        fn parent(&self) -> Option<String> {
            Some(self.stage.to_string_key())
        }
    }

    impl KeyWithTenant for StageFileIdent {
        fn tenant(&self) -> &Tenant {
            self.stage.tenant()
        }
    }

    impl kvapi::Value for StageFile {
        fn dependency_keys(&self) -> impl IntoIterator<Item = String> {
            []
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_common_meta_kvapi::kvapi::Key;

    use crate::principal::user_stage_file_ident::StageFileIdent;
    use crate::principal::StageIdent;
    use crate::tenant::Tenant;

    #[test]
    fn test_kvapi_key_for_stage_file_ident() {
        let tenant = Tenant::new_literal("tenant1");
        let stage = StageIdent::new(tenant, "stage1");
        let sfi = StageFileIdent::new(stage, "file1");

        let key = sfi.to_string_key();
        assert_eq!("__fd_stage_files/tenant1/stage1/file1", key,);
        assert_eq!(sfi, StageFileIdent::from_str_key(&key).unwrap());
    }
}
