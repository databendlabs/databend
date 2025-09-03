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

use std::any::Any;
use std::hash::{DefaultHasher, Hash, Hasher};
use std::sync::Arc;
use databend_common_catalog::plan::{PartInfo, PartInfoPtr, PartInfoType};
use databend_common_exception::ErrorCode;
use databend_common_meta_app::principal::{FileFormatParams, StageInfo};
use databend_common_storage::{StageFileInfo, StageFilesInfo};

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct InferSchemaPartInfo {
    pub files_info: StageFilesInfo,
    pub file_format_params: FileFormatParams,
    pub stage_info: StageInfo,
    pub stage_file_infos: Vec<StageFileInfo>,
    
}

#[typetag::serde(name = "infer_schema")]
impl PartInfo for InferSchemaPartInfo {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn equals(&self, info: &Box<dyn PartInfo>) -> bool {
        info.as_any()
            .downcast_ref::<InferSchemaPartInfo>()
            .is_some_and(|other| self == other)
    }

    fn hash(&self) -> u64 {
        let mut s = DefaultHasher::new();
        self.files_info.hash(&mut s);
        self.file_format_params.hash(&mut s);
        self.stage_info.hash(&mut s);
        s.finish()
    }

    fn part_type(&self) -> PartInfoType {
        PartInfoType::LazyLevel
    }
}

impl InferSchemaPartInfo {
    pub fn create(
        files_info: StageFilesInfo,
        file_format_params: FileFormatParams,
        stage_info: StageInfo,
        stage_file_infos: Vec<StageFileInfo>,
    ) -> PartInfoPtr {
        Arc::new(Box::new(InferSchemaPartInfo {
            files_info,
            file_format_params,
            stage_info,
            stage_file_infos,
        }))
    }

    pub fn from_part(info: &PartInfoPtr) -> databend_common_exception::Result<&InferSchemaPartInfo> {
        info.as_any()
            .downcast_ref::<InferSchemaPartInfo>()
            .ok_or_else(|| {
                ErrorCode::Internal("Cannot downcast from PartInfo to InferSchemaPartInfo.")
            })
    }
}