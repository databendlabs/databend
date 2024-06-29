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

use std::fmt::Debug;
use std::fmt::Display;
use std::fmt::Formatter;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::RemoteExpr;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_meta_app::principal::StageInfo;
use databend_common_storage::init_stage_operator;
use databend_common_storage::StageFileInfo;
use databend_common_storage::StageFilesInfo;

#[derive(serde::Serialize, serde::Deserialize, Clone, PartialEq, Eq)]
pub struct StageTableInfo {
    pub schema: TableSchemaRef,
    pub default_values: Option<Vec<RemoteExpr>>,
    pub files_info: StageFilesInfo,
    pub stage_info: StageInfo,
    pub files_to_copy: Option<Vec<StageFileInfo>>,
    // files that
    // - are listed as candidates to be copied
    // - but already exist in the meta server's "copied-files" set of target table
    // - should be ignored in the copy process
    // - may need to be purged as well (depends on the copy options)
    pub duplicated_files_detected: Vec<String>,
    pub is_select: bool,
}

impl StageTableInfo {
    pub fn schema(&self) -> Arc<TableSchema> {
        self.schema.clone()
    }

    pub fn desc(&self) -> String {
        self.stage_info.stage_name.clone()
    }

    #[async_backtrace::framed]
    pub async fn list_files(
        &self,
        thread_num: usize,
        max_files: Option<usize>,
    ) -> Result<Vec<StageFileInfo>> {
        let infos =
            list_stage_files(&self.stage_info, &self.files_info, thread_num, max_files).await?;
        Ok(infos)
    }
}

pub async fn list_stage_files(
    stage_info: &StageInfo,
    files_info: &StageFilesInfo,
    thread_num: usize,
    max_files: Option<usize>,
) -> Result<Vec<StageFileInfo>> {
    let op = init_stage_operator(stage_info)?;
    let infos = files_info
        .list(&op, thread_num, max_files)
        .await?
        .into_iter()
        .collect::<Vec<_>>();
    Ok(infos)
}

impl Debug for StageTableInfo {
    // Ignore the schema.
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{:?}", self.stage_info)
    }
}

impl Display for StageTableInfo {
    // Ignore the schema.
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "StageName {}", self.stage_info.stage_name)?;
        write!(f, "StageType {}", self.stage_info.stage_type)?;
        write!(f, "StageParam {}", self.stage_info.stage_params.storage)?;
        write!(f, "IsTemporary {}", self.stage_info.is_temporary)?;
        write!(f, "FileFormatParams {}", self.stage_info.file_format_params)?;
        write!(f, "CopyOption {}", self.stage_info.copy_options)
    }
}
