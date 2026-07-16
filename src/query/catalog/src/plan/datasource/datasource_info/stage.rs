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

use databend_common_ast::ast::CopyIntoTableOptions;
use databend_common_exception::Result;
use databend_common_expression::RemoteDefaultExpr;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_meta_app::principal::StageInfo;
use databend_common_meta_app::schema::TableInfo;
use databend_common_storage::DataOperator;
use databend_common_storage::StageFileInfo;
use databend_common_storage::StageFilesInfo;
use databend_common_storage::init_stage_operator;

use crate::plan::FullParquetMeta;
use crate::plan::ParquetCopySchema;

/// Metadata for the intentionally dangerous Fuse block recovery COPY source.
///
/// # Safety contract
///
/// Snapshot and segment metadata may be unavailable, so recovery maps physical
/// Parquet fields by their exact name and complete logical type. That is sound
/// only when the table has never dropped and re-added a column with the same
/// name, and no rename chain has returned to the same final name and physical
/// schema. A block footer cannot detect either history.
#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct FuseRecoveryBlocksInfo {
    pub table_info: TableInfo,
    pub block_prefix: String,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Default)]
pub struct StageTableInfo {
    // common
    pub stage_info: StageInfo,

    // copy into table only
    pub schema: TableSchemaRef,
    pub default_exprs: Option<Vec<RemoteDefaultExpr>>,
    pub files_info: StageFilesInfo,
    pub files_to_copy: Option<Vec<StageFileInfo>>,
    // files that
    // - are listed as candidates to be copied
    // - but already exist in the meta server's "copied-files" set of target table
    // - should be ignored in the copy process
    // - may need to be purged as well (depends on the copy options)
    pub duplicated_files_detected: Vec<String>,
    pub is_select: bool,
    pub copy_into_table_options: CopyIntoTableOptions,
    pub is_variant: bool,

    /// Complete schemas prepared for Parquet COPY readers on remote workers.
    #[serde(default)]
    pub parquet_copy_schemas: Vec<ParquetCopySchema>,

    /// Present only for `COPY ... FROM FUSE_RECOVERY_BLOCKS(...)`.
    #[serde(default)]
    pub fuse_recovery: Option<FuseRecoveryBlocksInfo>,

    // temp work round, when enable_schema_evolution, set it before read partition,
    // then the StageTableInfo will be dropped, so no need to free it
    #[serde(skip)]
    pub parquet_metas: Option<Vec<Arc<FullParquetMeta>>>,
}

impl PartialEq for StageTableInfo {
    fn eq(&self, other: &Self) -> bool {
        self.stage_info == other.stage_info
            && self.fuse_recovery.as_ref().map(|v| &v.block_prefix)
                == other.fuse_recovery.as_ref().map(|v| &v.block_prefix)
    }
}

impl Eq for StageTableInfo {}

impl StageTableInfo {
    pub fn schema(&self) -> Arc<TableSchema> {
        self.schema.clone()
    }

    pub fn desc(&self) -> String {
        if let Some(recovery) = &self.fuse_recovery {
            return format!("FUSE_RECOVERY_BLOCKS {}", recovery.block_prefix);
        }
        self.stage_info.stage_name.clone()
    }

    /// Return the source operator used by this stage-like scan.
    pub fn operator(&self) -> Result<opendal::Operator> {
        if self.fuse_recovery.is_some() {
            return Ok(DataOperator::instance().operator());
        }
        init_stage_operator(&self.stage_info)
    }

    #[async_backtrace::framed]
    pub async fn list_files(
        &self,
        thread_num: usize,
        max_files: Option<usize>,
    ) -> Result<Vec<StageFileInfo>> {
        let op = self.operator()?;
        let infos = self.files_info.list(&op, thread_num, max_files).await?;
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
        if let Some(recovery) = &self.fuse_recovery {
            return write!(f, "FUSE_RECOVERY_BLOCKS Prefix {}", recovery.block_prefix);
        }
        write!(f, "StageName {}", self.stage_info.stage_name)?;
        write!(f, "StageType {}", self.stage_info.stage_type)?;
        write!(f, "StageParam {}", self.stage_info.stage_params.storage)?;
        write!(f, "IsTemporary {}", self.stage_info.is_temporary)?;
        write!(f, "FileFormatParams {}", self.stage_info.file_format_params)?;
        Ok(())
    }
}
