// Copyright 2021 Datafuse Labs.
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
use std::path::Path;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use common_base::base::uuid;
use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::PartInfo;
use common_catalog::plan::PartStatistics;
use common_catalog::plan::Partitions;
use common_catalog::plan::PartitionsShuffleKind;
use common_catalog::plan::PushDownInfo;
use common_catalog::plan::StageTableInfo;
use common_catalog::table::AppendMode;
use common_catalog::table::Table;
use common_catalog::table_context::TableContext;
use common_datablocks::BlockCompactThresholds;
use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_app::schema::TableInfo;
use common_meta_types::StageType;
use common_meta_types::UserStageInfo;
use common_pipeline_core::Pipeline;
use common_storage::init_operator;
use opendal::layers::SubdirLayer;
use opendal::Operator;
use parking_lot::Mutex;
use regex::Regex;

use crate::list_file;
use crate::stage_table_sink::StageTableSink;
use crate::stat_file;

/// TODO: we need to track the data metrics in stage table.
pub struct StageTable {
    table_info: StageTableInfo,
    // This is no used but a placeholder.
    // But the Table trait need it:
    // fn get_table_info(&self) -> &TableInfo).
    table_info_placeholder: TableInfo,
    block_compact_threshold: Mutex<Option<BlockCompactThresholds>>,
}

impl StageTable {
    pub fn try_create(table_info: StageTableInfo) -> Result<Arc<dyn Table>> {
        let table_info_placeholder = TableInfo::default().set_schema(table_info.schema());

        Ok(Arc::new(Self {
            table_info,
            table_info_placeholder,
            block_compact_threshold: Default::default(),
        }))
    }

    /// Get operator with correctly prefix.
    pub fn get_op(ctx: &Arc<dyn TableContext>, stage: &UserStageInfo) -> Result<Operator> {
        if stage.stage_type == StageType::External {
            Ok(init_operator(&stage.stage_params.storage)?)
        } else {
            let pop = ctx.get_data_operator()?.operator();
            Ok(pop.layer(SubdirLayer::new(&stage.stage_prefix())))
        }
    }
}

#[async_trait::async_trait]
impl Table for StageTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    // External stage has no table info yet.
    fn get_table_info(&self) -> &TableInfo {
        &self.table_info_placeholder
    }

    async fn read_partitions(
        &self,
        ctx: Arc<dyn TableContext>,
        push_downs: Option<PushDownInfo>,
    ) -> Result<(PartStatistics, Partitions)> {
        let pushdown = push_downs.ok_or_else(|| {
            ErrorCode::Internal("stage_table: pushdown cannot be None(It's a bug)")
        })?;
        let stage_info = pushdown.stage.ok_or_else(|| {
            ErrorCode::Internal("stage_table: pushdown.stage cannot be None(It's a bug)")
        })?;

        // User set the files.
        let files = stage_info.files;

        // 1. List all files.
        let path = &stage_info.path;
        let mut all_files = if !files.is_empty() {
            let mut res = vec![];
            for file in files {
                // Here we add the path to the file: /path/to/path/file1.
                let new_path = Path::new(path).join(file).to_string_lossy().to_string();
                let info = stat_file(ctx.clone(), &new_path, &stage_info.user_stage_info).await?;
                res.push(info);
            }
            res
        } else {
            list_file(ctx.clone(), path, &stage_info.user_stage_info).await?
        };

        // 2. Retain pattern match files.
        {
            let pattern = &stage_info.pattern;
            if !pattern.is_empty() {
                let regex = Regex::new(pattern).map_err(|e| {
                    ErrorCode::SyntaxException(format!(
                        "Pattern format invalid, got:{}, error:{:?}",
                        pattern, e
                    ))
                })?;
                all_files.retain(|v| regex.is_match(&v.path));
            }
        }

        let partitions = all_files
            .iter()
            .map(|v| {
                let part_info: Box<dyn PartInfo> = Box::new(v.clone());
                Arc::new(part_info)
            })
            .collect::<Vec<_>>();
        Ok((
            PartStatistics::default(),
            Partitions::create(PartitionsShuffleKind::None, partitions),
        ))
    }

    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let (_, _, _) = (ctx, plan, pipeline);
        Ok(())
    }

    fn append_data(
        &self,
        ctx: Arc<dyn TableContext>,
        pipeline: &mut Pipeline,
        _: AppendMode,
        _: bool,
    ) -> Result<()> {
        let single = self.table_info.user_stage_info.copy_options.single;
        let op = StageTable::get_op(&ctx, &self.table_info.user_stage_info)?;

        let uuid = uuid::Uuid::new_v4().to_string();
        let group_id = AtomicUsize::new(0);

        // parallel compact unload, the partial block will flush into next operator
        if !single && pipeline.output_len() > 1 {
            pipeline.add_transform(|input, output| {
                let gid = group_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                StageTableSink::try_create(
                    input,
                    ctx.clone(),
                    self.table_info.clone(),
                    op.clone(),
                    Some(output),
                    uuid.clone(),
                    gid,
                )
            })?;
        }

        // final compact unload
        pipeline.resize(1)?;

        // Add sink pipe.
        pipeline.add_sink(|input| {
            let gid = group_id.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            StageTableSink::try_create(
                input,
                ctx.clone(),
                self.table_info.clone(),
                op.clone(),
                None,
                uuid.clone(),
                gid,
            )
        })
    }

    // TODO use tmp file_name & rename to have atomic commit
    async fn commit_insertion(
        &self,
        _ctx: Arc<dyn TableContext>,
        _operations: Vec<DataBlock>,
        _overwrite: bool,
    ) -> Result<()> {
        Ok(())
    }

    // Truncate the stage file.
    async fn truncate(&self, _ctx: Arc<dyn TableContext>, _: bool) -> Result<()> {
        Err(ErrorCode::Unimplemented(
            "S3 external table truncate() unimplemented yet!",
        ))
    }

    fn get_block_compact_thresholds(&self) -> BlockCompactThresholds {
        let guard = self.block_compact_threshold.lock();
        (*guard).expect("must success")
    }

    fn set_block_compact_thresholds(&self, thresholds: BlockCompactThresholds) {
        let mut guard = self.block_compact_threshold.lock();
        (*guard) = Some(thresholds)
    }
}
