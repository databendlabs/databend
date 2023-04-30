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
use std::ops::Deref;
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use common_base::base::uuid;
use common_catalog::plan::DataSourceInfo;
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
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::BlockThresholds;
use common_expression::DataBlock;
use common_meta_app::principal::StageInfo;
use common_meta_app::schema::TableInfo;
use common_meta_app::schema::UpsertTableCopiedFileReq;
use common_pipeline_core::Pipeline;
use common_pipeline_sources::input_formats::InputContext;
use common_pipeline_sources::input_formats::SplitInfo;
use common_storage::init_stage_operator;
use common_storage::StageFileInfo;
use dashmap::DashMap;
use opendal::Operator;
use parking_lot::Mutex;

use crate::stage_table_sink::StageTableSink;

/// TODO: we need to track the data metrics in stage table.
pub struct StageTable {
    table_info: StageTableInfo,
    // This is no used but a placeholder.
    // But the Table trait need it:
    // fn get_table_info(&self) -> &TableInfo).
    table_info_placeholder: TableInfo,
    block_compact_threshold: Mutex<Option<BlockThresholds>>,
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
    pub fn get_op(stage: &StageInfo) -> Result<Operator> {
        init_stage_operator(stage)
    }

    #[async_backtrace::framed]
    pub async fn list_files(
        stage_info: &StageTableInfo,
        max_files: Option<usize>,
    ) -> Result<Vec<StageFileInfo>> {
        let op = Self::get_op(&stage_info.stage_info)?;
        let infos = stage_info
            .files_info
            .list(&op, false, max_files)
            .await?
            .into_iter()
            .collect::<Vec<_>>();
        Ok(infos)
    }

    fn get_block_compact_thresholds_with_default(&self) -> BlockThresholds {
        let guard = self.block_compact_threshold.lock();
        match guard.deref() {
            None => BlockThresholds::default(),
            Some(t) => *t,
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

    fn get_data_source_info(&self) -> DataSourceInfo {
        DataSourceInfo::StageSource(self.table_info.clone())
    }

    #[async_backtrace::framed]
    async fn read_partitions(
        &self,
        ctx: Arc<dyn TableContext>,
        _push_downs: Option<PushDownInfo>,
    ) -> Result<(PartStatistics, Partitions)> {
        let stage_info = &self.table_info;
        // User set the files.
        let files = if let Some(files) = &stage_info.files_to_copy {
            files.clone()
        } else {
            StageTable::list_files(stage_info, None).await?
        };
        let format = InputContext::get_input_format(&stage_info.stage_info.file_format_params)?;
        let operator = StageTable::get_op(&stage_info.stage_info)?;
        let splits = format
            .get_splits(
                files,
                &stage_info.stage_info,
                &operator,
                &ctx.get_settings(),
            )
            .await?;

        let partitions = splits
            .into_iter()
            .map(|v| {
                let part_info: Box<dyn PartInfo> = Box::new((*v).clone());
                Arc::new(part_info)
            })
            .collect::<Vec<_>>();
        Ok((
            PartStatistics::default(),
            Partitions::create_nolazy(PartitionsShuffleKind::Seq, partitions),
        ))
    }

    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let stage_table_info =
            if let DataSourceInfo::StageSource(stage_table_info) = &plan.source_info {
                stage_table_info
            } else {
                return Err(ErrorCode::Internal(""));
            };

        let mut splits = vec![];
        for part in &plan.parts.partitions {
            if let Some(split) = part.as_any().downcast_ref::<SplitInfo>() {
                splits.push(Arc::new(split.clone()));
            }
        }

        //  Build copy pipeline.
        let settings = ctx.get_settings();
        let schema = stage_table_info.schema.clone();
        let stage_info = stage_table_info.stage_info.clone();
        let operator = StageTable::get_op(&stage_table_info.stage_info)?;
        let compact_threshold = self.get_block_compact_thresholds_with_default();
        let on_error_map = match ctx.get_on_error_map() {
            Some(m) => m,
            None => {
                let m = Arc::new(DashMap::new());
                ctx.set_on_error_map(m.clone());
                m
            }
        };
        let input_ctx = Arc::new(InputContext::try_create_from_copy(
            operator,
            settings,
            schema,
            stage_info,
            splits,
            ctx.get_scan_progress(),
            compact_threshold,
            on_error_map,
        )?);

        input_ctx.format.exec_copy(input_ctx.clone(), pipeline)?;
        Ok(())
    }

    fn append_data(
        &self,
        ctx: Arc<dyn TableContext>,
        pipeline: &mut Pipeline,
        _: AppendMode,
        _: bool,
    ) -> Result<()> {
        let single = self.table_info.stage_info.copy_options.single;
        let op = StageTable::get_op(&self.table_info.stage_info)?;

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
    #[async_backtrace::framed]
    async fn commit_insertion(
        &self,
        _ctx: Arc<dyn TableContext>,
        _operations: Vec<DataBlock>,
        _copied_files: Option<UpsertTableCopiedFileReq>,
        _overwrite: bool,
    ) -> Result<()> {
        Ok(())
    }

    // Truncate the stage file.
    #[async_backtrace::framed]
    async fn truncate(&self, _ctx: Arc<dyn TableContext>, _: bool) -> Result<()> {
        Err(ErrorCode::Unimplemented(
            "S3 external table truncate() unimplemented yet!",
        ))
    }

    fn get_block_thresholds(&self) -> BlockThresholds {
        let guard = self.block_compact_threshold.lock();
        (*guard).expect("must success")
    }

    fn set_block_thresholds(&self, thresholds: BlockThresholds) {
        let mut guard = self.block_compact_threshold.lock();
        (*guard) = Some(thresholds)
    }
}
