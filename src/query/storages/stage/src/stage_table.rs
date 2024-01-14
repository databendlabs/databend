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
use std::io::Read;
use std::ops::Deref;
use std::sync::Arc;

use dashmap::DashMap;
use databend_common_catalog::plan::DataSourceInfo;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartInfo;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PartitionsShuffleKind;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::plan::StageTableInfo;
use databend_common_catalog::table::AppendMode;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockThresholds;
use databend_common_expression::TableSchemaRefExt;
use databend_common_meta_app::principal::FileFormatParams;
use databend_common_meta_app::principal::StageInfo;
use databend_common_meta_app::schema::TableInfo;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_sources::input_formats::InputContext;
use databend_common_pipeline_sources::input_formats::SplitInfo;
use databend_common_storage::init_stage_operator;
use databend_common_storage::StageFileInfo;
use databend_common_storage::STDIN_FD;
use databend_common_storages_parquet::ParquetTableForCopy;
use log::debug;
use opendal::Operator;
use opendal::Scheme;
use parking_lot::Mutex;

/// TODO: we need to track the data metrics in stage table.
pub struct StageTable {
    pub(crate) table_info: StageTableInfo,
    // This is no used but a placeholder.
    // But the Table trait need it:
    // fn get_table_info(&self) -> &TableInfo).
    table_info_placeholder: TableInfo,
    block_compact_threshold: Mutex<Option<BlockThresholds>>,
}

impl StageTable {
    pub fn try_create(table_info: StageTableInfo) -> Result<Arc<dyn Table>> {
        let table_info_placeholder = TableInfo {
            ident: Default::default(),
            desc: "".to_string(),
            // `system.stage` is used to forbidden the user to select * from text files.
            name: "stage".to_string(),
            meta: Default::default(),
            tenant: "".to_string(),
            db_type: Default::default(),
        }
        .set_schema(table_info.schema());

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
        stage_info.list_files(max_files).await
    }

    fn get_block_compact_thresholds_with_default(&self) -> BlockThresholds {
        let guard = self.block_compact_threshold.lock();
        guard.deref().unwrap_or_default()
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
        _dry_run: bool,
    ) -> Result<(PartStatistics, Partitions)> {
        let stage_info = &self.table_info;
        if matches!(
            stage_info.stage_info.file_format_params,
            FileFormatParams::Parquet(_)
        ) {
            return ParquetTableForCopy::do_read_partitions(stage_info, ctx, _push_downs).await;
        }
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
        _put_cache: bool,
    ) -> Result<()> {
        let stage_table_info =
            if let DataSourceInfo::StageSource(stage_table_info) = &plan.source_info {
                stage_table_info
            } else {
                return Err(ErrorCode::Internal(""));
            };

        if matches!(
            stage_table_info.stage_info.file_format_params,
            FileFormatParams::Parquet(_)
        ) {
            return ParquetTableForCopy::do_read_data(ctx, plan, pipeline, _put_cache);
        }

        let projection = if let Some(PushDownInfo {
            projection: Some(Projection::Columns(columns)),
            ..
        }) = &plan.push_downs
        {
            Some(columns.clone())
        } else {
            None
        };

        let mut splits = vec![];
        for part in &plan.parts.partitions {
            if let Some(split) = part.as_any().downcast_ref::<SplitInfo>() {
                splits.push(Arc::new(split.clone()));
            }
        }

        //  Build copy pipeline.
        let settings = ctx.get_settings();
        let fields = stage_table_info
            .schema
            .fields()
            .iter()
            .filter(|f| f.computed_expr().is_none())
            .cloned()
            .collect::<Vec<_>>();
        let schema = TableSchemaRefExt::create(fields);
        let stage_info = stage_table_info.stage_info.clone();
        let operator = StageTable::get_op(&stage_table_info.stage_info)?;
        let compact_threshold = self.get_block_compact_thresholds_with_default();
        let on_error_map = ctx.get_on_error_map().unwrap_or_else(|| {
            let m = Arc::new(DashMap::new());
            ctx.set_on_error_map(m.clone());
            m
        });

        // inject stdin to memory
        if operator.info().scheme() == Scheme::Memory {
            let mut buffer = vec![];
            std::io::stdin().lock().read_to_end(&mut buffer)?;

            let bop = operator.blocking();
            bop.write(STDIN_FD, buffer)?;
        }

        let input_ctx = Arc::new(InputContext::try_create_from_copy(
            ctx.clone(),
            operator,
            settings,
            schema,
            stage_info,
            splits,
            ctx.get_scan_progress(),
            compact_threshold,
            on_error_map,
            self.table_info.is_select,
            projection,
            self.table_info.default_values.clone(),
        )?);
        debug!("start copy splits feeder in {}", ctx.get_cluster().local_id);
        input_ctx.format.exec_copy(input_ctx.clone(), pipeline)?;
        Ok(())
    }

    fn append_data(
        &self,
        ctx: Arc<dyn TableContext>,
        pipeline: &mut Pipeline,
        _: AppendMode,
    ) -> Result<()> {
        self.do_append_data(ctx, pipeline)
    }

    // Truncate the stage file.
    #[async_backtrace::framed]
    async fn truncate(&self, _ctx: Arc<dyn TableContext>) -> Result<()> {
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
