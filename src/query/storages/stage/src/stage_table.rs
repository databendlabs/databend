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
use std::sync::atomic::AtomicUsize;
use std::sync::Arc;

use common_base::base::uuid;
use common_catalog::plan::Extras;
use common_catalog::plan::PartStatistics;
use common_catalog::plan::Partitions;
use common_catalog::plan::ReadDataSourcePlan;
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
use common_pipeline_sources::processors::sources::input_formats::InputContext;
use common_pipeline_transforms::processors::transforms::TransformLimit;
use common_storage::init_operator;
use opendal::layers::SubdirLayer;
use opendal::Operator;
use parking_lot::Mutex;
use tracing::debug;

use crate::stage_table_sink::StageTableSink;

/// TODO: we need to track the data metrics in stage table.
pub struct StageTable {
    table_info: StageTableInfo,
    // This is no used but a placeholder.
    // But the Table trait need it:
    // fn get_table_info(&self) -> &TableInfo).
    table_info_placeholder: TableInfo,
    input_context: Mutex<Option<Arc<InputContext>>>,
    block_compact_threshold: Mutex<Option<BlockCompactThresholds>>,
}

impl StageTable {
    pub fn try_create(table_info: StageTableInfo) -> Result<Arc<dyn Table>> {
        let table_info_placeholder = TableInfo::default().set_schema(table_info.schema());

        Ok(Arc::new(Self {
            table_info,
            table_info_placeholder,
            input_context: Default::default(),
            block_compact_threshold: Default::default(),
        }))
    }

    fn get_input_context(&self) -> Option<Arc<InputContext>> {
        let guard = self.input_context.lock();
        guard.clone()
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
        _push_downs: Option<Extras>,
    ) -> Result<(PartStatistics, Partitions)> {
        let operator = StageTable::get_op(&ctx, &self.table_info.stage_info)?;
        let input_ctx = Arc::new(
            InputContext::try_create_from_copy(
                operator,
                ctx.get_settings().clone(),
                self.table_info.schema.clone(),
                self.table_info.stage_info.clone(),
                self.table_info.files.clone(),
                ctx.get_scan_progress(),
                self.get_block_compact_thresholds(),
            )
            .await?,
        );
        debug!("copy into {:?}", input_ctx);
        let mut guard = self.input_context.lock();
        *guard = Some(input_ctx);
        Ok((PartStatistics::default(), vec![]))
    }

    fn read_data(
        &self,
        _ctx: Arc<dyn TableContext>,
        _plan: &ReadDataSourcePlan,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        let input_ctx = self.get_input_context().unwrap();
        input_ctx.format.exec_copy(input_ctx.clone(), pipeline)?;

        let limit = self.table_info.stage_info.copy_options.size_limit;
        if limit > 0 {
            pipeline.resize(1)?;
            pipeline.add_transform(|transform_input_port, transform_output_port| {
                TransformLimit::try_create(
                    Some(limit),
                    0,
                    transform_input_port,
                    transform_output_port,
                )
            })?;
        }
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
        let op = StageTable::get_op(&ctx, &self.table_info.stage_info)?;

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
