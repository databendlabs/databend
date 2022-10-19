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

use common_datablocks::DataBlock;
use common_exception::ErrorCode;
use common_exception::Result;
use common_legacy_planners::Extras;
use common_legacy_planners::Partitions;
use common_legacy_planners::ReadDataSourcePlan;
use common_legacy_planners::StageTableInfo;
use common_legacy_planners::Statistics;
use common_meta_app::schema::TableInfo;
use common_meta_types::StageType;
use common_meta_types::UserStageInfo;
use common_pipeline_sources::processors::sources::input_formats::InputContext;
use common_storage::init_operator;
use opendal::layers::SubdirLayer;
use opendal::Operator;
use parking_lot::Mutex;
use tracing::info;

use super::stage_table_sink::StageTableSink;
use crate::pipelines::processors::TransformLimit;
use crate::pipelines::Pipeline;
use crate::sessions::TableContext;
use crate::storages::Table;

pub struct StageTable {
    table_info: StageTableInfo,
    // This is no used but a placeholder.
    // But the Table trait need it:
    // fn get_table_info(&self) -> &TableInfo).
    table_info_placeholder: TableInfo,
    input_context: Mutex<Option<Arc<InputContext>>>,
}

impl StageTable {
    pub fn try_create(table_info: StageTableInfo) -> Result<Arc<dyn Table>> {
        let table_info_placeholder = TableInfo::default().set_schema(table_info.schema());

        Ok(Arc::new(Self {
            table_info,
            table_info_placeholder,
            input_context: Default::default(),
        }))
    }

    fn get_input_context(&self) -> Option<Arc<InputContext>> {
        let guard = self.input_context.lock();
        guard.clone()
    }

    /// Get operator with correctly prefix.
    pub fn get_op(ctx: &Arc<dyn TableContext>, stage: &UserStageInfo) -> Result<Operator> {
        if stage.stage_type == StageType::Internal {
            let prefix = format!("/stage/{}/", stage.stage_name);
            let pop = ctx.get_data_operator()?.operator();
            Ok(pop.layer(SubdirLayer::new(&prefix)))
        } else {
            Ok(init_operator(&stage.stage_params.storage)?)
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
    ) -> Result<(Statistics, Partitions)> {
        let operator = StageTable::get_op(&ctx, &self.table_info.stage_info)?;
        let input_ctx = Arc::new(
            InputContext::try_create_from_copy(
                operator,
                ctx.get_settings().clone(),
                ctx.get_format_settings()?,
                self.table_info.schema.clone(),
                self.table_info.stage_info.clone(),
                self.table_info.files.clone(),
                ctx.get_scan_progress(),
            )
            .await?,
        );
        info!("copy into {:?}", input_ctx);
        let mut guard = self.input_context.lock();
        *guard = Some(input_ctx);
        Ok((Statistics::default(), vec![]))
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
        Err(ErrorCode::UnImplement(
            "S3 external table truncate() unimplemented yet!",
        ))
    }
}
