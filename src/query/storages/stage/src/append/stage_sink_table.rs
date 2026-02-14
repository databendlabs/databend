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
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::TableSchemaRef;
use databend_common_meta_app::principal::FileFormatParams;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline_transforms::TransformPipelineHelper;
use databend_storages_common_stage::CopyIntoLocationInfo;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;

use crate::StageTable;
use crate::append::output::SumSummaryTransform;
use crate::append::parquet_file::append_data_to_parquet_files;
use crate::append::partition::PartitionByRuntime;
use crate::append::partition::TransformPartitionBy;
use crate::append::row_based_file::append_data_to_row_based_files;

pub struct StageSinkTable {
    info: CopyIntoLocationInfo,
    schema: TableSchemaRef,
    // This is no used but a placeholder.
    // But the Table trait need it:
    // fn get_table_info(&self) -> &TableInfo).
    table_info_placeholder: TableInfo,
    create_by: String,
}

impl StageSinkTable {
    pub fn create(
        info: CopyIntoLocationInfo,
        schema: TableSchemaRef,
        create_by: String,
    ) -> Result<Arc<dyn Table>> {
        let table_info_placeholder = TableInfo {
            name: "stage_sink".to_string(),
            meta: TableMeta {
                engine: "STAGE_SINK".to_string(),
                ..Default::default()
            },
            ..Default::default()
        }
        .set_schema(schema.clone());
        Ok(Arc::new(Self {
            info,
            schema,
            table_info_placeholder,
            create_by,
        }))
    }

    // partition --> limit size (partition merge blocks) --> writer flush
    pub fn do_append_data(
        &self,
        ctx: Arc<dyn TableContext>,
        pipeline: &mut Pipeline,
    ) -> databend_common_exception::Result<()> {
        let settings = ctx.get_settings();
        let stage_info = &self.info.stage;

        let fmt = self.info.stage.file_format_params.clone();
        if let Some(expr) = &self.info.partition_by {
            let func_ctx = ctx.get_function_context()?;
            let runtime = Arc::new(PartitionByRuntime::try_create(expr.clone(), func_ctx)?);
            pipeline.add_transform(|input, output| {
                TransformPartitionBy::try_create(input, output, runtime.clone())
            })?;
        }

        let mem_limit = settings.get_max_memory_usage()? as usize;
        let mut max_threads = settings.get_max_threads()? as usize;
        if self.info.is_ordered {
            max_threads = 1;
            pipeline.try_resize(1)?;
        }

        let op = StageTable::get_op(stage_info)?;
        let query_id = ctx.get_id();
        let group_id = AtomicUsize::new(0);
        match fmt {
            FileFormatParams::Parquet(_) => append_data_to_parquet_files(
                pipeline,
                self.info.clone(),
                self.schema.clone(),
                op,
                query_id,
                &group_id,
                mem_limit,
                max_threads,
                self.create_by.clone(),
            )?,
            _ => append_data_to_row_based_files(
                pipeline,
                ctx.clone(),
                self.info.clone(),
                self.schema.clone(),
                op,
                query_id,
                &group_id,
                mem_limit,
                max_threads,
            )?,
        };
        if !self.info.options.detailed_output {
            pipeline.try_resize(1)?;
            pipeline.add_accumulating_transformer(SumSummaryTransform::default);
        }
        Ok(())
    }
}

impl Table for StageSinkTable {
    fn as_any(&self) -> &dyn Any {
        todo!()
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info_placeholder
    }

    fn append_data(
        &self,
        ctx: Arc<dyn TableContext>,
        pipeline: &mut Pipeline,
        _table_meta_timestamps: TableMetaTimestamps,
    ) -> databend_common_exception::Result<()> {
        self.do_append_data(ctx, pipeline)
    }
}
