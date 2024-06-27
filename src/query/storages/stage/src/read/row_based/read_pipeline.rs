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

use std::sync::Arc;

use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::plan::StageTableInfo;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::BlockThresholds;
use databend_common_meta_app::principal::StageFileCompression;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_sources::input_formats::InputContext;
use databend_common_pipeline_sources::EmptySource;
use databend_common_pipeline_sources::PrefetchAsyncSourcer;
use databend_common_pipeline_transforms::processors::TransformPipelineHelper;
use databend_common_settings::Settings;
use databend_common_storage::init_stage_operator;

use crate::read::load_context::LoadContext;
use crate::read::row_based::format::create_row_based_file_format;
use crate::read::row_based::processors::BlockBuilder;
use crate::read::row_based::processors::BytesReader;
use crate::read::row_based::processors::Decompressor;
use crate::read::row_based::processors::Separator;

pub struct RowBasedReadPipelineBuilder<'a> {
    pub(crate) stage_table_info: &'a StageTableInfo,
    pub(crate) compact_threshold: BlockThresholds,
}

impl RowBasedReadPipelineBuilder<'_> {
    fn build_read_stage_source(
        &self,
        ctx: Arc<dyn TableContext>,
        pipeline: &mut Pipeline,
        settings: &Settings,
        num_threads: usize,
    ) -> Result<()> {
        let operator = init_stage_operator(&self.stage_table_info.stage_info)?;
        let batch_size = settings.get_input_read_buffer_size()? as usize;
        pipeline.add_source(
            |output| {
                let reader = BytesReader::try_create(ctx.clone(), operator.clone(), batch_size, 1)?;
                PrefetchAsyncSourcer::create(ctx.clone(), output, reader)
            },
            num_threads,
        )?;
        Ok(())
    }

    // processors:
    // 1. BytesReader
    // 2. (optional) Decompressor
    // 3. Separator: cut file into RowBatches(bytes with row/field ends)
    // 4. (resize to threads): so row batches can be processed in parallel, regardless of the file it from.
    // 5. BlockBuilder: the slow part most of the time
    // make sure data from the same file is process in the same pipe in seq in step 1,2,3
    pub fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
    ) -> Result<()> {
        if plan.parts.is_empty() {
            // no file match
            pipeline.add_source(EmptySource::create, 1)?;
            return Ok(());
        };

        let pos_projection = if let Some(PushDownInfo {
            projection: Some(Projection::Columns(columns)),
            ..
        }) = &plan.push_downs
        {
            Some(columns.clone())
        } else {
            None
        };
        let settings = ctx.get_settings();
        ctx.set_partitions(plan.parts.clone())?;

        let max_threads = settings.get_max_threads()? as usize;
        let num_sources = std::cmp::min(max_threads, plan.parts.len());
        self.build_read_stage_source(ctx.clone(), pipeline, &settings, num_sources)?;

        let format =
            create_row_based_file_format(&self.stage_table_info.stage_info.file_format_params);

        let load_ctx = Arc::new(LoadContext::try_create(
            ctx.clone(),
            self.stage_table_info,
            pos_projection,
            self.compact_threshold,
        )?);

        match self
            .stage_table_info
            .stage_info
            .file_format_params
            .compression()
        {
            StageFileCompression::None => {}
            compression => {
                let algo = InputContext::get_compression_alg_copy(compression, "")?;
                pipeline.try_add_accumulating_transformer(|| {
                    Decompressor::try_create(load_ctx.clone(), algo)
                })?;
            }
        }

        pipeline.try_add_accumulating_transformer(|| {
            Separator::try_create(load_ctx.clone(), format.clone())
        })?;

        // todo(youngsofun): no need to resize if it is unlikely to be unbalanced
        pipeline.try_resize(max_threads)?;

        pipeline
            .try_add_accumulating_transformer(|| BlockBuilder::create(load_ctx.clone(), &format))?;

        Ok(())
    }
}
