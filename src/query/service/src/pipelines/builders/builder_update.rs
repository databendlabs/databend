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

use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_pipeline_sources::EmptySource;
use databend_common_pipeline_transforms::processors::TransformPipelineHelper;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_sql::executor::physical_plans::UpdateSource;
use databend_common_sql::StreamContext;
use databend_common_storages_fuse::operations::TransformSerializeBlock;
use databend_common_storages_fuse::FuseTable;

use crate::pipelines::processors::TransformAddStreamColumns;
use crate::pipelines::PipelineBuilder;

impl PipelineBuilder {
    pub(crate) fn build_update_source(&mut self, update: &UpdateSource) -> Result<()> {
        let table =
            self.ctx
                .build_table_by_table_info(&update.catalog_info, &update.table_info, None)?;
        let table = FuseTable::try_from_table(table.as_ref())?;

        if update.parts.is_empty() {
            return self.main_pipeline.add_source(EmptySource::create, 1);
        }
        self.ctx.set_partitions(update.parts.clone())?;
        let filter = update.filters.clone().map(|v| v.filter);
        table.add_update_source(
            self.ctx.clone(),
            filter,
            update.col_indices.clone(),
            update.update_list.clone(),
            update.computed_list.clone(),
            update.query_row_id_col,
            &mut self.main_pipeline,
        )?;

        if table.change_tracking_enabled() {
            let stream_ctx = StreamContext::try_create(
                self.ctx.get_function_context()?,
                table.schema_with_stream(),
                table.get_table_info().ident.seq,
                false,
            )?;
            self.main_pipeline
                .add_transformer(|| TransformAddStreamColumns::new(stream_ctx.clone()));
        }

        let block_thresholds = table.get_block_thresholds();
        // sort
        let cluster_stats_gen = table.cluster_gen_for_append(
            self.ctx.clone(),
            &mut self.main_pipeline,
            block_thresholds,
            None,
        )?;

        self.main_pipeline.add_transform(|input, output| {
            let proc = TransformSerializeBlock::try_create(
                self.ctx.clone(),
                input,
                output,
                table,
                cluster_stats_gen.clone(),
                MutationKind::Update,
            )?;
            proc.into_processor()
        })
    }
}
