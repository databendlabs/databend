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

use databend_common_exception::Result;
use databend_common_pipeline_transforms::processors::build_compact_block_no_split_pipeline;
use databend_common_sql::executor::physical_plans::HilbertSerialize;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_storages_factory::Table;
use databend_common_storages_fuse::operations::TransformSerializeBlock;
use databend_common_storages_fuse::statistics::ClusterStatsGenerator;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::TableContext;

use crate::pipelines::PipelineBuilder;

impl PipelineBuilder {
    pub(crate) fn build_hilbert_serialize(&mut self, serialize: &HilbertSerialize) -> Result<()> {
        self.build_pipeline(&serialize.input)?;
        let table = self
            .ctx
            .build_table_by_table_info(&serialize.table_info, None)?;
        let table = FuseTable::try_from_table(table.as_ref())?;

        let block_thresholds = table.get_block_thresholds();
        let max_threads = self.ctx.get_settings().get_max_threads()? as usize;
        build_compact_block_no_split_pipeline(
            &mut self.main_pipeline,
            block_thresholds,
            max_threads,
        )?;

        self.main_pipeline
            .add_transform(|transform_input_port, transform_output_port| {
                let proc = TransformSerializeBlock::try_create(
                    self.ctx.clone(),
                    transform_input_port,
                    transform_output_port,
                    table,
                    ClusterStatsGenerator::default(),
                    MutationKind::Recluster,
                    serialize.table_meta_timestamps,
                )?;
                proc.into_processor()
            })
    }
}
