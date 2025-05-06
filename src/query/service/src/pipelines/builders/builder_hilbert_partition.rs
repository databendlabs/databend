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

use std::sync::atomic;
use std::sync::atomic::AtomicUsize;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_io::constants::DEFAULT_BLOCK_BUFFER_SIZE;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_transforms::MemorySettings;
use databend_common_sql::executor::physical_plans::HilbertPartition;
use databend_common_sql::executor::physical_plans::MutationKind;
use databend_common_storages_fuse::operations::TransformSerializeBlock;
use databend_common_storages_fuse::statistics::ClusterStatsGenerator;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::FUSE_OPT_KEY_BLOCK_IN_MEM_SIZE_THRESHOLD;
use databend_storages_common_cache::TempDirManager;

use crate::pipelines::memory_settings::MemorySettingsExt;
use crate::pipelines::processors::transforms::CompactStrategy;
use crate::pipelines::processors::transforms::HilbertPartitionExchange;
use crate::pipelines::processors::transforms::TransformWindowPartitionCollect;
use crate::pipelines::PipelineBuilder;
use crate::spillers::SpillerDiskConfig;

impl PipelineBuilder {
    pub(crate) fn build_hilbert_partition(&mut self, partition: &HilbertPartition) -> Result<()> {
        self.build_pipeline(&partition.input)?;
        let num_processors = self.main_pipeline.output_len();
        let table = self
            .ctx
            .build_table_by_table_info(&partition.table_info, None)?;
        let table = FuseTable::try_from_table(table.as_ref())?;

        self.main_pipeline.exchange(
            num_processors,
            HilbertPartitionExchange::create(partition.num_partitions),
        );

        let settings = self.ctx.get_settings();
        let disk_bytes_limit = settings.get_window_partition_spilling_to_disk_bytes_limit()?;
        let temp_dir_manager = TempDirManager::instance();

        let enable_dio = settings.get_enable_dio()?;
        let disk_spill = temp_dir_manager
            .get_disk_spill_dir(disk_bytes_limit, &self.ctx.get_id())
            .map(|temp_dir| SpillerDiskConfig::new(temp_dir, enable_dio))
            .transpose()?;

        let window_spill_settings = MemorySettings::from_window_settings(&self.ctx)?;
        let processor_id = AtomicUsize::new(0);
        let max_bytes_per_block = std::cmp::min(
            4 * table.get_option(
                FUSE_OPT_KEY_BLOCK_IN_MEM_SIZE_THRESHOLD,
                DEFAULT_BLOCK_BUFFER_SIZE,
            ),
            400 * 1024 * 1024,
        );
        self.main_pipeline.add_transform(|input, output| {
            Ok(ProcessorPtr::create(Box::new(
                TransformWindowPartitionCollect::new(
                    self.ctx.clone(),
                    input,
                    output,
                    &settings,
                    processor_id.fetch_add(1, atomic::Ordering::AcqRel),
                    num_processors,
                    partition.num_partitions,
                    window_spill_settings.clone(),
                    disk_spill.clone(),
                    CompactStrategy::new(partition.rows_per_block, max_bytes_per_block),
                )?,
            )))
        })?;

        self.main_pipeline
            .add_transform(|transform_input_port, transform_output_port| {
                let proc = TransformSerializeBlock::try_create(
                    self.ctx.clone(),
                    transform_input_port,
                    transform_output_port,
                    table,
                    ClusterStatsGenerator::default(),
                    MutationKind::Recluster,
                    partition.table_meta_timestamps,
                )?;
                proc.into_processor()
            })
    }
}
