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
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::SortColumnDescription;
use databend_common_pipeline_core::processors::Processor;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_transforms::MemorySettings;
use databend_storages_common_cache::TempDirManager;

use crate::physical_plans::IPhysicalPlan;
use crate::physical_plans::Window;
use crate::physical_plans::WindowPartition;
use crate::pipelines::memory_settings::MemorySettingsExt;
use crate::pipelines::processors::transforms::FrameBound;
use crate::pipelines::processors::transforms::SortStrategy;
use crate::pipelines::processors::transforms::TransformWindow;
use crate::pipelines::processors::transforms::TransformWindowPartitionCollect;
use crate::pipelines::processors::transforms::WindowFunctionInfo;
use crate::pipelines::processors::transforms::WindowPartitionExchange;
use crate::pipelines::processors::transforms::WindowPartitionTopNExchange;
use crate::pipelines::processors::transforms::WindowSortDesc;
use crate::pipelines::PipelineBuilder;
use crate::spillers::SpillerDiskConfig;

impl PipelineBuilder {

    pub(crate) fn build_window_partition(
        &mut self,
        window_partition: &WindowPartition,
    ) -> Result<()> {
        self.build_pipeline(&window_partition.input)?;

        let num_processors = self.main_pipeline.output_len();

        // Settings.
        let settings = self.ctx.get_settings();
        let num_partitions = settings.get_window_num_partitions()?;

        let plan_schema = window_partition.output_schema()?;

        let partition_by = window_partition
            .partition_by
            .iter()
            .map(|index| plan_schema.index_of(&index.to_string()))
            .collect::<Result<Vec<_>>>()?;

        let sort_desc = window_partition
            .order_by
            .iter()
            .map(|desc| {
                let offset = plan_schema.index_of(&desc.order_by.to_string())?;
                Ok(SortColumnDescription {
                    offset,
                    asc: desc.asc,
                    nulls_first: desc.nulls_first,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        if let Some(top_n) = &window_partition.top_n
            && top_n.top < 10000
        {
            self.main_pipeline.exchange(
                num_processors,
                WindowPartitionTopNExchange::create(
                    partition_by.clone(),
                    sort_desc.clone(),
                    top_n.top,
                    top_n.func,
                    num_partitions as u64,
                ),
            )
        } else {
            self.main_pipeline.exchange(
                num_processors,
                WindowPartitionExchange::create(partition_by.clone(), num_partitions),
            );
        }

        let temp_dir_manager = TempDirManager::instance();
        let disk_bytes_limit = settings.get_window_partition_spilling_to_disk_bytes_limit()?;
        let enable_dio = settings.get_enable_dio()?;
        let disk_spill = temp_dir_manager
            .get_disk_spill_dir(disk_bytes_limit, &self.ctx.get_id())
            .map(|temp_dir| SpillerDiskConfig::new(temp_dir, enable_dio))
            .transpose()?;

        let have_order_col = window_partition.after_exchange.unwrap_or(false);
        let window_spill_settings = MemorySettings::from_window_settings(&self.ctx)?;

        let processor_id = AtomicUsize::new(0);
        self.main_pipeline.add_transform(|input, output| {
            let strategy = SortStrategy::try_create(
                &settings,
                sort_desc.clone(),
                plan_schema.clone(),
                have_order_col,
            )?;
            Ok(ProcessorPtr::create(Box::new(
                TransformWindowPartitionCollect::new(
                    self.ctx.clone(),
                    input,
                    output,
                    &settings,
                    processor_id.fetch_add(1, atomic::Ordering::AcqRel),
                    num_processors,
                    num_partitions,
                    window_spill_settings.clone(),
                    disk_spill.clone(),
                    strategy,
                )?,
            )))
        })
    }
}
