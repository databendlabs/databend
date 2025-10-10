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
use std::sync::atomic;
use std::sync::atomic::AtomicUsize;

use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::SortColumnDescription;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_transforms::MemorySettings;
use databend_common_sql::executor::physical_plans::SortDesc;
use databend_common_sql::IndexType;
use databend_storages_common_cache::TempDirManager;

use crate::physical_plans::explain::PlanStatsInfo;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::format::WindowPartitionFormatter;
use crate::physical_plans::physical_plan::IPhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlanMeta;
use crate::physical_plans::physical_sort::SortStep;
use crate::pipelines::memory_settings::MemorySettingsExt;
use crate::pipelines::processors::transforms::SortStrategy;
use crate::pipelines::processors::transforms::TransformWindowPartitionCollect;
use crate::pipelines::processors::transforms::WindowPartitionExchange;
use crate::pipelines::processors::transforms::WindowPartitionTopNExchange;
use crate::pipelines::PipelineBuilder;
use crate::spillers::SpillerDiskConfig;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct WindowPartition {
    pub meta: PhysicalPlanMeta,
    pub input: PhysicalPlan,
    pub partition_by: Vec<IndexType>,
    pub order_by: Vec<SortDesc>,
    pub sort_step: SortStep,
    pub top_n: Option<WindowPartitionTopN>,

    pub stat_info: Option<PlanStatsInfo>,
}

#[typetag::serde]
impl IPhysicalPlan for WindowPartition {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
    }

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&self.input))
    }

    fn children_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&mut self.input))
    }

    fn formatter(&self) -> Result<Box<dyn PhysicalFormat + '_>> {
        Ok(WindowPartitionFormatter::create(self))
    }

    #[recursive::recursive]
    fn try_find_single_data_source(&self) -> Option<&DataSourcePlan> {
        self.input.try_find_single_data_source()
    }

    fn derive(&self, mut children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert_eq!(children.len(), 1);
        let input = children.pop().unwrap();
        PhysicalPlan::new(WindowPartition {
            meta: self.meta.clone(),
            input,
            partition_by: self.partition_by.clone(),
            order_by: self.order_by.clone(),
            sort_step: self.sort_step,
            top_n: self.top_n.clone(),
            stat_info: self.stat_info.clone(),
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        self.input.build_pipeline(builder)?;

        let num_processors = builder.main_pipeline.output_len();

        // Settings.
        let settings = builder.settings.clone();
        let num_partitions = builder.settings.get_window_num_partitions()?;

        let plan_schema = self.output_schema()?;

        let partition_by = self
            .partition_by
            .iter()
            .map(|index| plan_schema.index_of(&index.to_string()))
            .collect::<Result<Vec<_>>>()?;

        let sort_desc = self
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

        if let Some(top_n) = &self.top_n
            && top_n.top < 10000
        {
            builder.main_pipeline.exchange(
                num_processors,
                WindowPartitionTopNExchange::create(
                    partition_by.clone(),
                    sort_desc.clone(),
                    top_n.top,
                    top_n.func,
                    num_partitions as u64,
                ),
            )?;
        } else {
            builder.main_pipeline.exchange(
                num_processors,
                WindowPartitionExchange::create(partition_by.clone(), num_partitions),
            )?;
        }

        let temp_dir_manager = TempDirManager::instance();
        let disk_bytes_limit = settings.get_window_partition_spilling_to_disk_bytes_limit()?;
        let enable_dio = settings.get_enable_dio()?;
        let disk_spill = temp_dir_manager
            .get_disk_spill_dir(disk_bytes_limit, &builder.ctx.get_id())
            .map(|temp_dir| SpillerDiskConfig::new(temp_dir, enable_dio))
            .transpose()?;

        let have_order_col = match self.sort_step {
            SortStep::Single | SortStep::Partial => false,
            SortStep::Final => true,
            _ => unimplemented!(),
        };
        let window_spill_settings = MemorySettings::from_window_settings(&builder.ctx)?;
        let enable_backpressure_spiller = settings.get_enable_backpressure_spiller()?;

        let processor_id = AtomicUsize::new(0);
        builder.main_pipeline.add_transform(|input, output| {
            let strategy = SortStrategy::try_create(
                &settings,
                sort_desc.clone(),
                plan_schema.clone(),
                have_order_col,
            )?;
            Ok(ProcessorPtr::create(Box::new(
                TransformWindowPartitionCollect::new(
                    builder.ctx.clone(),
                    input,
                    output,
                    &settings,
                    processor_id.fetch_add(1, atomic::Ordering::AcqRel),
                    num_processors,
                    num_partitions,
                    window_spill_settings.clone(),
                    disk_spill.clone(),
                    enable_backpressure_spiller,
                    strategy,
                )?,
            )))
        })
    }
}

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct WindowPartitionTopN {
    pub func: WindowPartitionTopNFunc,
    pub top: usize,
}

#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
pub enum WindowPartitionTopNFunc {
    RowNumber,
    Rank,
    DenseRank,
}
