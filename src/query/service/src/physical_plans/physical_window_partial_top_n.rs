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

use databend_common_catalog::plan::DataSourcePlan;
use databend_common_exception::Result;
use databend_common_expression::SortColumnDescription;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline_transforms::AccumulatingTransformer;
use databend_common_settings::Settings;
use databend_common_sql::Symbol;
use databend_common_sql::executor::physical_plans::SortDesc;

use crate::physical_plans::Exchange;
use crate::physical_plans::PhysicalPlanCast;
use crate::physical_plans::WindowPartitionTopN;
use crate::physical_plans::explain::PlanStatsInfo;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::format::WindowPartialTopNFormatter;
use crate::physical_plans::physical_plan::IPhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlanMeta;
use crate::pipelines::PipelineBuilder;
use crate::pipelines::processors::transforms::TransformWindowPartialTopN;
use crate::pipelines::processors::transforms::WindowPartitionExchange;

/// Pre-exchange reduction for a `WindowPartition` with a pushed-down ranking
/// top-n. It runs on the sending side of the hash exchange (or directly below
/// the window partition in standalone mode) and keeps, per partition key,
/// only the rows that can still rank within the top-n.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct WindowPartialTopN {
    pub meta: PhysicalPlanMeta,
    pub input: PhysicalPlan,
    pub partition_by: Vec<Symbol>,
    pub order_by: Vec<SortDesc>,
    pub top_n: WindowPartitionTopN,

    pub stat_info: Option<PlanStatsInfo>,
}

impl WindowPartialTopN {
    /// Wrap `input` with the partial top-n when the setting allows it. When
    /// `input` is a hash exchange the reduction goes below it so the rows are
    /// dropped before they cross the network.
    pub fn wrap_input(
        settings: &Settings,
        input: PhysicalPlan,
        partition_by: &[Symbol],
        order_by: &[SortDesc],
        top_n: Option<&WindowPartitionTopN>,
        stat_info: Option<&PlanStatsInfo>,
    ) -> Result<PhysicalPlan> {
        let Some(top_n) = top_n else {
            return Ok(input);
        };
        if !settings.get_enable_window_partial_top_n()? || partition_by.is_empty() {
            return Ok(input);
        }

        let partial = |input: PhysicalPlan| {
            PhysicalPlan::new(WindowPartialTopN {
                meta: PhysicalPlanMeta::new("WindowPartialTopN"),
                input,
                partition_by: partition_by.to_vec(),
                order_by: order_by.to_vec(),
                top_n: top_n.clone(),
                stat_info: stat_info.cloned(),
            })
        };

        if let Some(exchange) = Exchange::from_physical_plan(&input) {
            return Ok(PhysicalPlan::new(Exchange {
                meta: exchange.meta.clone(),
                input: partial(exchange.input.clone()),
                kind: exchange.kind.clone(),
                keys: exchange.keys.clone(),
                ignore_exchange: exchange.ignore_exchange,
                allow_adjust_parallelism: exchange.allow_adjust_parallelism,
            }));
        }

        Ok(partial(input))
    }
}

#[typetag::serde]
impl IPhysicalPlan for WindowPartialTopN {
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
        Ok(WindowPartialTopNFormatter::create(self))
    }

    #[recursive::recursive]
    fn try_find_single_data_source(&self) -> Option<&DataSourcePlan> {
        self.input.try_find_single_data_source()
    }

    fn derive(&self, mut children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert_eq!(children.len(), 1);
        let input = children.pop().unwrap();
        PhysicalPlan::new(WindowPartialTopN {
            meta: self.meta.clone(),
            input,
            partition_by: self.partition_by.clone(),
            order_by: self.order_by.clone(),
            top_n: self.top_n.clone(),
            stat_info: self.stat_info.clone(),
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        self.input.build_pipeline(builder)?;

        let settings = &builder.settings;
        let num_processors = builder.main_pipeline.output_len();
        let num_partitions = settings.get_window_num_partitions()?;
        let max_memory = settings
            .get_window_partial_top_n_memory_mb()?
            .saturating_mul(1024 * 1024) as usize;
        let passthrough_ratio =
            settings.get_window_partial_top_n_passthrough_ratio()? as f64 / 100.0;

        let schema = self.output_schema()?;
        let partition_by = self
            .partition_by
            .iter()
            .map(|index| schema.index_of(&index.to_string()))
            .collect::<Result<Vec<_>>>()?;
        let order_by = self
            .order_by
            .iter()
            .map(|desc| {
                let offset = schema.index_of(&desc.order_by.to_string())?;
                Ok(SortColumnDescription {
                    offset,
                    asc: desc.asc,
                    nulls_first: desc.nulls_first,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        // Route every row of a partition to one thread first, so each thread
        // holds candidates for its own share of the keys instead of all of
        // them.
        builder.main_pipeline.exchange(
            num_processors,
            WindowPartitionExchange::create(partition_by.clone(), num_partitions),
        )?;

        builder.main_pipeline.add_transform(|input, output| {
            Ok(ProcessorPtr::create(AccumulatingTransformer::create(
                input,
                output,
                TransformWindowPartialTopN::new(
                    partition_by.clone(),
                    order_by.clone(),
                    self.top_n.top,
                    self.top_n.func,
                    max_memory,
                    passthrough_ratio,
                ),
            )))
        })
    }
}
