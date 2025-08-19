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
use std::collections::HashMap;
use std::sync::Arc;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
#[allow(unused_imports)]
use databend_common_expression::DataBlock;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::HashTableConfig;
use databend_common_expression::LimitType;
use databend_common_expression::SortColumnDescription;
use databend_common_functions::aggregates::AggregateFunctionFactory;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_transforms::TransformPipelineHelper;
use databend_common_pipeline_transforms::TransformSortPartial;
use databend_common_sql::executor::physical_plans::AggregateFunctionDesc;
use databend_common_sql::executor::physical_plans::SortDesc;
use databend_common_sql::IndexType;
use databend_common_storage::DataOperator;
use itertools::Itertools;

use crate::physical_plans::explain::PlanStatsInfo;
use crate::physical_plans::format::AggregatePartialFormatter;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::physical_plan::IPhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlanMeta;
use crate::pipelines::processors::transforms::aggregator::AggregateInjector;
use crate::pipelines::processors::transforms::aggregator::PartialSingleStateAggregator;
use crate::pipelines::processors::transforms::aggregator::TransformAggregateSpillWriter;
use crate::pipelines::processors::transforms::aggregator::TransformPartialAggregate;
use crate::pipelines::PipelineBuilder;

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct AggregatePartial {
    pub meta: PhysicalPlanMeta,
    pub input: PhysicalPlan,
    pub group_by: Vec<IndexType>,
    pub agg_funcs: Vec<AggregateFunctionDesc>,
    pub enable_experimental_aggregate_hashtable: bool,
    pub group_by_display: Vec<String>,

    // Order by keys if keys are subset of group by key, then we can use rank to filter data in previous
    pub rank_limit: Option<(Vec<SortDesc>, usize)>,
    // Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

#[typetag::serde]
impl IPhysicalPlan for AggregatePartial {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
    }

    #[recursive::recursive]
    fn output_schema(&self) -> Result<DataSchemaRef> {
        let input_schema = self.input.output_schema()?;

        let mut fields = Vec::with_capacity(self.agg_funcs.len() + self.group_by.len());
        let factory = AggregateFunctionFactory::instance();

        for desc in &self.agg_funcs {
            let name = desc.output_column.to_string();

            if desc.sig.udaf.is_some() {
                fields.push(DataField::new(
                    &name,
                    DataType::Tuple(vec![DataType::Binary]),
                ));
                continue;
            }

            let func = factory
                .get(
                    &desc.sig.name,
                    desc.sig.params.clone(),
                    desc.sig.args.clone(),
                    desc.sig.sort_descs.clone(),
                )
                .unwrap();

            fields.push(DataField::new(&name, func.serialize_data_type()))
        }

        for (idx, field) in self.group_by.iter().zip(
            self.group_by
                .iter()
                .map(|index| input_schema.field_with_name(&index.to_string())),
        ) {
            fields.push(DataField::new(&idx.to_string(), field?.data_type().clone()));
        }

        Ok(DataSchemaRefExt::create(fields))
    }

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&self.input))
    }

    fn children_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&mut self.input))
    }

    fn formatter(&self) -> Result<Box<dyn PhysicalFormat + '_>> {
        Ok(AggregatePartialFormatter::create(self))
    }

    fn get_desc(&self) -> Result<String> {
        Ok(self.agg_funcs.iter().map(|x| x.display.clone()).join(", "))
    }

    fn get_labels(&self) -> Result<HashMap<String, Vec<String>>> {
        let mut labels = HashMap::with_capacity(2);

        if !self.group_by_display.is_empty() {
            labels.insert(String::from("Grouping keys"), self.group_by_display.clone());
        }

        if !self.agg_funcs.is_empty() {
            labels.insert(
                String::from("Aggregate Functions"),
                self.agg_funcs.iter().map(|x| x.display.clone()).collect(),
            );
        }

        Ok(labels)
    }

    fn derive(&self, mut children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert_eq!(children.len(), 1);

        PhysicalPlan::new(AggregatePartial {
            input: children.remove(0),
            meta: self.meta.clone(),
            group_by: self.group_by.clone(),
            agg_funcs: self.agg_funcs.clone(),
            enable_experimental_aggregate_hashtable: self.enable_experimental_aggregate_hashtable,
            group_by_display: self.group_by_display.clone(),
            rank_limit: self.rank_limit.clone(),
            stat_info: self.stat_info.clone(),
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        builder.contain_sink_processor = true;
        self.input.build_pipeline(builder)?;

        let max_block_size = builder.settings.get_max_block_size()?;
        let max_threads = builder.settings.get_max_threads()?;
        let max_spill_io_requests = builder.settings.get_max_spill_io_requests()?;

        let enable_experimental_aggregate_hashtable = builder
            .settings
            .get_enable_experimental_aggregate_hashtable()?;

        let params = PipelineBuilder::build_aggregator_params(
            self.input.output_schema()?,
            &self.group_by,
            &self.agg_funcs,
            enable_experimental_aggregate_hashtable,
            builder.is_exchange_parent(),
            max_block_size as usize,
            max_spill_io_requests as usize,
        )?;

        if params.group_columns.is_empty() {
            return builder.main_pipeline.try_add_accumulating_transformer(|| {
                PartialSingleStateAggregator::try_new(&params)
            });
        }

        let schema_before_group_by = params.input_schema.clone();

        // Need a global atomic to read the max current radix bits hint
        let partial_agg_config = if !builder.is_exchange_parent() {
            HashTableConfig::default().with_partial(true, max_threads as usize)
        } else {
            HashTableConfig::default()
                .cluster_with_partial(true, builder.ctx.get_cluster().nodes.len())
        };

        // For rank limit, we can filter data using sort with rank before partial
        if let Some(rank_limit) = &self.rank_limit {
            let sort_desc = rank_limit
                .0
                .iter()
                .map(|desc| {
                    let offset = schema_before_group_by.index_of(&desc.order_by.to_string())?;
                    Ok(SortColumnDescription {
                        offset,
                        asc: desc.asc,
                        nulls_first: desc.nulls_first,
                    })
                })
                .collect::<Result<Vec<_>>>()?;
            let sort_desc: Arc<[_]> = sort_desc.into();

            builder.main_pipeline.add_transformer(|| {
                TransformSortPartial::new(LimitType::LimitRank(rank_limit.1), sort_desc.clone())
            });
        }

        builder.main_pipeline.add_transform(|input, output| {
            Ok(ProcessorPtr::create(TransformPartialAggregate::try_create(
                builder.ctx.clone(),
                input,
                output,
                params.clone(),
                partial_agg_config.clone(),
            )?))
        })?;

        // If cluster mode, spill write will be completed in exchange serialize, because we need scatter the block data first
        if !builder.is_exchange_parent() {
            let operator = DataOperator::instance().spill_operator();
            let location_prefix = builder.ctx.query_id_spill_prefix();

            builder.main_pipeline.add_transform(|input, output| {
                Ok(ProcessorPtr::create(
                    TransformAggregateSpillWriter::try_create(
                        builder.ctx.clone(),
                        input,
                        output,
                        operator.clone(),
                        params.clone(),
                        location_prefix.clone(),
                    )?,
                ))
            })?;
        }

        builder.exchange_injector = AggregateInjector::create(builder.ctx.clone(), params.clone());
        Ok(())
    }
}
