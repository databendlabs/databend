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

use databend_common_catalog::cache_policy::CachePolicy;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::Projection;
use databend_common_exception::Result;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_storages_fuse::operations::BlockIdPartitionExchange;
use databend_common_storages_fuse::operations::build_row_fetch_pipeline;
use itertools::Itertools;

use crate::physical_plans::MutationSplit;
use crate::physical_plans::PhysicalPlanCast;
use crate::physical_plans::explain::PlanStatsInfo;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::format::RowFetchFormatter;
use crate::physical_plans::physical_plan::IPhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlanMeta;
use crate::pipelines::PipelineBuilder;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct RowFetch {
    pub meta: PhysicalPlanMeta,
    pub input: PhysicalPlan,
    // cloned from `input`.
    pub source: Box<DataSourcePlan>,
    // projection on the source table schema.
    pub cols_to_fetch: Projection,
    pub row_id_col_offset: usize,
    pub fetched_fields: Vec<DataField>,
    pub need_wrap_nullable: bool,
    /// How the fetched blocks are read; nodes without it in the plan read as queries do.
    #[serde(default)]
    pub cache_policy: CachePolicy,
    /// When true, a block_id repartition is inserted before RowFetch to reduce
    /// duplicate block reads. Applicable to join-based mutation paths (MERGE INTO,
    /// UPDATE...FROM). Not applicable to SELECT+LIMIT where the exchange would
    /// destroy sort order.
    pub enable_block_id_repartition: bool,

    /// Only used for explain
    pub stat_info: Option<PlanStatsInfo>,
}

#[typetag::serde]
impl IPhysicalPlan for RowFetch {
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
        let mut fields = self.input.output_schema()?.fields().clone();
        fields.extend_from_slice(&self.fetched_fields);
        Ok(DataSchemaRefExt::create(fields))
    }

    fn children<'a>(&'a self) -> Box<dyn Iterator<Item = &'a PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&self.input))
    }

    fn children_mut<'a>(&'a mut self) -> Box<dyn Iterator<Item = &'a mut PhysicalPlan> + 'a> {
        Box::new(std::iter::once(&mut self.input))
    }

    fn formatter(&self) -> Result<Box<dyn PhysicalFormat + '_>> {
        Ok(RowFetchFormatter::create(self))
    }

    #[recursive::recursive]
    fn try_find_single_data_source(&self) -> Option<&DataSourcePlan> {
        self.input.try_find_single_data_source()
    }

    fn get_desc(&self) -> Result<String> {
        let table_schema = self.source.source_info.schema();
        let projected_schema = self.cols_to_fetch.project_schema(&table_schema);
        Ok(projected_schema.fields.iter().map(|f| f.name()).join(", "))
    }

    fn derive(&self, mut children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert_eq!(children.len(), 1);
        let input = children.pop().unwrap();
        PhysicalPlan::new(RowFetch {
            meta: self.meta.clone(),
            input,
            source: self.source.clone(),
            cols_to_fetch: self.cols_to_fetch.clone(),
            row_id_col_offset: self.row_id_col_offset,
            fetched_fields: self.fetched_fields.clone(),
            need_wrap_nullable: self.need_wrap_nullable,
            cache_policy: self.cache_policy,
            enable_block_id_repartition: self.enable_block_id_repartition,
            stat_info: self.stat_info.clone(),
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        self.input.build_pipeline(builder)?;

        let max_threads = builder.settings.get_max_threads()? as usize;
        let mixed_matched = MutationSplit::check_physical_plan(&self.input);

        if !mixed_matched {
            // For MatchedOnly MERGE INTO, add block_id repartition before RowFetch
            // to reduce duplicate block reads.
            // Not applicable to SELECT+LIMIT: pipeline.exchange() merges partitions
            // with non-deterministic output order, which would destroy the sort
            // order produced by Sort+Limit.
            if self.enable_block_id_repartition {
                if max_threads > 1
                    && builder
                        .settings
                        .get_enable_mutation_block_id_repartition()?
                {
                    let exchange =
                        Arc::new(BlockIdPartitionExchange::create(self.row_id_col_offset));
                    builder.main_pipeline.exchange(max_threads, exchange)?;
                }
            }
        }

        // MixedMatched path: MutationSplit is upstream, where the repartition already
        // happened, and only its even lanes fetch rows.
        build_row_fetch_pipeline(
            builder.ctx.clone(),
            &mut builder.main_pipeline,
            self.row_id_col_offset,
            &self.source,
            self.cols_to_fetch.clone(),
            self.need_wrap_nullable,
            self.cache_policy,
            |lane| !mixed_matched || lane % 2 == 0,
        )
    }
}
