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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_sql::plans::CacheSource;
use databend_common_sql::ColumnSet;

use crate::physical_plans::format::CacheScanFormatter;
use crate::physical_plans::format::PhysicalFormat;
use crate::physical_plans::physical_plan::IPhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlanMeta;
use crate::physical_plans::physical_plan_builder::PhysicalPlanBuilder;
use crate::pipelines::processors::transforms::CacheSourceState;
use crate::pipelines::processors::transforms::HashJoinCacheState;
use crate::pipelines::processors::transforms::NewHashJoinCacheState;
use crate::pipelines::processors::transforms::TransformCacheScan;
use crate::pipelines::HashJoinStateRef;
use crate::pipelines::PipelineBuilder;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct CacheScan {
    pub meta: PhysicalPlanMeta,
    pub cache_source: CacheSource,
    pub output_schema: DataSchemaRef,
}

#[typetag::serde]
impl IPhysicalPlan for CacheScan {
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
        Ok(self.output_schema.clone())
    }

    fn formatter(&self) -> Result<Box<dyn PhysicalFormat + '_>> {
        Ok(CacheScanFormatter::create(self))
    }

    fn derive(&self, children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert!(children.is_empty());
        PhysicalPlan::new(CacheScan {
            meta: self.meta.clone(),
            cache_source: self.cache_source.clone(),
            output_schema: self.output_schema.clone(),
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        let max_threads = builder.settings.get_max_threads()?;
        let max_block_size = builder.settings.get_max_block_size()? as usize;
        let cache_source_state = match &self.cache_source {
            CacheSource::HashJoinBuild((cache_index, column_indexes)) => {
                match builder.hash_join_states.get(cache_index) {
                    Some(HashJoinStateRef::OldHashJoinState(hash_join_state)) => {
                        CacheSourceState::OldHashJoinCacheState(HashJoinCacheState::new(
                            column_indexes.clone(),
                            hash_join_state.clone(),
                            max_block_size,
                        ))
                    }
                    Some(HashJoinStateRef::NewHashJoinState(hash_join_state)) => {
                        CacheSourceState::NewHashJoinCacheState(NewHashJoinCacheState::new(
                            column_indexes.clone(),
                            hash_join_state.clone(),
                        ))
                    }
                    None => {
                        return Err(ErrorCode::Internal(
                            "Hash join state not found during building cache scan".to_string(),
                        ));
                    }
                }
            }
        };

        builder.main_pipeline.add_source(
            |output| {
                TransformCacheScan::create(builder.ctx.clone(), output, cache_source_state.clone())
            },
            max_threads as usize,
        )
    }
}

impl PhysicalPlanBuilder {
    pub async fn build_cache_scan(
        &mut self,
        scan: &databend_common_sql::plans::CacheScan,
        required: ColumnSet,
    ) -> Result<PhysicalPlan> {
        // 1. Prune unused Columns.
        let used: ColumnSet = required.intersection(&scan.columns).cloned().collect();
        let (cache_source, fields) = if used == scan.columns {
            (scan.cache_source.clone(), scan.schema.fields().clone())
        } else {
            let new_scan = scan.prune_columns(used);
            (
                new_scan.cache_source.clone(),
                new_scan.schema.fields().clone(),
            )
        };
        // 2. Build physical plan.
        Ok(PhysicalPlan::new(CacheScan {
            cache_source,
            meta: PhysicalPlanMeta::new("CacheScan"),
            output_schema: DataSchemaRefExt::create(fields),
        }))
    }
}
