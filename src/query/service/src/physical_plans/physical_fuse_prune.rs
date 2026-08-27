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

use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::PartitionsShuffleKind;
use databend_common_exception::Result;
use databend_common_expression::DataSchemaRef;
use databend_common_storages_fuse::FuseTable;

use super::physical_plan::IPhysicalPlan;
use super::physical_plan::PhysicalPlan;
use super::physical_plan::PhysicalPlanMeta;
use crate::pipelines::PipelineBuilder;
use crate::servers::flight::v1::exchange::FusePartExchangeInjector;
use crate::sessions::TableContextPartitionStats;
use crate::sessions::TableContextTableFactory;

/// A Fuse source operator that emits only the block partitions surviving pruning.
///
/// Its output consists of empty data blocks carrying `BlockPartitionMeta`; actual block data is
/// read by `FuseBlockRead` after the metadata exchange.
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct FusePrune {
    pub meta: PhysicalPlanMeta,
    pub source: Box<DataSourcePlan>,
}

#[typetag::serde]
impl IPhysicalPlan for FusePrune {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
    }

    fn output_schema(&self) -> Result<DataSchemaRef> {
        Ok(DataSchemaRef::default())
    }

    fn try_find_single_data_source(&self) -> Option<&DataSourcePlan> {
        Some(&self.source)
    }

    fn get_all_data_source(&self, sources: &mut Vec<(u32, Box<DataSourcePlan>)>) {
        sources.push((self.get_id(), self.source.clone()));
    }

    fn set_pruning_stats(&mut self, stats: &mut HashMap<u32, PartStatistics>) {
        if let Some(stat) = stats.remove(&self.get_id()) {
            self.source.statistics = stat;
        }
    }

    fn is_warehouse_distributed_plan(&self) -> bool {
        self.source.parts.kind == PartitionsShuffleKind::BroadcastWarehouse
    }

    fn get_desc(&self) -> Result<String> {
        Ok(format!(
            "{}.{}",
            self.source.source_info.catalog_name(),
            self.source.source_info.desc()
        ))
    }

    fn get_labels(&self) -> Result<HashMap<String, Vec<String>>> {
        Ok(HashMap::from([
            (String::from("Full table name"), vec![format!(
                "{}.{}",
                self.source.source_info.catalog_name(),
                self.source.source_info.desc()
            )]),
            (String::from("Total partitions"), vec![
                self.source.statistics.partitions_total.to_string(),
            ]),
        ]))
    }

    fn derive(&self, children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert!(children.is_empty());
        PhysicalPlan::new(self.clone())
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        let table = builder.ctx.build_table_from_source_plan(&self.source)?;
        builder.ctx.set_partitions(self.source.parts.clone())?;

        if let Some(prune_pipeline) = table.build_prune_pipeline(
            builder.ctx.clone(),
            &self.source,
            &mut builder.main_pipeline,
            self.get_id(),
        )? {
            builder.pipelines.push(prune_pipeline);
        }

        let fuse_table = FuseTable::try_from_table(table.as_ref())?;
        fuse_table.do_read_pruned_partitions(
            builder.ctx.clone(),
            &self.source,
            &mut builder.main_pipeline,
        )?;
        builder.exchange_injector = FusePartExchangeInjector::create();
        Ok(())
    }
}

impl FusePrune {
    pub fn create(source: Box<DataSourcePlan>) -> PhysicalPlan {
        PhysicalPlan::new(FusePrune {
            meta: PhysicalPlanMeta::new("FusePrune"),
            source,
        })
    }
}
