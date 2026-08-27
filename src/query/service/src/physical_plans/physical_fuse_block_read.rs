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
use std::collections::BTreeMap;
use std::collections::HashMap;

use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::InternalColumn;
use databend_common_catalog::plan::Partitions;
use databend_common_exception::Result;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::FieldIndex;
use databend_common_sql::IndexType;
use databend_common_sql::executor::physical_plans::FragmentKind;
use databend_common_storages_fuse::FuseTable;

use super::Exchange;
use super::FusePrune;
use super::TableScan;
use super::explain::PlanStatsInfo;
use super::physical_plan::IPhysicalPlan;
use super::physical_plan::PhysicalPlan;
use super::physical_plan::PhysicalPlanMeta;
use super::physical_table_scan::build_scan_output_pipeline;
use crate::pipelines::PipelineBuilder;
use crate::servers::flight::v1::exchange::FusePartExchangeInjector;
use crate::sessions::TableContextTableFactory;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct FuseBlockRead {
    pub meta: PhysicalPlanMeta,
    pub input: PhysicalPlan,
    pub scan_id: usize,
    pub name_mapping: BTreeMap<String, String>,
    pub source: Box<DataSourcePlan>,
    pub internal_column: Option<BTreeMap<FieldIndex, InternalColumn>>,
    pub table_index: Option<IndexType>,
    pub stat_info: Option<PlanStatsInfo>,
}

#[typetag::serde]
impl IPhysicalPlan for FuseBlockRead {
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
        TableScan::output_fields(self.source.schema(), &self.name_mapping).map(DataSchema::new_ref)
    }

    fn children(&self) -> Box<dyn Iterator<Item = &'_ PhysicalPlan> + '_> {
        Box::new(std::iter::once(&self.input))
    }

    fn children_mut(&mut self) -> Box<dyn Iterator<Item = &'_ mut PhysicalPlan> + '_> {
        Box::new(std::iter::once(&mut self.input))
    }

    fn try_find_single_data_source(&self) -> Option<&DataSourcePlan> {
        Some(&self.source)
    }

    fn is_distributed_plan(&self) -> bool {
        true
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
            (
                format!(
                    "Columns ({} / {})",
                    self.output_schema()?.num_fields(),
                    std::cmp::max(
                        self.output_schema()?.num_fields(),
                        self.source.source_info.schema().num_fields(),
                    )
                ),
                self.name_mapping.keys().cloned().collect(),
            ),
        ]))
    }

    fn derive(&self, mut children: Vec<PhysicalPlan>) -> PhysicalPlan {
        assert_eq!(children.len(), 1);
        PhysicalPlan::new(FuseBlockRead {
            meta: self.meta.clone(),
            input: children.pop().unwrap(),
            scan_id: self.scan_id,
            name_mapping: self.name_mapping.clone(),
            source: self.source.clone(),
            internal_column: self.internal_column.clone(),
            table_index: self.table_index,
            stat_info: self.stat_info.clone(),
        })
    }

    fn build_pipeline2(&self, builder: &mut PipelineBuilder) -> Result<()> {
        let old_injector = builder.exchange_injector.clone();
        builder.exchange_injector = FusePartExchangeInjector::create();
        self.input.build_pipeline(builder)?;
        builder.exchange_injector = old_injector;

        let table = builder.ctx.build_table_from_source_plan(&self.source)?;
        let fuse_table = FuseTable::try_from_table(table.as_ref())?;
        fuse_table.do_read_data_from_partitions(
            builder.ctx.clone(),
            &self.source,
            &mut builder.main_pipeline,
            true,
        )?;
        build_scan_output_pipeline(
            builder,
            &self.source,
            &self.name_mapping,
            &self.internal_column,
        )
    }
}

impl FuseBlockRead {
    pub fn create(scan: TableScan) -> PhysicalPlan {
        let TableScan {
            scan_id,
            name_mapping,
            mut source,
            internal_column,
            table_index,
            stat_info,
            ..
        } = scan;

        let input = PhysicalPlan::new(Exchange {
            meta: PhysicalPlanMeta::new("Exchange"),
            input: FusePrune::create(source.clone()),
            kind: FragmentKind::Normal,
            keys: vec![],
            ignore_exchange: false,
            allow_adjust_parallelism: false,
        });

        // The destination reader only needs schema, push-downs and snapshot information. Keeping
        // the global lazy segment list here would duplicate it into every destination fragment.
        source.parts = Partitions::default();

        PhysicalPlan::new(FuseBlockRead {
            meta: PhysicalPlanMeta::new("FuseBlockRead"),
            input,
            scan_id,
            name_mapping,
            source,
            internal_column,
            table_index,
            stat_info,
        })
    }
}
