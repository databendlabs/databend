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
use std::collections::HashSet;

use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartInfoType;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::table::TableExt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::DataSchemaRef;
use databend_common_meta_app::schema::TableInfo;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;

use crate::physical_plans::physical_plan::DeriveHandle;
use crate::physical_plans::physical_plan::IPhysicalPlan;
use crate::physical_plans::physical_plan::PhysicalPlanMeta;
use crate::physical_plans::CommitSink;
use crate::physical_plans::CommitType;
use crate::physical_plans::Exchange;
use crate::physical_plans::FragmentKind;
use crate::physical_plans::MutationKind;
use crate::physical_plans::PhysicalPlanBuilder;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct CompactSource {
    pub meta: PhysicalPlanMeta,
    pub parts: Partitions,
    pub table_info: TableInfo,
    pub column_ids: HashSet<ColumnId>,
    pub table_meta_timestamps: TableMetaTimestamps,
}

#[typetag::serde]
impl IPhysicalPlan for CompactSource {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn get_meta(&self) -> &PhysicalPlanMeta {
        &self.meta
    }

    fn get_meta_mut(&mut self) -> &mut PhysicalPlanMeta {
        &mut self.meta
    }

    fn derive(&self, children: Vec<Box<dyn IPhysicalPlan>>) -> Box<dyn IPhysicalPlan> {
        assert!(children.is_empty());
        Box::new(self.clone())
    }
}

impl PhysicalPlanBuilder {
    pub async fn build_compact_block(
        &mut self,
        compact_block: &databend_common_sql::plans::OptimizeCompactBlock,
    ) -> Result<Box<dyn IPhysicalPlan>> {
        let databend_common_sql::plans::OptimizeCompactBlock {
            catalog,
            database,
            table,
            limit,
        } = compact_block;

        let tenant = self.ctx.get_tenant();
        let catalog = self.ctx.get_catalog(catalog).await?;
        let tbl = catalog.get_table(&tenant, database, table).await?;
        // check mutability
        tbl.check_mutable()?;

        let table_info = tbl.get_table_info().clone();

        let Some((parts, snapshot)) = tbl.compact_blocks(self.ctx.clone(), limit.clone()).await?
        else {
            return Err(ErrorCode::NoNeedToCompact(format!(
                "No need to do compact for '{database}'.'{table}'"
            )));
        };

        let table_meta_timestamps = self
            .ctx
            .get_table_meta_timestamps(tbl.as_ref(), Some(snapshot.clone()))?;

        let merge_meta = parts.partitions_type() == PartInfoType::LazyLevel;
        let mut root: Box<dyn IPhysicalPlan> = Box::new(CompactSource {
            parts,
            table_info: table_info.clone(),
            column_ids: snapshot.schema.to_leaf_column_id_set(),
            table_meta_timestamps,
            meta: PhysicalPlanMeta::new("ConstantTableScan"),
        });

        let is_distributed = (!self.ctx.get_cluster().is_empty())
            && self.ctx.get_settings().get_enable_distributed_compact()?;
        if is_distributed {
            root = Box::new(Exchange {
                input: root,
                kind: FragmentKind::Merge,
                keys: vec![],
                allow_adjust_parallelism: true,
                ignore_exchange: false,
                meta: PhysicalPlanMeta::new("ConstantTableScan"),
            });
        }

        root = Box::new(CommitSink {
            input: root,
            table_info,
            snapshot: Some(snapshot),
            commit_type: CommitType::Mutation {
                kind: MutationKind::Compact,
                merge_meta,
            },
            update_stream_meta: vec![],
            deduplicated_label: None,
            recluster_info: None,
            table_meta_timestamps,
            meta: PhysicalPlanMeta::new("CommitSink"),
        });

        root.adjust_plan_id(&mut 0);
        Ok(root)
    }
}
