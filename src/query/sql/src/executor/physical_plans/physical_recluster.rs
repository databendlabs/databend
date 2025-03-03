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

use databend_common_catalog::plan::PartInfoType;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::plan::ReclusterInfoSideCar;
use databend_common_catalog::plan::ReclusterParts;
use databend_common_catalog::plan::ReclusterTask;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::TableInfo;
use databend_enterprise_hilbert_clustering::get_hilbert_clustering_handler;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;

use crate::executor::physical_plans::CommitSink;
use crate::executor::physical_plans::CommitType;
use crate::executor::physical_plans::CompactSource;
use crate::executor::physical_plans::Exchange;
use crate::executor::physical_plans::FragmentKind;
use crate::executor::physical_plans::MutationKind;
use crate::executor::PhysicalPlan;
use crate::executor::PhysicalPlanBuilder;
use crate::optimizer::SExpr;
use crate::ColumnSet;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Recluster {
    pub plan_id: u32,
    pub tasks: Vec<ReclusterTask>,
    pub table_info: TableInfo,
    pub table_meta_timestamps: TableMetaTimestamps,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct HilbertSerialize {
    pub plan_id: u32,
    pub input: Box<PhysicalPlan>,
    pub table_info: TableInfo,
    pub table_meta_timestamps: TableMetaTimestamps,
}

impl PhysicalPlanBuilder {
    pub async fn build_recluster(
        &mut self,
        s_expr: &SExpr,
        recluster: &crate::plans::Recluster,
        required: ColumnSet,
    ) -> Result<PhysicalPlan> {
        let crate::plans::Recluster {
            catalog,
            database,
            table,
            filters,
            limit,
        } = recluster;

        let tbl = self.ctx.get_table(catalog, database, table).await?;
        let push_downs = filters.clone().map(|v| PushDownInfo {
            filters: Some(v),
            ..PushDownInfo::default()
        });
        let table_info = tbl.get_table_info().clone();
        let is_hilbert = !s_expr.children.is_empty();
        let commit_type = CommitType::Mutation {
            kind: MutationKind::Recluster,
            merge_meta: false,
        };
        let mut plan = if is_hilbert {
            let handler = get_hilbert_clustering_handler();
            let Some((recluster_info, snapshot)) = handler
                .do_hilbert_clustering(tbl.clone(), self.ctx.clone(), push_downs)
                .await?
            else {
                return Err(ErrorCode::NoNeedToRecluster(format!(
                    "No need to do recluster for '{database}'.'{table}'"
                )));
            };

            let table_meta_timestamps = self
                .ctx
                .get_table_meta_timestamps(tbl.get_id(), Some(snapshot.clone()))?;

            let plan = self.build(s_expr.child(0)?, required).await?;
            let plan = PhysicalPlan::HilbertSerialize(Box::new(HilbertSerialize {
                plan_id: 0,
                input: Box::new(plan),
                table_info: table_info.clone(),
                table_meta_timestamps,
            }));
            PhysicalPlan::CommitSink(Box::new(CommitSink {
                input: Box::new(plan),
                table_info,
                snapshot: Some(snapshot),
                commit_type,
                update_stream_meta: vec![],
                deduplicated_label: None,
                plan_id: u32::MAX,
                recluster_info: Some(recluster_info),
                table_meta_timestamps,
            }))
        } else {
            let Some((parts, snapshot)) =
                tbl.recluster(self.ctx.clone(), push_downs, *limit).await?
            else {
                return Err(ErrorCode::NoNeedToRecluster(format!(
                    "No need to do recluster for '{database}'.'{table}'"
                )));
            };

            let table_meta_timestamps = self
                .ctx
                .get_table_meta_timestamps(tbl.get_id(), Some(snapshot.clone()))?;

            if parts.is_empty() {
                return Err(ErrorCode::NoNeedToRecluster(format!(
                    "No need to do recluster for '{database}'.'{table}'"
                )));
            }

            let is_distributed = parts.is_distributed(self.ctx.clone());
            match parts {
                ReclusterParts::Recluster {
                    tasks,
                    remained_blocks,
                    removed_segment_indexes,
                    removed_segment_summary,
                } => {
                    let mut root = PhysicalPlan::Recluster(Box::new(Recluster {
                        tasks,
                        table_info: table_info.clone(),
                        plan_id: u32::MAX,
                        table_meta_timestamps,
                    }));

                    if is_distributed {
                        root = PhysicalPlan::Exchange(Exchange {
                            plan_id: 0,
                            input: Box::new(root),
                            kind: FragmentKind::Merge,
                            keys: vec![],
                            allow_adjust_parallelism: true,
                            ignore_exchange: false,
                        });
                    }
                    PhysicalPlan::CommitSink(Box::new(CommitSink {
                        input: Box::new(root),
                        table_info,
                        snapshot: Some(snapshot),
                        commit_type,
                        update_stream_meta: vec![],
                        deduplicated_label: None,
                        plan_id: u32::MAX,
                        recluster_info: Some(ReclusterInfoSideCar {
                            merged_blocks: remained_blocks,
                            removed_segment_indexes,
                            removed_statistics: removed_segment_summary,
                        }),
                        table_meta_timestamps,
                    }))
                }
                ReclusterParts::Compact(parts) => {
                    let merge_meta = parts.partitions_type() == PartInfoType::LazyLevel;
                    let mut root = PhysicalPlan::CompactSource(Box::new(CompactSource {
                        parts,
                        table_info: table_info.clone(),
                        column_ids: snapshot.schema.to_leaf_column_id_set(),
                        plan_id: u32::MAX,
                        table_meta_timestamps,
                    }));

                    if is_distributed {
                        root = PhysicalPlan::Exchange(Exchange {
                            plan_id: 0,
                            input: Box::new(root),
                            kind: FragmentKind::Merge,
                            keys: vec![],
                            allow_adjust_parallelism: true,
                            ignore_exchange: false,
                        });
                    }

                    PhysicalPlan::CommitSink(Box::new(CommitSink {
                        input: Box::new(root),
                        table_info,
                        snapshot: Some(snapshot),
                        commit_type: CommitType::Mutation {
                            kind: MutationKind::Compact,
                            merge_meta,
                        },
                        update_stream_meta: vec![],
                        deduplicated_label: None,
                        plan_id: u32::MAX,
                        recluster_info: None,
                        table_meta_timestamps,
                    }))
                }
            }
        };
        plan.adjust_plan_id(&mut 0);
        Ok(plan)
    }
}
