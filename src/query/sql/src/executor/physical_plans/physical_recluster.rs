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
use databend_storages_common_table_meta::table::ClusterType;

use crate::executor::physical_plans::CommitSink;
use crate::executor::physical_plans::CompactSource;
use crate::executor::physical_plans::Exchange;
use crate::executor::physical_plans::FragmentKind;
use crate::executor::physical_plans::MutationKind;
use crate::executor::PhysicalPlan;
use crate::executor::PhysicalPlanBuilder;
use crate::plans::set_update_stream_columns;
use crate::plans::Plan;
use crate::Planner;

#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct Recluster {
    pub plan_id: u32,
    pub tasks: Vec<ReclusterTask>,
    pub table_info: TableInfo,
}

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug)]
pub struct HilbertSerialize {
    pub plan_id: u32,
    pub input: Box<PhysicalPlan>,
    pub table_info: TableInfo,
}

impl PhysicalPlanBuilder {
    pub async fn build_recluster(
        &mut self,
        recluster: &crate::plans::Recluster,
    ) -> Result<PhysicalPlan> {
        let crate::plans::Recluster {
            catalog,
            database,
            table,
            filters,
            limit,
            hilbert_stmt,
        } = recluster;

        let tbl = self.ctx.get_table(catalog, database, table).await?;
        let push_downs = filters.clone().map(|v| PushDownInfo {
            filters: Some(v),
            ..PushDownInfo::default()
        });
        let table_info = tbl.get_table_info().clone();
        let mut plan = match hilbert_stmt {
            Some(stmt) => {
                let handler = get_hilbert_clustering_handler();
                let Some((recluster_info, snapshot)) = handler
                    .do_hilbert_clustering(tbl.clone(), self.ctx.clone(), push_downs)
                    .await?
                else {
                    return Err(ErrorCode::NoNeedToRecluster(format!(
                        "No need to do recluster for '{database}'.'{table}'"
                    )));
                };

                let mut planner = Planner::new(self.ctx.clone());
                let plan = planner.plan_stmt(stmt).await?;
                let (mut s_expr, metadata, bind_context) = match plan {
                    Plan::Query {
                        s_expr,
                        metadata,
                        bind_context,
                        ..
                    } => (s_expr, metadata, bind_context),
                    v => unreachable!("Input plan must be Query, but it's {}", v),
                };
                self.set_metadata(metadata);
                if tbl.change_tracking_enabled() {
                    *s_expr = set_update_stream_columns(&s_expr)?;
                }
                let plan = self.build(&s_expr, bind_context.column_set()).await?;

                let plan = PhysicalPlan::HilbertSerialize(Box::new(HilbertSerialize {
                    plan_id: 0,
                    input: Box::new(plan),
                    table_info: table_info.clone(),
                }));
                PhysicalPlan::CommitSink(Box::new(CommitSink {
                    input: Box::new(plan),
                    table_info,
                    snapshot: Some(snapshot),
                    mutation_kind: MutationKind::Recluster(ClusterType::Hilbert),
                    update_stream_meta: vec![],
                    merge_meta: false,
                    deduplicated_label: None,
                    plan_id: u32::MAX,
                    recluster_info: Some(recluster_info),
                }))
            }
            None => {
                let Some((parts, snapshot)) =
                    tbl.recluster(self.ctx.clone(), push_downs, *limit).await?
                else {
                    return Err(ErrorCode::NoNeedToRecluster(format!(
                        "No need to do recluster for '{database}'.'{table}'"
                    )));
                };
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
                            mutation_kind: MutationKind::Recluster(ClusterType::Linear),
                            update_stream_meta: vec![],
                            merge_meta: false,
                            deduplicated_label: None,
                            plan_id: u32::MAX,
                            recluster_info: Some(ReclusterInfoSideCar {
                                merged_blocks: remained_blocks,
                                removed_segment_indexes,
                                removed_statistics: removed_segment_summary,
                            }),
                        }))
                    }
                    ReclusterParts::Compact(parts) => {
                        let merge_meta = parts.partitions_type() == PartInfoType::LazyLevel;
                        let mut root = PhysicalPlan::CompactSource(Box::new(CompactSource {
                            parts,
                            table_info: table_info.clone(),
                            column_ids: snapshot.schema.to_leaf_column_id_set(),
                            plan_id: u32::MAX,
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
                            mutation_kind: MutationKind::Compact,
                            update_stream_meta: vec![],
                            merge_meta,
                            deduplicated_label: None,
                            plan_id: u32::MAX,
                            recluster_info: None,
                        }))
                    }
                }
            }
        };
        plan.adjust_plan_id(&mut 0);
        Ok(plan)
    }
}
