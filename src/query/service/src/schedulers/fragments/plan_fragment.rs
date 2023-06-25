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

use std::sync::Arc;

use common_catalog::plan::DataSourcePlan;
use common_catalog::plan::Partitions;
use common_exception::ErrorCode;
use common_exception::Result;
use common_sql::executor::DeletePartial;
use common_sql::executor::DistributedCopyIntoTable;

use crate::api::DataExchange;
use crate::schedulers::Fragmenter;
use crate::schedulers::QueryFragmentAction;
use crate::schedulers::QueryFragmentActions;
use crate::schedulers::QueryFragmentsActions;
use crate::sessions::QueryContext;
use crate::sql::executor::PhysicalPlan;
use crate::sql::executor::PhysicalPlanReplacer;
use crate::sql::executor::TableScan;

/// Type of plan fragment
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum FragmentType {
    /// Root fragment of a query plan
    Root,

    /// Intermediate fragment of a query plan,
    /// doesn't contain any `TableScan` operator.
    Intermediate,

    /// Leaf fragment of a query plan, which contains
    /// a `TableScan` operator.
    Source,
    /// Leaf fragment of a delete plan, which contains
    /// a `DeletePartial` operator.
    DeleteLeaf,
}

#[derive(Clone)]
pub struct PlanFragment {
    pub plan: PhysicalPlan,
    pub fragment_type: FragmentType,
    pub fragment_id: usize,
    pub exchange: Option<DataExchange>,
    pub query_id: String,

    // The fragments to ask data from.
    pub source_fragments: Vec<PlanFragment>,
}

impl PlanFragment {
    pub fn get_actions(
        &self,
        ctx: Arc<QueryContext>,
        actions: &mut QueryFragmentsActions,
    ) -> Result<()> {
        for input in self.source_fragments.iter() {
            input.get_actions(ctx.clone(), actions)?;
        }

        let mut fragment_actions = QueryFragmentActions::create(self.fragment_id);

        match &self.fragment_type {
            FragmentType::Root => {
                let action = QueryFragmentAction::create(
                    Fragmenter::get_local_executor(ctx),
                    self.plan.clone(),
                );
                fragment_actions.add_action(action);
                if let Some(ref exchange) = self.exchange {
                    fragment_actions.set_exchange(exchange.clone());
                }
                actions.add_fragment_actions(fragment_actions)?;
            }
            FragmentType::Intermediate => {
                if self
                    .source_fragments
                    .iter()
                    .any(|fragment| matches!(&fragment.exchange, Some(DataExchange::Merge(_))))
                {
                    // If this is a intermediate fragment with merge input,
                    // we will only send it to coordinator node.
                    let action = QueryFragmentAction::create(
                        Fragmenter::get_local_executor(ctx),
                        self.plan.clone(),
                    );
                    fragment_actions.add_action(action);
                } else {
                    // Otherwise distribute the fragment to all the executors.
                    for executor in Fragmenter::get_executors(ctx) {
                        let action = QueryFragmentAction::create(executor, self.plan.clone());
                        fragment_actions.add_action(action);
                    }
                }
                if let Some(ref exchange) = self.exchange {
                    fragment_actions.set_exchange(exchange.clone());
                }
                actions.add_fragment_actions(fragment_actions)?;
            }
            FragmentType::Source => {
                // Redistribute partitions
                let mut fragment_actions = self.redistribute_source_fragment(ctx)?;
                if let Some(ref exchange) = self.exchange {
                    fragment_actions.set_exchange(exchange.clone());
                }
                actions.add_fragment_actions(fragment_actions)?;
            }
            FragmentType::DeleteLeaf => {
                let mut fragment_actions = self.redistribute_delete_leaf(ctx)?;
                if let Some(ref exchange) = self.exchange {
                    fragment_actions.set_exchange(exchange.clone());
                }
                actions.add_fragment_actions(fragment_actions)?;
            }
        }

        Ok(())
    }

    /// Redistribute partitions of current source fragment to executors.
    fn redistribute_source_fragment(&self, ctx: Arc<QueryContext>) -> Result<QueryFragmentActions> {
        if self.fragment_type != FragmentType::Source {
            return Err(ErrorCode::Internal(
                "Cannot redistribute a non-source fragment".to_string(),
            ));
        }

        let read_source = self.get_read_source()?;

        let executors = Fragmenter::get_executors(ctx);
        // Redistribute partitions of ReadDataSourcePlan.
        let mut fragment_actions = QueryFragmentActions::create(self.fragment_id);

        let partitions = &read_source.parts;
        let partition_reshuffle = partitions.reshuffle(executors)?;

        for (executor, parts) in partition_reshuffle.iter() {
            let mut new_read_source = read_source.clone();
            new_read_source.parts = parts.clone();
            let mut plan = self.plan.clone();

            // Replace `ReadDataSourcePlan` with rewritten one and generate new fragment for it.
            let mut replace_read_source = ReplaceReadSource {
                source: new_read_source,
            };
            plan = replace_read_source.replace(&plan)?;

            fragment_actions
                .add_action(QueryFragmentAction::create(executor.clone(), plan.clone()));
        }

        Ok(fragment_actions)
    }

    fn redistribute_delete_leaf(&self, ctx: Arc<QueryContext>) -> Result<QueryFragmentActions> {
        let plan = match &self.plan {
            PhysicalPlan::ExchangeSink(plan) => plan,
            _ => unreachable!("logic error"),
        };
        let plan = match plan.input.as_ref() {
            PhysicalPlan::DeletePartial(plan) => plan,
            _ => unreachable!("logic error"),
        };
        let partitions = &plan.parts;
        let executors = Fragmenter::get_executors(ctx);
        let mut fragment_actions = QueryFragmentActions::create(self.fragment_id);
        let partition_reshuffle = partitions.reshuffle(executors)?;

        for (executor, parts) in partition_reshuffle.iter() {
            let mut plan = self.plan.clone();

            let mut replace_delete_partial = ReplaceDeletePartial {
                partitions: parts.clone(),
            };
            plan = replace_delete_partial.replace(&plan)?;

            fragment_actions
                .add_action(QueryFragmentAction::create(executor.clone(), plan.clone()));
        }

        Ok(fragment_actions)
    }

    fn get_read_source(&self) -> Result<DataSourcePlan> {
        if self.fragment_type != FragmentType::Source {
            return Err(ErrorCode::Internal(
                "Cannot get read source from a non-source fragment".to_string(),
            ));
        }

        let mut source = vec![];

        let mut collect_read_source = |plan: &PhysicalPlan| {
            if let PhysicalPlan::TableScan(scan) = plan {
                source.push(*scan.source.clone())
            } else if let PhysicalPlan::DistributedCopyIntoTable(distributed_plan) = plan {
                source.push(*distributed_plan.source.clone())
            }
        };

        PhysicalPlan::traverse(
            &self.plan,
            &mut |_| true,
            &mut collect_read_source,
            &mut |_| {},
        );

        if source.len() != 1 {
            Err(ErrorCode::Internal(
                "Invalid source fragment with multiple table scan".to_string(),
            ))
        } else {
            Ok(source.remove(0))
        }
    }
}

pub struct ReplaceReadSource {
    pub source: DataSourcePlan,
}

impl PhysicalPlanReplacer for ReplaceReadSource {
    fn replace_table_scan(&mut self, plan: &TableScan) -> Result<PhysicalPlan> {
        Ok(PhysicalPlan::TableScan(TableScan {
            plan_id: plan.plan_id,
            source: Box::new(self.source.clone()),
            name_mapping: plan.name_mapping.clone(),
            table_index: plan.table_index,
            stat_info: plan.stat_info.clone(),
            internal_column: plan.internal_column.clone(),
        }))
    }

    fn replace_copy_into_table(&mut self, plan: &DistributedCopyIntoTable) -> Result<PhysicalPlan> {
        Ok(PhysicalPlan::DistributedCopyIntoTable(
            DistributedCopyIntoTable {
                plan_id: plan.plan_id,
                catalog_name: plan.catalog_name.clone(),
                database_name: plan.database_name.clone(),
                table_name: plan.table_name.clone(),
                required_values_schema: plan.required_values_schema.clone(),
                values_consts: plan.values_consts.clone(),
                required_source_schema: plan.required_source_schema.clone(),
                write_mode: plan.write_mode,
                validation_mode: plan.validation_mode.clone(),
                force: plan.force,
                stage_table_info: plan.stage_table_info.clone(),
                source: Box::new(self.source.clone()),
                thresholds: plan.thresholds,
                files: plan.files.clone(),
                table_info: plan.table_info.clone(),
            },
        ))
    }
}

struct ReplaceDeletePartial {
    pub partitions: Partitions,
}

impl PhysicalPlanReplacer for ReplaceDeletePartial {
    fn replace_delete_partial(&mut self, plan: &DeletePartial) -> Result<PhysicalPlan> {
        Ok(PhysicalPlan::DeletePartial(Box::new(DeletePartial {
            parts: self.partitions.clone(),
            ..plan.clone()
        })))
    }
}
