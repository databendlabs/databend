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

use std::collections::HashSet;

use super::BroadcastSink;
use super::Exchange;
use super::FuseBlockRead;
use super::MaterializedCTE;
use super::PhysicalPlan;
use super::PhysicalPlanCast;
use super::TableScan;

/// Inject the Fuse metadata exchange only after the optimizer has finalized the row-data
/// exchanges. A scan inside an existing exchange input can be split into a pruning source and a
/// reader fragment that runs on all executors. A scan in the root fragment may be coordinator-only
/// and must keep the original `TableScan` execution path.
pub fn optimize_distributed_fuse_pruning(
    plan: &PhysicalPlan,
    eligible_scan_ids: &HashSet<usize>,
) -> PhysicalPlan {
    if eligible_scan_ids.is_empty() {
        return plan.clone();
    }

    rewrite(plan, eligible_scan_ids, false)
}

#[recursive::recursive]
fn rewrite(
    plan: &PhysicalPlan,
    eligible_scan_ids: &HashSet<usize>,
    can_host_distributed_reader: bool,
) -> PhysicalPlan {
    if can_host_distributed_reader
        && let Some(scan) = TableScan::from_physical_plan(plan)
        && eligible_scan_ids.contains(&scan.scan_id)
    {
        return FuseBlockRead::create(scan.clone());
    }

    // Replacing a scan inside an Exchange input splits that non-root fragment into a pruning
    // source and an intermediate reader fragment. Fragmenter schedules the reader on all
    // executors, so every metadata exchange destination has a receiver. Materialized CTE and
    // broadcast sink inputs are also non-root fragments without requiring a parent Exchange.
    let children_can_host_distributed_reader = can_host_distributed_reader
        || Exchange::check_physical_plan(plan)
        || MaterializedCTE::check_physical_plan(plan)
        || BroadcastSink::check_physical_plan(plan);
    let children = plan
        .children()
        .map(|child| {
            rewrite(
                child,
                eligible_scan_ids,
                children_can_host_distributed_reader,
            )
        })
        .collect();

    plan.derive(children)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use databend_common_catalog::plan::DataSourceInfo;
    use databend_common_catalog::plan::DataSourcePlan;
    use databend_common_catalog::plan::PartStatistics;
    use databend_common_catalog::plan::Partitions;
    use databend_common_expression::TableSchema;
    use databend_common_meta_app::schema::TableInfo;
    use databend_common_sql::executor::physical_plans::FragmentKind;

    use super::*;
    use crate::physical_plans::FusePrune;
    use crate::physical_plans::PhysicalPlanMeta;

    fn table_scan(scan_id: usize) -> PhysicalPlan {
        let schema = Arc::new(TableSchema::empty());
        let mut table_info = TableInfo::simple("default", "t", schema.clone());
        table_info.meta.engine = "FUSE".to_string();
        let source = DataSourcePlan {
            source_info: DataSourceInfo::TableSource(table_info),
            output_schema: schema,
            parts: Partitions::default(),
            statistics: PartStatistics::default(),
            description: String::new(),
            tbl_args: None,
            push_downs: None,
            internal_columns: None,
            base_block_ids: None,
            block_meta_options: Default::default(),
            table_index: 0,
            scan_id,
        };

        TableScan::create(
            scan_id,
            Default::default(),
            Box::new(source),
            None,
            None,
            None,
        )
    }

    #[test]
    fn injects_metadata_exchange_only_into_distributed_fragments() {
        let eligible = HashSet::from([1]);

        let local = optimize_distributed_fuse_pruning(&table_scan(1), &eligible);
        assert!(TableScan::check_physical_plan(&local));

        let distributed = PhysicalPlan::new(Exchange {
            meta: PhysicalPlanMeta::new("Exchange"),
            input: table_scan(1),
            kind: FragmentKind::Merge,
            keys: vec![],
            ignore_exchange: false,
            allow_adjust_parallelism: true,
        });
        let distributed = optimize_distributed_fuse_pruning(&distributed, &eligible);

        let outer_exchange = Exchange::from_physical_plan(&distributed).unwrap();
        let block_read = FuseBlockRead::from_physical_plan(&outer_exchange.input).unwrap();
        let metadata_exchange = Exchange::from_physical_plan(&block_read.input).unwrap();
        assert_eq!(metadata_exchange.kind, FragmentKind::Normal);
        assert!(FusePrune::check_physical_plan(&metadata_exchange.input));
    }
}
