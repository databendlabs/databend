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

use std::collections::BTreeMap;
use std::fmt::Debug;

use databend_common_exception::ErrorCode;
use log::warn;

use crate::pipelines::executor::PlanNodeMemoryUsage;
use crate::servers::flight::v1::exchange::DataExchangeManager;
use crate::sessions::SessionManager;

pub(super) const TOP_MEMORY_PLAN_NODE_LIMIT: usize = 10;

pub(super) fn out_of_limit_error(error: impl Debug) -> ErrorCode {
    ErrorCode::MemoryExceedsLimit(format!("{error:?}"))
}

pub(super) fn log_memory_limit_diagnostics<C>(cause: &ErrorCode<C>, message: &str) {
    if cause.code() != ErrorCode::MEMORY_EXCEEDS_LIMIT {
        return;
    }

    let mut top_memory_plan_nodes_by_query =
        SessionManager::instance().get_queries_top_memory_plan_nodes(TOP_MEMORY_PLAN_NODE_LIMIT);
    top_memory_plan_nodes_by_query.extend(
        DataExchangeManager::instance()
            .get_queries_top_memory_plan_nodes(TOP_MEMORY_PLAN_NODE_LIMIT),
    );
    let top_memory_plan_nodes_by_query = format_top_memory_plan_nodes_by_query(
        top_memory_plan_nodes_by_query,
        TOP_MEMORY_PLAN_NODE_LIMIT,
    );

    warn!(top_memory_plan_nodes_by_query = top_memory_plan_nodes_by_query; "{message}");
}

fn format_top_memory_plan_nodes_by_query(
    top_memory_plan_nodes_by_query: Vec<(String, Vec<PlanNodeMemoryUsage>)>,
    limit: usize,
) -> String {
    let mut grouped = BTreeMap::<String, Vec<PlanNodeMemoryUsage>>::new();
    for (query_id, plan_nodes) in top_memory_plan_nodes_by_query {
        grouped.entry(query_id).or_default().extend(plan_nodes);
    }

    let mut query_memory = Vec::new();
    let mut top_plan_nodes = Vec::new();
    for (query_id, mut plan_nodes) in grouped {
        let total_current_bytes = plan_nodes
            .iter()
            .map(|plan_node| plan_node.current_bytes)
            .sum::<usize>();
        query_memory.push(format!("({query_id:?}, {total_current_bytes})"));

        plan_nodes.sort_by(|left, right| {
            right
                .current_bytes
                .cmp(&left.current_bytes)
                .then(right.peak_bytes.cmp(&left.peak_bytes))
                .then(left.identity.cmp(&right.identity))
        });
        plan_nodes.truncate(limit);

        let plan_nodes = plan_nodes
            .into_iter()
            .map(|plan_node| {
                format!(
                    "({:?}, {}, {})",
                    plan_node.identity, plan_node.current_bytes, plan_node.peak_bytes
                )
            })
            .collect::<Vec<_>>()
            .join(", ");
        top_plan_nodes.push(format!("query_id: {query_id} [{plan_nodes}]"));
    }

    format!(
        "query_memory: [{}]; top_plan_nodes: {}",
        query_memory.join(", "),
        top_plan_nodes.join("; ")
    )
}

#[cfg(test)]
mod tests {
    use databend_common_base::runtime::OutOfLimit;
    use databend_common_exception::ErrorCode;

    use super::format_top_memory_plan_nodes_by_query;
    use super::out_of_limit_error;
    use crate::pipelines::executor::PlanNodeMemoryUsage;

    #[test]
    fn test_out_of_limit_error_uses_memory_exceeds_limit() {
        let err = out_of_limit_error(OutOfLimit::new(100, 50));

        assert_eq!(err.code(), ErrorCode::MEMORY_EXCEEDS_LIMIT);
        assert_eq!(err.name(), "MemoryExceedsLimit");
        assert!(err.message().contains("memory usage"));
        assert!(err.message().contains("exceeds limit"));
    }

    #[test]
    fn test_format_top_memory_plan_nodes_by_query_groups_query_id() {
        let output = format_top_memory_plan_nodes_by_query(
            vec![
                ("query-b".to_string(), vec![PlanNodeMemoryUsage {
                    identity: "AggregateFinal [#2]".to_string(),
                    current_bytes: 200,
                    peak_bytes: 100,
                }]),
                ("query-a".to_string(), vec![PlanNodeMemoryUsage {
                    identity: "TableScan [#1]".to_string(),
                    current_bytes: 20,
                    peak_bytes: 80,
                }]),
                ("query-b".to_string(), vec![PlanNodeMemoryUsage {
                    identity: "AggregatePartial [#3]".to_string(),
                    current_bytes: 30,
                    peak_bytes: 120,
                }]),
            ],
            10,
        );

        assert_eq!(
            output,
            "query_memory: [(\"query-a\", 20), (\"query-b\", 230)]; top_plan_nodes: query_id: query-a [(\"TableScan [#1]\", 20, 80)]; query_id: query-b [(\"AggregateFinal [#2]\", 200, 100), (\"AggregatePartial [#3]\", 30, 120)]"
        );
    }
}
