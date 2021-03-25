// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::sync::Arc;

use arrow::datatypes::Schema;

use crate::datasources::Statistics;
use crate::datavalues::DataSchema;
use crate::error::FuseQueryResult;
use crate::planners::{EmptyPlan, PlanNode, ReadDataSourcePlan};
use crate::sessions::FuseQueryContextRef;

pub struct PlanScheduler {}

impl PlanScheduler {
    pub fn schedule(ctx: FuseQueryContextRef, plan: &PlanNode) -> FuseQueryResult<Vec<PlanNode>> {
        let mut source_plan = ReadDataSourcePlan {
            db: "".to_string(),
            table: "".to_string(),
            schema: Arc::new(Schema::empty()),
            partitions: vec![],
            statistics: Statistics::default(),
            description: "".to_string(),
        };

        // Get the source plan node from walk.
        plan.walk_postorder(|plan| match plan {
            PlanNode::ReadSource(node) => {
                source_plan = node.clone();
                Ok(false)
            }
            _ => Ok(true),
        })?;

        // If partition numbers <= current node cpus, schedule all the partitions to current node.
        let partitions = source_plan.partitions.clone();
        let max_threads = ctx.get_max_threads()? as usize;
        if max_threads > partitions.len() {
            return Ok(vec![plan.clone()]);
        }

        let mut results = vec![];
        let cluster = ctx.try_get_cluster()?;
        let cluster_nodes = cluster.get_nodes()?;
        let priority_sum = if cluster_nodes.is_empty() {
            1
        } else {
            cluster_nodes.iter().map(|n| n.priority).sum()
        };

        let total = partitions.len();
        let num_nodes = cluster_nodes.len();
        let mut index = 0;
        let mut num_chunks_so_far = 0;
        while index < num_nodes {
            let mut new_source_plan = source_plan.clone();
            let remainder = ((cluster_nodes[index].priority as usize) * total) % (priority_sum as usize);
            let chunk_size = ((cluster_nodes[index].priority as usize) * total - remainder) / (priority_sum as usize) + 1;
            new_source_plan.partitions.clone_from_slice(&partitions[num_chunks_so_far..num_chunks_so_far+chunk_size]);
            num_chunks_so_far += chunk_size;
            index += 1;

            let mut rewritten_node = PlanNode::Empty(EmptyPlan {
                schema: Arc::new(DataSchema::empty()),
            });

            // Walk and rewrite the plan from the source.
            plan.walk_postorder(|node| {
                if let PlanNode::ReadSource(_) = node {
                    rewritten_node = PlanNode::ReadSource(new_source_plan.clone());
                } else {
                    let mut clone_node = node.clone();
                    clone_node.set_input(&rewritten_node)?;
                    rewritten_node = clone_node;
                }
                Ok(true)
            })?;
            results.push(rewritten_node);
        }
        assert_eq!(num_chunks_so_far, total);
        Ok(results)
    }
}
