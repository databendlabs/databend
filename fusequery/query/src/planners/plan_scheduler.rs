// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::cmp::min;
use std::sync::Arc;

use arrow::datatypes::Schema;
use common_datavalues::DataSchema;
use common_planners::{EmptyPlan, PlanNode, ReadDataSourcePlan, Statistics};

use crate::error::FuseQueryResult;
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
            0
        } else {
            cluster_nodes.iter().map(|n| n.priority as usize).sum()
        };

        let total_chunks = partitions.len();
        let mut index = 0;
        let mut num_chunks_so_far = 0;
        let mut chunk_size;

        while num_chunks_so_far < total_chunks {
            let mut new_source_plan = source_plan.clone();
            // We have at lease one node
            if priority_sum > 0 {
                let p_usize = cluster_nodes[index].priority as usize;
                let remainder = (p_usize * total_chunks) % priority_sum;
                let left = total_chunks - num_chunks_so_far;
                chunk_size = min(
                    (p_usize * total_chunks - remainder) / priority_sum + 1,
                    left,
                );
                index += 1;
            } else {
                chunk_size = total_chunks;
            }
            new_source_plan.partitions = vec![];
            new_source_plan
                .partitions
                .extend_from_slice(&partitions[num_chunks_so_far..num_chunks_so_far + chunk_size]);
            num_chunks_so_far += chunk_size;

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
        Ok(results)
    }
}
