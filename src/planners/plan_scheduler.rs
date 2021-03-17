// Copyright 2020-2021 The FuseQuery Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use crate::datasources::Statistics;
use crate::datavalues::DataSchema;
use crate::error::FuseQueryResult;
use crate::planners::{EmptyPlan, PlanNode, ReadDataSourcePlan};
use crate::sessions::FuseQueryContextRef;
use arrow::datatypes::Schema;
use std::sync::Arc;

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
        let cluster_nums = if cluster_nodes.is_empty() {
            1
        } else {
            cluster_nodes.len()
        };

        // Align to the cluster numbers.
        let align = (partitions.len() % cluster_nums) / cluster_nums + 1;
        let chunk_size = (partitions.len() + align) / (cluster_nums);
        for chunks in partitions.chunks(chunk_size) {
            let mut new_source_plan = source_plan.clone();
            new_source_plan.partitions = Vec::from(chunks);

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
