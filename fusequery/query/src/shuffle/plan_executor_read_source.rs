// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use std::collections::hash_map::Entry::Occupied;
use std::collections::hash_map::Entry::Vacant;
use std::collections::HashMap;
use std::sync::Arc;

use common_exception::Result;
use common_management::cluster::ClusterExecutor;
use common_planners::PlanNode;
use common_planners::ReadDataSourcePlan;

use crate::sessions::FuseQueryContextRef;
use crate::shuffle::ExecutorPlan;
use crate::shuffle::LocalReadSourceExecutorPlan;
use crate::shuffle::RemoteReadSourceExecutorPlan;

pub struct ReadSourceExecutorPlan(pub Arc<Box<dyn ExecutorPlan>>);

impl ReadSourceExecutorPlan {
    pub fn create(
        ctx: &FuseQueryContextRef,
        plan: &ReadDataSourcePlan,
        nest_getter: &Arc<Box<dyn ExecutorPlan>>,
        executors: &[Arc<ClusterExecutor>],
    ) -> Result<Arc<Box<dyn ExecutorPlan>>> {
        let table = ctx.get_table(&plan.db, &plan.table)?;

        if !table.is_local() {
            let new_partitions_size = ctx.get_max_threads()? as usize * executors.len();
            let new_source_plan =
                table.read_plan(ctx.clone(), &*plan.scan_plan, new_partitions_size)?;

            // We always put adjacent partitions in the same node
            let new_partitions = &new_source_plan.parts;
            let mut executors_partitions = HashMap::new();
            let partitions_pre_node = new_partitions.len() / executors.len();

            for (executor_index, executor) in executors.iter().enumerate() {
                let mut partitions = vec![];
                let partitions_offset = partitions_pre_node * executor_index;

                for partition_index in 0..partitions_pre_node {
                    partitions.push((new_partitions[partitions_offset + partition_index]).clone());
                }

                if !partitions.is_empty() {
                    executors_partitions.insert(executor.name.clone(), partitions);
                }
            }

            // For some irregular partitions, we assign them to the head nodes
            let offset = partitions_pre_node * executors.len();
            for index in 0..(new_partitions.len() % executors.len()) {
                let executor_name = &executors[index].name;
                match executors_partitions.entry(executor_name.clone()) {
                    Vacant(entry) => {
                        let partitions = vec![new_partitions[offset + index].clone()];
                        entry.insert(partitions);
                    }
                    Occupied(mut entry) => {
                        entry.get_mut().push(new_partitions[offset + index].clone());
                    }
                }
            }

            let nested_getter = RemoteReadSourceExecutorPlan(
                new_source_plan,
                Arc::new(executors_partitions),
                nest_getter.clone(),
            );
            return Ok(Arc::new(Box::new(ReadSourceExecutorPlan(Arc::new(
                Box::new(nested_getter),
            )))));
        }

        let nested_getter = LocalReadSourceExecutorPlan(plan.clone(), nest_getter.clone());
        Ok(Arc::new(Box::new(ReadSourceExecutorPlan(Arc::new(
            Box::new(nested_getter),
        )))))
    }
}

impl ExecutorPlan for ReadSourceExecutorPlan {
    fn get_plan(
        &self,
        executor_name: &str,
        executors: &[Arc<ClusterExecutor>],
    ) -> Result<PlanNode> {
        self.0.get_plan(executor_name, executors)
    }
}
