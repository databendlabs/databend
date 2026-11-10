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

use super::ReclusterTaskKind;

/// Planning-time shares of the existing node resources, not additional allowances.
/// Executors must recheck their own live memory budget before running a batch.
#[derive(Clone, Copy, Debug)]
pub struct ReclusterTaskBudget {
    pub nodes: usize,
    pub threads_per_node: usize,
    pub bytes_per_node: usize,
}

impl ReclusterTaskBudget {
    /// Bound independent pipelines without adding a session setting.
    pub const MAX_TASKS_PER_NODE: usize = 2;

    pub fn max_tasks(&self) -> usize {
        self.nodes
            .saturating_mul(self.threads_per_node.min(Self::MAX_TASKS_PER_NODE))
    }

    /// Construct a deterministic, size-aware placement. A successful placement
    /// is a feasibility witness; the bounded heuristic need not find every fit.
    /// Unordered/full-sort tasks retain exclusive use of an executor.
    pub fn assign(
        &self,
        tasks: impl IntoIterator<Item = (usize, ReclusterTaskKind)>,
    ) -> Option<Vec<Vec<usize>>> {
        if self.nodes == 0 || self.threads_per_node == 0 {
            return None;
        }
        let tasks_per_node = self.threads_per_node.min(Self::MAX_TASKS_PER_NODE);
        let mut tasks = tasks.into_iter().enumerate().collect::<Vec<_>>();
        if tasks.len() > self.max_tasks() {
            return None;
        }
        tasks.sort_by(|left, right| {
            let exclusive = |kind| kind == ReclusterTaskKind::SortBlocks;
            exclusive(right.1.1)
                .cmp(&exclusive(left.1.1))
                .then_with(|| right.1.0.cmp(&left.1.0))
                .then_with(|| left.0.cmp(&right.0))
        });
        let mut batches = vec![Vec::new(); self.nodes];
        let mut bytes = vec![0usize; self.nodes];
        let mut exclusive = vec![false; self.nodes];
        for (task_idx, (task_bytes, kind)) in tasks {
            if task_bytes > self.bytes_per_node {
                return None;
            }
            let is_exclusive = kind == ReclusterTaskKind::SortBlocks;
            let node = (0..self.nodes)
                .filter(|&node| {
                    !exclusive[node]
                        && batches[node].len() < tasks_per_node
                        && (!is_exclusive || batches[node].is_empty())
                        && task_bytes <= self.bytes_per_node - bytes[node]
                })
                .min_by_key(|&node| (bytes[node], batches[node].len(), node))?;
            batches[node].push(task_idx);
            bytes[node] += task_bytes;
            exclusive[node] = is_exclusive;
        }
        Some(
            batches
                .into_iter()
                .filter(|batch| !batch.is_empty())
                .collect(),
        )
    }
}
