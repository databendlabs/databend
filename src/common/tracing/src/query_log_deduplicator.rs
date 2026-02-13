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

use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

#[derive(Default)]
pub struct QueryLogDeduplicator {
    // Number of in-flight executions for one query context.
    active_executions: AtomicUsize,
    // Guards "start" query log emission.
    start_logged: AtomicBool,
    // Guards terminal ("final") log emission.
    terminal_logged: AtomicBool,
}

impl QueryLogDeduplicator {
    /// Marks one execution enter.
    /// Returns true only for the first start log.
    pub fn on_execution_start(&self) -> bool {
        self.active_executions.fetch_add(1, Ordering::AcqRel);
        self.start_logged
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    /// Marks one execution leave.
    /// Returns true only when this leave reaches terminal state.
    pub fn on_execution_finish(&self) -> bool {
        loop {
            let current = self.active_executions.load(Ordering::Acquire);
            if current == 0 {
                return false;
            }

            let next = current - 1;
            if self
                .active_executions
                .compare_exchange(current, next, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                if next != 0 {
                    return false;
                }

                return self
                    .terminal_logged
                    .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::QueryLogDeduplicator;

    #[test]
    fn test_start_is_emitted_once() {
        let deduplicator = QueryLogDeduplicator::default();
        assert!(deduplicator.on_execution_start());
        assert!(!deduplicator.on_execution_start());
    }

    #[test]
    fn test_finish_is_emitted_only_on_terminal_execution() {
        let deduplicator = QueryLogDeduplicator::default();
        assert!(deduplicator.on_execution_start());
        assert!(!deduplicator.on_execution_start());

        assert!(!deduplicator.on_execution_finish());
        assert!(deduplicator.on_execution_finish());
        assert!(!deduplicator.on_execution_finish());
    }
}
