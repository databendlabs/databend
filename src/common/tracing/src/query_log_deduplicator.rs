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

use parking_lot::Mutex;

#[derive(Default)]
pub struct QueryLogDeduplicator {
    // Serialized query log emission state for one shared query context.
    inner: Mutex<QueryLogDeduplicatorInner>,
}

#[derive(Default)]
struct QueryLogDeduplicatorInner {
    active_executions: usize,
    start_logged: bool,
    terminal_logged: bool,
}

impl QueryLogDeduplicator {
    /// Marks one execution enter.
    /// Returns true only for the first start log.
    pub fn on_execution_start(&self) -> bool {
        let mut inner = self.inner.lock();

        inner.active_executions += 1;
        if inner.start_logged {
            false
        } else {
            inner.start_logged = true;
            true
        }
    }

    /// Marks one execution leave.
    /// Returns true only when logs should be emitted.
    /// Error leave always enters terminal state immediately.
    pub fn on_execution_finish(&self, has_error: bool) -> bool {
        let mut inner = self.inner.lock();

        if inner.terminal_logged {
            return false;
        }

        if has_error {
            inner.active_executions = 0;
            inner.terminal_logged = true;
            return true;
        }

        if inner.active_executions == 0 {
            return false;
        }

        inner.active_executions -= 1;
        if inner.active_executions != 0 {
            return false;
        }

        inner.terminal_logged = true;
        true
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

        assert!(!deduplicator.on_execution_finish(false));
        assert!(deduplicator.on_execution_finish(false));
        assert!(!deduplicator.on_execution_finish(false));
    }

    #[test]
    fn test_error_finish_is_emitted_immediately() {
        let deduplicator = QueryLogDeduplicator::default();
        assert!(deduplicator.on_execution_start());
        assert!(!deduplicator.on_execution_start());

        assert!(deduplicator.on_execution_finish(true));
        assert!(!deduplicator.on_execution_finish(false));
        assert!(!deduplicator.on_execution_finish(true));
    }
}
