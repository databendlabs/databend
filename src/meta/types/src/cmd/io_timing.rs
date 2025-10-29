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

use std::fmt;
use std::time::Duration;
use std::time::Instant;

use crate::CmdContext;

/// Tracks I/O operation timing during log entry application.
#[derive(Debug, Clone, Default)]
pub struct IoTiming {
    /// Records of I/O operations: (operation_type, details, duration)
    operations: Vec<(String, String, Duration)>,
}

/// RAII-based timer for automatic I/O timing recording.
///
/// Records timing when dropped, using Rust's RAII pattern.
/// This ensures timing is recorded even if the function returns early.
pub struct IoTimer<'a> {
    cmd_ctx: &'a CmdContext,
    op_type: String,
    details: String,
    start: Instant,
}

impl<'a> IoTimer<'a> {
    pub fn new(
        cmd_ctx: &'a CmdContext,
        op_type: impl Into<String>,
        details: impl Into<String>,
    ) -> Self {
        Self {
            cmd_ctx,
            op_type: op_type.into(),
            details: details.into(),
            start: Instant::now(),
        }
    }
}

impl Drop for IoTimer<'_> {
    fn drop(&mut self) {
        self.cmd_ctx
            .record_io(&self.op_type, &self.details, self.start.elapsed());
    }
}

impl IoTiming {
    pub fn new() -> Self {
        Self {
            operations: Vec::new(),
        }
    }

    /// Record an I/O operation with its timing.
    pub fn record(
        &mut self,
        op_type: impl Into<String>,
        details: impl Into<String>,
        duration: Duration,
    ) {
        self.operations
            .push((op_type.into(), details.into(), duration));
    }

    /// Calculate total I/O time across all operations.
    pub fn total_duration(&self) -> Duration {
        self.operations.iter().map(|(_, _, d)| *d).sum()
    }
}

impl fmt::Display for IoTiming {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.operations.is_empty() {
            return write!(f, "no_io");
        }

        let mut last_op_type = "";
        let mut first = true;

        for (op_type, details, duration) in &self.operations {
            if op_type != last_op_type {
                if !first {
                    write!(f, "), ")?;
                }
                write!(f, "{}(", op_type)?;
                last_op_type = op_type;
                first = false;
            } else {
                write!(f, ", ")?;
            }

            write!(f, "{}:{}ms", details, duration.as_millis())?;
        }

        if !first {
            write!(f, ")")?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_io_timing_basic() {
        let mut timing = IoTiming::new();
        timing.record("get", "key1", Duration::from_millis(5));
        timing.record("get", "key2", Duration::from_millis(3));
        timing.record("list", "prefix:10", Duration::from_millis(20));

        assert_eq!(timing.total_duration(), Duration::from_millis(28));
        assert_eq!(
            timing.to_string(),
            "get(key1:5ms, key2:3ms), list(prefix:10:20ms)"
        );
    }

    #[test]
    fn test_io_timing_empty() {
        let timing = IoTiming::new();
        assert_eq!(timing.total_duration(), Duration::ZERO);
        assert_eq!(timing.to_string(), "no_io");
    }
}
