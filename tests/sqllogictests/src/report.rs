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
use std::fmt::Write;
use std::time::Duration;

use sqllogictest::TestError;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct NonDefaultSetting {
    name: String,
    value: String,
    default_value: String,
    level: String,
}

impl NonDefaultSetting {
    pub(crate) fn new(
        name: impl Into<String>,
        value: impl Into<String>,
        default_value: impl Into<String>,
        level: impl Into<String>,
    ) -> Self {
        Self {
            name: name.into(),
            value: value.into(),
            default_value: default_value.into(),
            level: level.into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct ErrorRecord {
    filename: String,
    query_id: Option<String>,
    non_default_settings: Vec<NonDefaultSetting>,
    detail: String,
}

impl ErrorRecord {
    pub(crate) fn new(
        filename: impl Into<String>,
        error: TestError,
        query_id: Option<String>,
        non_default_settings: Vec<NonDefaultSetting>,
    ) -> Self {
        Self {
            filename: filename.into(),
            query_id,
            non_default_settings,
            detail: error.display(true).to_string(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RunStatus {
    NoTestsRun,
    Passed,
    Failed,
}

pub(crate) struct RunReport {
    selected_files: usize,
    discovered_tests: usize,
    tests_were_run: bool,
    failed_files: usize,
    no_fail_fast: bool,
    duration: Duration,
    error_records: Vec<ErrorRecord>,
}

impl RunReport {
    pub(crate) fn new(
        selected_files: usize,
        discovered_tests: usize,
        tests_were_run: bool,
        no_fail_fast: bool,
        duration: Duration,
        mut error_records: Vec<ErrorRecord>,
    ) -> Self {
        error_records.sort();
        let failed_files = error_records
            .iter()
            .map(|record| record.filename.as_str())
            .collect::<HashSet<_>>()
            .len();

        Self {
            selected_files,
            discovered_tests,
            tests_were_run,
            failed_files,
            no_fail_fast,
            duration,
            error_records,
        }
    }

    pub(crate) fn render(&self) -> String {
        let mut output = String::new();
        writeln!(&mut output, "Summary: {}", self.summary_line()).unwrap();

        if self.status() == RunStatus::Failed {
            writeln!(&mut output).unwrap();
            writeln!(&mut output, "Failures:").unwrap();
            self.render_failures(&mut output);
        }

        output
    }

    pub(crate) fn has_failures(&self) -> bool {
        !self.error_records.is_empty()
    }

    fn status(&self) -> RunStatus {
        if !self.tests_were_run {
            RunStatus::NoTestsRun
        } else if self.error_records.is_empty() {
            RunStatus::Passed
        } else {
            RunStatus::Failed
        }
    }

    fn summary_line(&self) -> String {
        match self.status() {
            RunStatus::NoTestsRun => "⚠️  No tests were run.".to_string(),
            RunStatus::Passed => format!(
                "✅ Passed, {} test(s) across {} file(s) completed in {} ms.",
                self.discovered_tests,
                self.selected_files,
                self.duration.as_millis()
            ),
            RunStatus::Failed => format!(
                "❌ Failed, {} record(s) across {} file(s); {} discovered test(s); fail fast {}; {} ms.",
                self.error_records.len(),
                self.failed_files,
                self.discovered_tests,
                if self.no_fail_fast {
                    "disabled"
                } else {
                    "enabled"
                },
                self.duration.as_millis()
            ),
        }
    }

    fn render_failures(&self, output: &mut String) {
        let mut current_file: Option<&str> = None;
        let mut index_in_file = 0;

        for record in &self.error_records {
            if current_file != Some(record.filename.as_str()) {
                if current_file.is_some() {
                    writeln!(output).unwrap();
                }
                current_file = Some(record.filename.as_str());
                index_in_file = 0;
                writeln!(output, "[{}]", record.filename).unwrap();
            }

            index_in_file += 1;
            writeln!(
                output,
                "  {}. query_id: {}",
                index_in_file,
                record.query_id.as_deref().unwrap_or("unknown")
            )
            .unwrap();

            if !record.non_default_settings.is_empty() {
                writeln!(output, "     non-default settings:").unwrap();
                for setting in &record.non_default_settings {
                    writeln!(
                        output,
                        "       {} = {} (default: {}, level: {})",
                        setting.name, setting.value, setting.default_value, setting.level
                    )
                    .unwrap();
                }
            }

            for line in record.detail.lines() {
                writeln!(output, "     {line}").unwrap();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn render_report_for_no_tests_run() {
        let report = RunReport::new(0, 0, false, true, Duration::from_millis(7), vec![]);

        let rendered = report.render();

        assert_eq!(rendered, "Summary: ⚠️  No tests were run.\n");
    }

    #[test]
    fn render_report_for_success() {
        let report = RunReport::new(2, 5, true, true, Duration::from_millis(11), vec![]);

        let rendered = report.render();

        assert_eq!(
            rendered,
            "Summary: ✅ Passed, 5 test(s) across 2 file(s) completed in 11 ms.\n"
        );
        assert!(!rendered.contains("Failures:"));
    }

    #[test]
    fn render_report_for_success_with_seconds() {
        let report = RunReport::new(1, 3, true, true, Duration::from_millis(1_500), vec![]);

        let rendered = report.render();

        assert_eq!(
            rendered,
            "Summary: ✅ Passed, 3 test(s) across 1 file(s) completed in 1500 ms.\n"
        );
    }

    #[test]
    fn render_report_groups_failures_by_file() {
        let report = RunReport::new(
            3,
            8,
            true,
            false,
            Duration::from_millis(19),
            vec![
                ErrorRecord {
                    filename: "b.test".to_string(),
                    query_id: None,
                    non_default_settings: vec![],
                    detail: "second error".to_string(),
                },
                ErrorRecord {
                    filename: "a.test".to_string(),
                    query_id: Some("query-1".to_string()),
                    non_default_settings: vec![
                        NonDefaultSetting::new("max_threads", "8", "16", "SESSION"),
                        NonDefaultSetting::new("timezone", "UTC", "SYSTEM", "SESSION"),
                    ],
                    detail: "first error\nwith more detail".to_string(),
                },
            ],
        );

        let rendered = report.render();

        assert!(
            rendered.contains(
                "[a.test]\n  1. query_id: query-1\n     non-default settings:\n       max_threads = 8 (default: 16, level: SESSION)\n       timezone = UTC (default: SYSTEM, level: SESSION)\n     first error\n     with more detail"
            )
        );
        assert!(rendered.contains("[b.test]\n  1. query_id: unknown\n     second error"));
        assert!(rendered.contains(
            "❌ Failed, 2 record(s) across 2 file(s); 8 discovered test(s); fail fast enabled; 19 ms."
        ));
        assert!(rendered.contains("\nFailures:\n"));
    }
}
