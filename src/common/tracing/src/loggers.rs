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

use std::io::Write;
use std::path::Path;

use databend_common_base::runtime::LimitMemGuard;
use databend_common_base::runtime::ThreadTracker;
use log::Record;
use logforth::append::rolling_file::RollingFileWriter;
use logforth::append::rolling_file::Rotation;
use logforth::append::RollingFile;
use logforth::non_blocking::NonBlockingBuilder;
use logforth::Append;
use logforth::Diagnostic;
use logforth::Layout;
use serde_json::Map;

/// Create a `BufWriter<NonBlocking>` for a rolling file logger.
pub(crate) fn new_rolling_file_appender(
    dir: &str,
    name: impl ToString,
    max_files: usize,
    max_file_size: usize,
) -> (RollingFileWarp, Box<dyn Send + Sync + 'static>) {
    let rolling = RollingFileWriter::builder()
        .rotation(Rotation::Hourly)
        .filename_prefix(name.to_string())
        .max_log_files(max_files)
        .max_file_size(max_file_size)
        .build(dir)
        .expect("failed to initialize rolling file appender");
    let (non_blocking, guard) = NonBlockingBuilder::new("log-file-appender", rolling).finish();

    (
        RollingFileWarp::new(RollingFile::new(non_blocking)),
        Box::new(guard),
    )
}

#[derive(Debug)]
pub struct RollingFileWarp {
    inner: RollingFile,
}

impl RollingFileWarp {
    pub fn new(inner: RollingFile) -> Self {
        Self { inner }
    }

    /// Sets the layout used to format log records as bytes.
    pub fn with_layout(self, layout: impl Layout) -> Self {
        Self::new(self.inner.with_layout(layout))
    }
}

impl Append for RollingFileWarp {
    fn append(&self, record: &Record, diagnostics: &[Diagnostic]) -> anyhow::Result<()> {
        let _guard = LimitMemGuard::enter_unlimited();
        self.inner.append(record, diagnostics)
    }
}

#[derive(Debug)]
pub struct IdenticalLayout;

impl Layout for IdenticalLayout {
    fn format(&self, record: &Record, _diagnostics: &[Diagnostic]) -> anyhow::Result<Vec<u8>> {
        Ok(format!("{}", record.args()).into_bytes())
    }
}

#[derive(Debug)]
pub struct TextLayout<const FIXED_TIME: bool = false>;

impl<const FIXED_TIME: bool> Layout for TextLayout<FIXED_TIME> {
    fn format(&self, record: &Record, _diagnostics: &[Diagnostic]) -> anyhow::Result<Vec<u8>> {
        let mut buf = Vec::new();
        if let Some(query_id) = ThreadTracker::query_id() {
            write!(buf, "{query_id} ")?;
        }

        let timestamp = if FIXED_TIME {
            chrono::DateTime::from_timestamp(1431648000, 123456789)
                .unwrap()
                .with_timezone(&chrono::Local)
        } else {
            chrono::Local::now()
        };

        write!(
            buf,
            "{} {:>5} {}: {}:{} {}",
            timestamp.to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
            record.level(),
            record.module_path().unwrap_or(""),
            Path::new(record.file().unwrap_or_default())
                .file_name()
                .and_then(|name| name.to_str())
                .unwrap_or_default(),
            record.line().unwrap_or(0),
            record.args(),
        )?;
        record.key_values().visit(&mut KvWriter(&mut buf))?;

        Ok(buf)
    }
}

#[derive(Debug)]
pub struct JsonLayout<const FIXED_TIME: bool = false>;

impl<const FIXED_TIME: bool> Layout for JsonLayout<FIXED_TIME> {
    fn format(&self, record: &Record, _diagnostics: &[Diagnostic]) -> anyhow::Result<Vec<u8>> {
        let mut fields = Map::new();
        fields.insert("message".to_string(), format!("{}", record.args()).into());
        for (k, v) in collect_kvs(record.key_values()) {
            fields.insert(k, v.into());
        }

        let timestamp = if FIXED_TIME {
            chrono::DateTime::from_timestamp(1431648000, 123456789)
                .unwrap()
                .with_timezone(&chrono::Local)
        } else {
            chrono::Local::now()
        };

        let s = match ThreadTracker::query_id() {
            None => {
                format!(
                    r#"{{"timestamp":"{}","level":"{}","fields":{}}}"#,
                    timestamp.to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
                    record.level(),
                    serde_json::to_string(&fields).unwrap_or_default(),
                )
            }
            Some(query_id) => {
                format!(
                    r#"{{"timestamp":"{}","level":"{}","query_id":"{}","fields":{}}}"#,
                    timestamp.to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
                    record.level(),
                    query_id,
                    serde_json::to_string(&fields).unwrap_or_default(),
                )
            }
        };

        Ok(s.into_bytes())
    }
}

pub struct KvWriter<'a>(pub &'a mut Vec<u8>);

impl<'kvs> log::kv::VisitSource<'kvs> for KvWriter<'_> {
    fn visit_pair(
        &mut self,
        key: log::kv::Key<'kvs>,
        value: log::kv::Value<'kvs>,
    ) -> Result<(), log::kv::Error> {
        write!(self.0, " {key}={value}")?;
        Ok(())
    }
}

pub fn collect_kvs(kv: &dyn log::kv::Source) -> Vec<(String, String)> {
    let mut collector = KvCollector { kv: Vec::new() };
    kv.visit(&mut collector).ok();
    collector.kv
}

struct KvCollector {
    kv: Vec<(String, String)>,
}

impl<'kvs> log::kv::VisitSource<'kvs> for KvCollector {
    fn visit_pair(
        &mut self,
        key: log::kv::Key<'kvs>,
        value: log::kv::Value<'kvs>,
    ) -> Result<(), log::kv::Error> {
        self.kv.push((key.to_string(), value.to_string()));
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use log::Level;
    use log::Record;

    use super::*;

    // Test case structure for shared test data
    struct TestCase {
        name: &'static str,
        message: &'static str,
        record: Record<'static>,
        expected_text_output: &'static str, /* Expected complete output for TextLayout with fixed time */
        expected_json_output: &'static str, /* Expected complete output for JsonLayout with fixed time */
    }

    // Helper function to create test cases with records
    fn create_test_cases() -> Vec<TestCase> {
        vec![
            TestCase {
                name: "basic message",
                message: "test message",
                record: Record::builder()
                    .args(format_args!("test message"))
                    .level(Level::Info)
                    .target("test_target")
                    .module_path(Some("test::module"))
                    .file(Some("test_file.rs"))
                    .line(Some(42))
                    .build(),
                expected_text_output: "2015-05-15T08:00:00.123456+08:00  INFO test::module: test_file.rs:42 test message",
                expected_json_output: r#"{"timestamp":"2015-05-15T08:00:00.123456+08:00","level":"INFO","fields":{"message":"test message"}}"#,
            },
            TestCase {
                name: "empty message",
                message: "",
                record: Record::builder()
                    .args(format_args!(""))
                    .level(Level::Info)
                    .target("test_target")
                    .module_path(Some("test::module"))
                    .file(Some("test_file.rs"))
                    .line(Some(42))
                    .build(),
                expected_text_output: "2015-05-15T08:00:00.123456+08:00  INFO test::module: test_file.rs:42 ",
                expected_json_output: r#"{"timestamp":"2015-05-15T08:00:00.123456+08:00","level":"INFO","fields":{"message":""}}"#,
            },
            TestCase {
                name: "error level",
                message: "error occurred",
                record: Record::builder()
                    .args(format_args!("error occurred"))
                    .level(Level::Error)
                    .target("test_target")
                    .module_path(Some("test::module"))
                    .file(Some("test_file.rs"))
                    .line(Some(42))
                    .build(),
                expected_text_output: "2015-05-15T08:00:00.123456+08:00 ERROR test::module: test_file.rs:42 error occurred",
                expected_json_output: r#"{"timestamp":"2015-05-15T08:00:00.123456+08:00","level":"ERROR","fields":{"message":"error occurred"}}"#,
            },
            TestCase {
                name: "warn level",
                message: "warning message",
                record: Record::builder()
                    .args(format_args!("warning message"))
                    .level(Level::Warn)
                    .target("test_target")
                    .module_path(Some("test::module"))
                    .file(Some("test_file.rs"))
                    .line(Some(42))
                    .build(),
                expected_text_output: "2015-05-15T08:00:00.123456+08:00  WARN test::module: test_file.rs:42 warning message",
                expected_json_output: r#"{"timestamp":"2015-05-15T08:00:00.123456+08:00","level":"WARN","fields":{"message":"warning message"}}"#,
            },
            TestCase {
                name: "debug level",
                message: "debug info",
                record: Record::builder()
                    .args(format_args!("debug info"))
                    .level(Level::Debug)
                    .target("test_target")
                    .module_path(Some("test::module"))
                    .file(Some("test_file.rs"))
                    .line(Some(42))
                    .build(),
                expected_text_output: "2015-05-15T08:00:00.123456+08:00 DEBUG test::module: test_file.rs:42 debug info",
                expected_json_output: r#"{"timestamp":"2015-05-15T08:00:00.123456+08:00","level":"DEBUG","fields":{"message":"debug info"}}"#,
            },
            TestCase {
                name: "trace level",
                message: "trace data",
                record: Record::builder()
                    .args(format_args!("trace data"))
                    .level(Level::Trace)
                    .target("test_target")
                    .module_path(Some("test::module"))
                    .file(Some("test_file.rs"))
                    .line(Some(42))
                    .build(),
                expected_text_output: "2015-05-15T08:00:00.123456+08:00 TRACE test::module: test_file.rs:42 trace data",
                expected_json_output: r#"{"timestamp":"2015-05-15T08:00:00.123456+08:00","level":"TRACE","fields":{"message":"trace data"}}"#,
            },
        ]
    }

    #[test]
    fn test_identical_layout() {
        let layout = IdenticalLayout;
        let diagnostics = [];
        let test_cases = create_test_cases();

        for test_case in &test_cases {
            let result = layout.format(&test_case.record, &diagnostics).unwrap();
            let output = String::from_utf8(result).unwrap();

            // IdenticalLayout should return the message as-is
            assert_eq!(
                output, test_case.message,
                "Failed test case '{}': expected '{}', got '{}'",
                test_case.name, test_case.message, output
            );
        }
    }

    #[test]
    fn test_text_layout() {
        let layout = TextLayout::<true>; // Use fixed time
        let diagnostics = [];
        let test_cases = create_test_cases();

        for test_case in &test_cases {
            let result = layout.format(&test_case.record, &diagnostics).unwrap();
            let output = String::from_utf8(result).unwrap();

            assert_eq!(
                output, test_case.expected_text_output,
                "Failed test case '{}': expected '{}', got '{}'",
                test_case.name, test_case.expected_text_output, output
            );
        }
    }

    #[test]
    fn test_json_layout() {
        let layout = JsonLayout::<true>; // Use fixed time
        let diagnostics = [];
        let test_cases = create_test_cases();

        for test_case in &test_cases {
            let result = layout.format(&test_case.record, &diagnostics).unwrap();
            let output = String::from_utf8(result).unwrap();

            assert_eq!(
                output, test_case.expected_json_output,
                "Failed test case '{}': expected '{}', got '{}'",
                test_case.name, test_case.expected_json_output, output
            );
        }
    }
}
