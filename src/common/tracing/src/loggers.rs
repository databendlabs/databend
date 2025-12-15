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
use jiff::Timestamp;
use jiff::Zoned;
use jiff::fmt::temporal::DateTimePrinter;
use jiff::tz::TimeZone;
use log::Record;
use log::kv::Key;
use log::kv::Source;
use log::kv::Value;
use log::kv::VisitSource;
use logforth::Append;
use logforth::Diagnostic;
use logforth::Layout;
use logforth::append::RollingFile;
use logforth::append::rolling_file::RollingFileWriter;
use logforth::append::rolling_file::Rotation;
use logforth::non_blocking::NonBlockingBuilder;
use serde_json::Map;
use serde_json::Value as JsonValue;

use crate::module_tag::label_for_module;

const PRINTER: DateTimePrinter = DateTimePrinter::new().precision(Some(6));

pub fn format_timestamp(zdt: &Zoned) -> String {
    let timestamp = zdt.timestamp();
    let tz = zdt.time_zone();
    let offset = tz.to_offset(timestamp);
    PRINTER.timestamp_with_offset_to_string(&timestamp, offset)
}

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

        let zdt = if FIXED_TIME {
            Timestamp::new(1431648000, 123456789)
                .unwrap()
                .to_zoned(TimeZone::system())
        } else {
            Zoned::now()
        };
        {
            let timestamp = zdt.timestamp();
            let tz = zdt.time_zone();
            let offset = tz.to_offset(timestamp);
            PRINTER.print_timestamp_with_offset(&timestamp, offset, &mut buf)?;
        }

        let level = record.level();
        let module = record.module_path().map(label_for_module).unwrap_or("");
        let log_file = Path::new(record.file().unwrap_or_default())
            .file_name()
            .and_then(|name| name.to_str())
            .unwrap_or_default();
        let line = record.line().unwrap_or(0);
        let msg = record.args();
        write!(buf, " {level:>5} {module}: {log_file}:{line} {msg}")?;
        record.key_values().visit(&mut KvWriter(&mut buf))?;

        Ok(buf)
    }
}

#[derive(Debug)]
pub struct JsonLayout<const FIXED_TIME: bool = false>;

impl<const FIXED_TIME: bool> Layout for JsonLayout<FIXED_TIME> {
    fn format(&self, record: &Record, _diagnostics: &[Diagnostic]) -> anyhow::Result<Vec<u8>> {
        let timestamp = if FIXED_TIME {
            Timestamp::new(1431648000, 123456789)
                .unwrap()
                .to_zoned(TimeZone::system())
        } else {
            Zoned::now()
        };

        let mut buf = Vec::new();
        write!(
            &mut buf,
            r#"{{"timestamp":"{}","level":"{}","#,
            format_timestamp(&timestamp),
            record.level(),
        )?;
        match ThreadTracker::query_id() {
            None => {
                write!(&mut buf, r#""fields":"#)?;
            }
            Some(query_id) => {
                write!(&mut buf, r#""query_id":"{query_id}","fields":"#)?;
            }
        };

        let mut fields = Map::new();
        fields.insert("message".to_string(), format!("{}", record.args()).into());
        fields.extend(collect_kvs(record.key_values()));

        serde_json::to_writer(&mut buf, &fields)?;
        buf.write_all(b"}")?;

        Ok(buf)
    }
}

pub struct KvWriter<'a>(pub &'a mut Vec<u8>);

impl<'kvs> VisitSource<'kvs> for KvWriter<'_> {
    fn visit_pair(&mut self, key: Key<'kvs>, value: Value<'kvs>) -> Result<(), log::kv::Error> {
        write!(self.0, " {key}={value}")?;
        Ok(())
    }
}

pub fn collect_kvs(kv: &dyn Source) -> Vec<(String, JsonValue)> {
    let mut collector = KvCollector { kv: Vec::new() };
    kv.visit(&mut collector).ok();
    collector.kv
}

struct KvCollector {
    kv: Vec<(String, JsonValue)>,
}

impl<'kvs> VisitSource<'kvs> for KvCollector {
    fn visit_pair(&mut self, key: Key<'kvs>, value: Value<'kvs>) -> Result<(), log::kv::Error> {
        self.kv.push((
            key.to_string(),
            serde_json::to_value(value).unwrap_or_else(|e| e.to_string().into()),
        ));
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
        expected_text_output: String,
        expected_json_output: String,
    }

    // Helper function to create test cases with records
    fn create_test_cases() -> Vec<TestCase> {
        let fixed_timestamp = Timestamp::new(1431648000, 123456789)
            .unwrap()
            .to_zoned(TimeZone::system());
        let timestamp = format_timestamp(&fixed_timestamp);

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
                expected_text_output: format!(
                    "{timestamp}  INFO test::module: test_file.rs:42 test message"
                ),
                expected_json_output: format!(
                    r#"{{"timestamp":"{timestamp}","level":"INFO","fields":{{"message":"test message"}}}}"#
                ),
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
                expected_text_output: format!("{timestamp}  INFO test::module: test_file.rs:42 "),
                expected_json_output: format!(
                    r#"{{"timestamp":"{timestamp}","level":"INFO","fields":{{"message":""}}}}"#
                ),
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
                expected_text_output: format!(
                    "{timestamp} ERROR test::module: test_file.rs:42 error occurred"
                ),
                expected_json_output: format!(
                    r#"{{"timestamp":"{timestamp}","level":"ERROR","fields":{{"message":"error occurred"}}}}"#
                ),
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
                expected_text_output: format!(
                    "{timestamp}  WARN test::module: test_file.rs:42 warning message"
                ),
                expected_json_output: format!(
                    r#"{{"timestamp":"{timestamp}","level":"WARN","fields":{{"message":"warning message"}}}}"#
                ),
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
                expected_text_output: format!(
                    "{timestamp} DEBUG test::module: test_file.rs:42 debug info"
                ),
                expected_json_output: format!(
                    r#"{{"timestamp":"{timestamp}","level":"DEBUG","fields":{{"message":"debug info"}}}}"#
                ),
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
                expected_text_output: format!(
                    "{timestamp} TRACE test::module: test_file.rs:42 trace data"
                ),
                expected_json_output: format!(
                    r#"{{"timestamp":"{timestamp}","level":"TRACE","fields":{{"message":"trace data"}}}}"#
                ),
            },
            TestCase {
                name: "message with key-values",
                message: "user action",
                record: Record::builder()
                    .args(format_args!("user action"))
                    .level(Level::Info)
                    .target("test_target")
                    .module_path(Some("test::module"))
                    .file(Some("test_file.rs"))
                    .line(Some(42))
                    .key_values(&[
                        ("user_id", &123u32 as &dyn log::kv::ToValue),
                        ("action", &"login" as _),
                    ])
                    .build(),
                expected_text_output: format!(
                    "{timestamp}  INFO test::module: test_file.rs:42 user action user_id=123 action=login"
                ),
                expected_json_output: format!(
                    r#"{{"timestamp":"{timestamp}","level":"INFO","fields":{{"message":"user action","user_id":123,"action":"login"}}}}"#
                ),
            },
            TestCase {
                name: "error with context",
                message: "database connection failed",
                record: Record::builder()
                    .args(format_args!("database connection failed"))
                    .level(Level::Error)
                    .target("test_target")
                    .module_path(Some("test::module"))
                    .file(Some("test_file.rs"))
                    .line(Some(42))
                    .key_values(&[
                        ("error_code", &500u32 as &dyn log::kv::ToValue),
                        ("retry_count", &3u32 as _),
                        ("host", &"localhost" as _),
                    ])
                    .build(),
                expected_text_output: format!(
                    "{timestamp} ERROR test::module: test_file.rs:42 database connection failed error_code=500 retry_count=3 host=localhost"
                ),
                expected_json_output: format!(
                    r#"{{"timestamp":"{timestamp}","level":"ERROR","fields":{{"message":"database connection failed","error_code":500,"retry_count":3,"host":"localhost"}}}}"#
                ),
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

            assert_eq!(output, test_case.expected_text_output,);
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

            assert_eq!(output, test_case.expected_json_output);
        }
    }
}
