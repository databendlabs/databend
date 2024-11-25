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

use std::path::Path;

use databend_common_base::runtime::ThreadTracker;
use log::Record;
use logforth::append::rolling_file::NonBlockingBuilder;
use logforth::append::rolling_file::RollingFileWriter;
use logforth::append::rolling_file::Rotation;
use logforth::append::RollingFile;
use logforth::layout::collect_kvs;
use logforth::layout::CustomLayout;
use logforth::layout::KvDisplay;
use logforth::Layout;
use serde_json::Map;

/// Create a `BufWriter<NonBlocking>` for a rolling file logger.
pub(crate) fn new_rolling_file_appender(
    dir: &str,
    name: impl ToString,
    max_files: usize,
) -> (RollingFile, Box<dyn Send + Sync + 'static>) {
    let rolling = RollingFileWriter::builder()
        .rotation(Rotation::Hourly)
        .filename_prefix(name.to_string())
        .max_log_files(max_files)
        .build(dir)
        .expect("failed to initialize rolling file appender");
    let (non_blocking, guard) = NonBlockingBuilder::default()
        .thread_name("log-file-appender")
        .finish(rolling);

    (RollingFile::new(non_blocking), Box::new(guard))
}

pub fn get_layout(format: &str) -> Layout {
    match format {
        "text" => text_layout(),
        "json" => json_layout(),
        "identical" => identical_layout(),
        _ => unimplemented!("file logging format {format} is not supported"),
    }
}

fn identical_layout() -> Layout {
    CustomLayout::new(|record: &Record| Ok(format!("{}", record.args()).into_bytes())).into()
}

fn text_layout() -> Layout {
    CustomLayout::new(|record: &Record| {
        let s = match ThreadTracker::query_id() {
            None => format!(
                "{} {:>5} {}: {}:{} {}{}",
                chrono::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
                record.level(),
                record.module_path().unwrap_or(""),
                Path::new(record.file().unwrap_or_default())
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or_default(),
                record.line().unwrap_or(0),
                record.args(),
                KvDisplay::new(record.key_values()),
            ),
            Some(query_id) => format!(
                "{} {} {:>5} {}: {}:{} {}{}",
                query_id,
                chrono::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
                record.level(),
                record.module_path().unwrap_or(""),
                Path::new(record.file().unwrap_or_default())
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or_default(),
                record.line().unwrap_or(0),
                record.args(),
                KvDisplay::new(record.key_values()),
            ),
        };

        Ok(s.into_bytes())
    })
    .into()
}

fn json_layout() -> Layout {
    CustomLayout::new(|record: &Record| {
        let mut fields = Map::new();
        fields.insert("message".to_string(), format!("{}", record.args()).into());
        for (k, v) in collect_kvs(record.key_values()) {
            fields.insert(k, v.into());
        }

        let s = match ThreadTracker::query_id() {
            None => {
                format!(
                    r#"{{"timestamp":"{}","level":"{}","fields":{}}}"#,
                    chrono::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
                    record.level(),
                    serde_json::to_string(&fields).unwrap_or_default(),
                )
            }
            Some(query_id) => {
                format!(
                    r#"{{"timestamp":"{}","level":"{}","query_id":"{}","fields":{}}}"#,
                    chrono::Local::now().to_rfc3339_opts(chrono::SecondsFormat::Micros, true),
                    record.level(),
                    query_id,
                    serde_json::to_string(&fields).unwrap_or_default(),
                )
            }
        };

        Ok(s.into_bytes())
    })
    .into()
}
