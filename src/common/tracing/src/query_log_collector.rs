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
use logforth::layout::KvDisplay;
use logforth::Append;

#[derive(Debug)]
pub struct QueryLogCollector {}

impl QueryLogCollector {
    pub fn new() -> QueryLogCollector {
        QueryLogCollector {}
    }
}

impl Append for QueryLogCollector {
    fn append(&self, record: &Record) -> anyhow::Result<()> {
        if let Some(settings) = ThreadTracker::capture_log_settings() {
            if let Some(queue) = settings.queue.as_ref() {
                // ignore push error
                let _ = queue.push(format!(
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
                ));
            }
        }

        Ok(())
    }

    fn flush(&self) {
        // do nothing
    }
}
