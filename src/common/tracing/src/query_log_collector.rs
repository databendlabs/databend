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

use databend_common_base::runtime::ThreadTracker;
use jiff::Zoned;
use log::Record;
use logforth::Append;
use logforth::Diagnostic;

use crate::loggers::KvWriter;
use crate::loggers::format_timestamp;

#[derive(Debug)]
pub struct QueryLogCollector {}

impl QueryLogCollector {
    pub fn new() -> QueryLogCollector {
        QueryLogCollector {}
    }
}

impl Append for QueryLogCollector {
    fn append(&self, record: &Record, _diagnostics: &[Diagnostic]) -> anyhow::Result<()> {
        let Some(settings) = ThreadTracker::capture_log_settings() else {
            return Ok(());
        };
        let Some(queue) = settings.queue.as_ref() else {
            return Ok(());
        };

        let mut buf = Vec::new();
        write!(
            buf,
            "{} {:>5} {}: {}:{} {}",
            format_timestamp(&Zoned::now()),
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
        // ignore push error
        let _ = queue.push(String::from_utf8(buf)?);
        Ok(())
    }

    fn flush(&self) {
        // do nothing
    }
}
