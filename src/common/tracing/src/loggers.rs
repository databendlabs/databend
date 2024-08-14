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

use logforth::append::rolling_file::NonBlockingBuilder;
use logforth::append::rolling_file::RollingFileWriter;
use logforth::append::rolling_file::Rotation;
use logforth::append::RollingFile;
use logforth::layout::JsonLayout;
use logforth::layout::TextLayout;
use logforth::Layout;

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
        "text" => TextLayout::default().into(),
        "json" => JsonLayout::default().into(),
        _ => unimplemented!("file logging format {format} is not supported"),
    }
}
