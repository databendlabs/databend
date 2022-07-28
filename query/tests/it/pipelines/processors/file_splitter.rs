// Copyright 2022 Datafuse Labs.
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

use std::collections::VecDeque;
use std::sync::Arc;

use common_base::base::tokio;
use common_base::base::ProgressValues;
use common_datavalues::DataField;
use common_datavalues::DataSchema;
use common_datavalues::StringType;
use common_exception::Result;
use common_formats::format_csv::CsvInputFormat;
use common_io::prelude::FormatSettings;
use databend_query::pipelines::processors::FileSplitter;
use databend_query::pipelines::processors::FileSplitterState;
use futures_util::io::Cursor;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_split_csv_with_header() -> Result<()> {
    // data
    let contents = r#""a","b"
v1,v2
v3,v4
"#
    .as_bytes();
    let reader = Cursor::new(contents);
    let fields = vec![
        DataField::new("c1", StringType::new_impl()),
        DataField::new("c2", StringType::new_impl()),
    ];
    let schema = Arc::new(DataSchema::new(fields));

    // set up
    let format_settings = FormatSettings {
        skip_header: 1,
        input_buffer_size: 1,
        ..Default::default()
    };
    let file_format =
        CsvInputFormat::try_create("", schema.clone(), Default::default(), 0, 1, 1024)?;
    let mut splitter = FileSplitter::create(reader, file_format, format_settings, None);

    // run
    let mut output_splits: VecDeque<Vec<u8>> = VecDeque::new();
    let mut progress = ProgressValues::default();
    while !matches!(splitter.state(), FileSplitterState::Finished) {
        splitter.async_process().await?;
        splitter.process(&mut output_splits, &mut progress)?;
    }
    let exp = VecDeque::from(vec![b"v1,v2\nv3,v4\n".to_vec()]);
    assert_eq!(output_splits, exp);
    Ok(())
}
