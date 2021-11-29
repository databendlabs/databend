// Copyright 2021 Datafuse Labs.
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

use std::fs::File;
use std::io::Write;

use common_base::tokio;
use common_dal::DataAccessor;
use common_dal::Local;
use common_datablocks::assert_blocks_eq;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataType;
use common_streams::CsvSource;
use common_streams::Source;
use common_streams::ValueSource;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_parse_values() {
    let buffer =
        "(1,  'str',   1) , (-1, ' str ' ,  1.1) , ( 2,  'aa aa', 2.2),  (3, \"33'33\", 3.3)   ";

    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", DataType::Int8, false),
        DataField::new("b", DataType::String, false),
        DataField::new("c", DataType::Float64, false),
    ]);
    let mut values_source = ValueSource::new(buffer.as_bytes(), schema, 10);
    let block = values_source.read().await.unwrap().unwrap();
    assert_blocks_eq(
        vec![
            "+----+-------+-----+",
            "| a  | b     | c   |",
            "+----+-------+-----+",
            "| 1  | str   | 1   |",
            "| -1 |  str  | 1.1 |",
            "| 2  | aa aa | 2.2 |",
            "| 3  | 33'33 | 3.3 |",
            "+----+-------+-----+",
        ],
        &[block],
    );

    let block = values_source.read().await.unwrap();
    assert!(block.is_none());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_parse_csvs() {
    let dir = tempfile::tempdir().unwrap();
    let name = "my-temporary-note.txt";
    let file_path = dir.path().join(name);
    let mut file = File::create(file_path).unwrap();
    writeln!(file, "1,\"1\",1.11\n2,\"2\",2\n3,\"3-'3'-3\",3\"").unwrap();

    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", DataType::Int8, false),
        DataField::new("b", DataType::String, false),
        DataField::new("c", DataType::Float64, false),
    ]);

    let local = Local::with_path(dir.path().to_path_buf());
    let stream = local.get_input_stream(name, None).unwrap();
    let mut csv_source = CsvSource::try_create(stream, schema, false, 10).unwrap();
    let block = csv_source.read().await.unwrap().unwrap();
    assert_blocks_eq(
        vec![
            "+---+---------+------+",
            "| a | b       | c    |",
            "+---+---------+------+",
            "| 1 | 1       | 1.11 |",
            "| 2 | 2       | 2    |",
            "| 3 | 3-'3'-3 | 3    |",
            "+---+---------+------+",
        ],
        &[block],
    );

    let block = csv_source.read().await.unwrap();
    assert!(block.is_none());

    drop(file);
    dir.close().unwrap();
}
