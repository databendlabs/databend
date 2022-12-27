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

use comfy_table::Table;
use common_base::base::tokio;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::BlockEntry;
use common_expression::Column;
use common_expression::ColumnFrom;
use common_expression::DataBlock;
use common_expression::Value;
use common_streams::*;
use futures::stream::StreamExt;
use goldenfile::Mint;

#[tokio::test(flavor = "multi_thread")]
async fn test_datablock_stream() {
    let mut mint = Mint::new("tests/it/testdata");
    let file = &mut mint.new_goldenfile("stream_datablock.txt").unwrap();

    let data_blocks = vec![
        DataBlock::new(
            Vec![
                BlockEntry {
                    data_type: DataType::String,
                    value: Value::Column(Column::from_data(vec!["a1", "a2", "a3"])),
                },
                BlockEntry {
                    data_type: DataType::Number(NumberDataType::Int32),
                    value: Value::Column(Column::from_data(vec![1i32, 1, 1])),
                },
            ],
            3,
        ),
        DataBlock::new(
            Vec![
                BlockEntry {
                    data_type: DataType::String,
                    value: Value::Column(Column::from_data(vec!["b1", "b2", "b3"])),
                },
                BlockEntry {
                    data_type: DataType::Number(NumberDataType::Int32),
                    value: Value::Column(Column::from_data(vec![2i32, 2, 2])),
                },
            ],
            3,
        ),
        DataBlock::new(
            Vec![
                BlockEntry {
                    data_type: DataType::String,
                    value: Value::Column(Column::from_data(vec!["c1", "c2", "c3"])),
                },
                BlockEntry {
                    data_type: DataType::Number(NumberDataType::Int32),
                    value: Value::Column(Column::from_data(vec![3i32, 3, 3])),
                },
            ],
            3,
        ),
    ];

    let mut stream = DataBlockStream::create(None, chunks);

    while let Some(res) = stream.next().await {
        assert!(res.is_ok());
        let chunk = res.unwrap();

        let mut table = Table::new();
        table.load_preset("||--+-++|    ++++++");
        table.set_header(["name", "age"]);

        for _ in 0..chunk.num_rows() {
            let mut row = Vec::with_capacity(chunk.num_columns());
            for i in 0..chunk.num_columns() {
                let (value, _) = chunk.column(i);
                row.push(format!("{}", value));
            }
            table.add_row(row);
        }
        writeln!(file, "{table}\n\n").unwrap();
    }
}
