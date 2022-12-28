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

use std::io::Write;
use std::sync::Arc;

use comfy_table::Table;
use common_base::base::tokio;
use common_base::base::*;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::BlockEntry;
use common_expression::Column;
use common_expression::ColumnFrom;
use common_expression::DataBlock;
use common_expression::Value;
use databend_query::stream::DataBlockStream;
use databend_query::stream::ProgressStream;
use futures::TryStreamExt;
use goldenfile::Mint;

#[tokio::test(flavor = "multi_thread")]
async fn test_progress_stream() -> Result<()> {
    let mut mint = Mint::new("tests/it/testdata");
    let file = &mut mint.new_goldenfile("stream_datablock.txt").unwrap();

    let block = DataBlock::new(
        vec![BlockEntry {
            data_type: DataType::Number(NumberDataType::Int64),
            value: Value::Column(Column::from_data(vec![1i64, 2, 3])),
        }],
        3,
    );

    let blocks = vec![block.clone(), block.clone(), block];
    let input = DataBlockStream::create(None, blocks);

    let progress = Arc::new(Progress::create());
    let stream = ProgressStream::try_create(Box::pin(input), progress)?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 1);

    let mut table = Table::new();
    table.load_preset("||--+-++|    ++++++");
    table.set_header(["a"]);
    for _ in 0..block.num_rows() {
        let mut row = Vec::with_capacity(block.num_columns());
        for i in 0..block.num_columns() {
            let col = block.get_by_offset(i);
            row.push(format!("{}", col.value));
        }
        table.add_row(row);
    }
    writeln!(file, "{table}\n\n").unwrap();

    Ok(())
}
