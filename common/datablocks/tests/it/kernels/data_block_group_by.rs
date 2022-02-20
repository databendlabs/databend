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

use common_datablocks::*;
use common_datavalues::prelude::*;
use common_exception::Result;

#[test]
fn test_data_block_group_by() -> Result<()> {
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", i8::to_data_type()),
        DataField::new("b", Vu8::to_data_type()),
    ]);

    let block = DataBlock::create(schema, vec![
        Series::from_data(vec![1i8, 1, 2, 1, 2, 3]),
        Series::from_data(vec!["x1", "x1", "x2", "x1", "x2", "x3"]),
    ]);

    let columns = &["a".to_string(), "b".to_string()];
    let table = DataBlock::group_by_blocks(&block, columns)?;
    for block in table {
        match block.num_rows() {
            1 => {
                let expected = vec![
                    "+---+----+",
                    "| a | b  |",
                    "+---+----+",
                    "| 3 | x3 |",
                    "+---+----+",
                ];
                common_datablocks::assert_blocks_sorted_eq(expected, &[block]);
            }
            2 => {
                let expected = vec![
                    "+---+----+",
                    "| a | b  |",
                    "+---+----+",
                    "| 2 | x2 |",
                    "| 2 | x2 |",
                    "+---+----+",
                ];
                common_datablocks::assert_blocks_sorted_eq(expected, &[block]);
            }
            3 => {
                let expected = vec![
                    "+---+----+",
                    "| a | b  |",
                    "+---+----+",
                    "| 1 | x1 |",
                    "| 1 | x1 |",
                    "| 1 | x1 |",
                    "+---+----+",
                ];
                common_datablocks::assert_blocks_sorted_eq(expected, &[block]);
            }
            _ => unreachable!(),
        }
    }
    Ok(())
}
