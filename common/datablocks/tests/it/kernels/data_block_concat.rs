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
fn test_data_block_concat() -> Result<()> {
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", i64::to_data_type()),
        DataField::new("b", Vu8::to_data_type()),
    ]);

    let blocks = vec![
        DataBlock::create(schema.clone(), vec![
            Series::from_data(vec![1i64, 2, 3]),
            Series::from_data(vec!["b1", "b2", "b3"]),
        ]),
        DataBlock::create(schema.clone(), vec![
            Series::from_data(vec![4i64, 5, 6]),
            Series::from_data(vec!["b1", "b2", "b3"]),
        ]),
        DataBlock::create(schema, vec![
            Series::from_data(vec![7i64, 8, 9]),
            Series::from_data(vec!["b1", "b2", "b3"]),
        ]),
    ];

    let results = DataBlock::concat_blocks(&blocks)?;
    assert_eq!(blocks[0].schema(), results.schema());

    let expected = vec![
        "+---+----+",
        "| a | b  |",
        "+---+----+",
        "| 1 | b1 |",
        "| 2 | b2 |",
        "| 3 | b3 |",
        "| 4 | b1 |",
        "| 5 | b2 |",
        "| 6 | b3 |",
        "| 7 | b1 |",
        "| 8 | b2 |",
        "| 9 | b3 |",
        "+---+----+",
    ];
    common_datablocks::assert_blocks_eq(expected, &[results]);
    Ok(())
}
