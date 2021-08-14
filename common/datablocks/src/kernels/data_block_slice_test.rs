// Copyright 2020 Datafuse Labs.
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

use common_datavalues::prelude::*;
use common_exception::Result;

use crate::*;

#[test]
fn test_data_block_slice() -> Result<()> {
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", DataType::Int64, false),
        DataField::new("b", DataType::Float64, false),
    ]);

    let raw = DataBlock::create(schema.clone(), vec![
        Series::new(vec![1i64, 2, 3, 4, 5]).into(),
        Series::new(vec![1.0f64, 2., 3., 4., 5.]).into(),
    ]);

    let sliced = DataBlock::split_block_by_size(&raw, 1)?;
    assert_eq!(sliced.len(), 5);

    let expected = vec![
        "+---+---+",
        "| a | b |",
        "+---+---+",
        "| 1 | 1 |",
        "| 2 | 2 |",
        "| 3 | 3 |",
        "| 4 | 4 |",
        "| 5 | 5 |",
        "+---+---+",
    ];
    crate::assert_blocks_eq(expected, &sliced);
    Ok(())
}
