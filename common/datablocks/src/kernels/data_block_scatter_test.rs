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
fn test_data_block_scatter() -> Result<()> {
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", DataType::Int64, false),
        DataField::new("b", DataType::Float64, false),
    ]);

    let raw = DataBlock::create(schema, vec![
        Series::new(vec![1i64, 2, 3]).into(),
        Series::new(vec![1.0f64, 2., 3.]).into(),
    ]);

    let indices = DataColumn::Array(Series::new([0u64, 1, 0]));
    let scattered = DataBlock::scatter_block(&raw, &indices, 2)?;
    assert_eq!(scattered.len(), 2);
    assert_eq!(raw.schema(), scattered[0].schema());
    assert_eq!(raw.schema(), scattered[1].schema());
    assert_eq!(scattered[0].num_rows(), 2);
    assert_eq!(scattered[1].num_rows(), 1);

    let expected = vec![
        "+---+---+",
        "| a | b |",
        "+---+---+",
        "| 1 | 1 |",
        "| 3 | 3 |",
        "| 2 | 2 |",
        "+---+---+",
    ];
    crate::assert_blocks_eq(expected, &scattered);

    Ok(())
}
