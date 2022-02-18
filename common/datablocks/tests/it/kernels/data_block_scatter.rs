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
fn test_data_block_scatter() -> Result<()> {
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", i64::to_data_type()),
        DataField::new("b", f64::to_data_type()),
    ]);

    let raw = DataBlock::create(schema, vec![
        Series::from_data(vec![1i64, 2, 3]),
        Series::from_data(vec![1.0f64, 2., 3.]),
    ]);

    let indices = &[0usize, 1, 0];
    let scattered = DataBlock::scatter_block(&raw, indices, 2)?;
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
    common_datablocks::assert_blocks_eq(expected, &scattered);

    Ok(())
}
