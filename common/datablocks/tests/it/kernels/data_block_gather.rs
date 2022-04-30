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
use pretty_assertions::assert_eq;

#[test]
fn test_data_block_gather() -> Result<()> {
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", i64::to_data_type()),
        DataField::new("b", u64::to_data_type()),
    ]);

    let num = 111;

    let raw1 = DataBlock::create(schema.clone(), vec![
        Series::from_data((0..num as i64).step_by(3).collect::<Vec<_>>()),
        Series::from_data((0..num as u64).step_by(3).collect::<Vec<_>>()),
    ]);

    let raw2 = DataBlock::create(schema.clone(), vec![
        Series::from_data((1..num as i64).step_by(3).collect::<Vec<_>>()),
        Series::from_data((1..num as u64).step_by(3).collect::<Vec<_>>()),
    ]);

    let raw3 = DataBlock::create(schema.clone(), vec![
        Series::from_data((2..num as i64).step_by(3).collect::<Vec<_>>()),
        Series::from_data((2..num as u64).step_by(3).collect::<Vec<_>>()),
    ]);

    let mut a = 0;
    let mut b = 0;
    let mut c = 0;

    let indices: Vec<(usize, usize)> = (0..num)
        .map(|i| {
            if i % 3 == 0 {
                a += 1;
                (0, a - 1)
            } else if i % 3 == 1 {
                b += 1;
                (1, b - 1)
            } else {
                c += 1;
                (2, c - 1)
            }
        })
        .collect();

    let gathered = DataBlock::gather_blocks(&[raw1, raw2, raw3], &indices)?;
    assert_eq!(gathered.num_rows(), num);

    let expected = DataBlock::create(schema, vec![
        Series::from_data((0..num as i64).collect::<Vec<_>>()),
        Series::from_data((0..num as u64).collect::<Vec<_>>()),
    ]);

    assert_eq!(gathered, expected);
    Ok(())
}
