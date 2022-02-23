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
fn test_data_block_group_by_hash() -> Result<()> {
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", i8::to_data_type()),
        DataField::new("b", i8::to_data_type()),
        DataField::new("c", i8::to_data_type()),
        DataField::new("x", Vu8::to_data_type()),
    ]);

    let block = DataBlock::create(schema, vec![
        Series::from_data(vec![1i8, 1, 2, 1, 2, 3]),
        Series::from_data(vec![1i8, 1, 2, 1, 2, 3]),
        Series::from_data(vec![1i8, 1, 2, 1, 2, 3]),
        Series::from_data(vec!["x1", "x1", "x2", "x1", "x2", "x3"]),
    ]);

    let method = DataBlock::choose_hash_method(&block, &["a".to_string(), "x".to_string()])?;
    assert_eq!(method.name(), HashMethodSerializer::default().name(),);

    let method = DataBlock::choose_hash_method(&block, &[
        "a".to_string(),
        "b".to_string(),
        "c".to_string(),
    ])?;

    assert_eq!(method.name(), HashMethodKeysU32::default().name());

    let hash = HashMethodKeysU32::default();
    let columns = vec!["a", "b", "c"];

    let mut group_columns = Vec::with_capacity(columns.len());
    {
        for col in columns {
            group_columns.push(block.try_column_by_name(col)?);
        }
    }

    let keys = hash.build_keys(&group_columns, block.num_rows())?;
    assert_eq!(keys, vec![
        0x10101, 0x10101, 0x20202, 0x10101, 0x20202, 0x30303
    ]);
    Ok(())
}
