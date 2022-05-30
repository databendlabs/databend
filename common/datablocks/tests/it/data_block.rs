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

use common_arrow::arrow::array::ArrayRef;
use common_arrow::arrow::chunk::Chunk;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use pretty_assertions::assert_eq;

#[test]
fn test_data_block() -> Result<()> {
    let schema = DataSchemaRefExt::create(vec![DataField::new("a", i64::to_data_type())]);

    let block = DataBlock::create(schema.clone(), vec![Series::from_data(vec![1, 2, 3])]);
    assert_eq!(&schema, block.schema());

    assert_eq!(3, block.num_rows());
    assert_eq!(1, block.num_columns());
    assert_eq!(3, block.try_column_by_name("a")?.len());
    assert_eq!(3, block.column(0).len());

    assert!(block.try_column_by_name("a").is_ok());
    assert!(block.try_column_by_name("a_not_found").is_err());

    // first and last test.
    assert_eq!(1, block.first("a")?.as_i64()?);
    assert_eq!(3, block.last("a")?.as_i64()?);

    Ok(())
}

#[test]
fn test_data_block_convert() -> Result<()> {
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", DateType::new_impl()),
        DataField::new("b", TimestampType::new_impl(0)),
        DataField::new("b", TimestampType::new_impl(3)),
    ]);

    let block = DataBlock::create(schema.clone(), vec![
        Series::from_data(vec![1i32, 2, 3]),
        Series::from_data(vec![1i64, 2, 3]),
        Series::from_data(vec![1i64, 2, 3]),
    ]);
    assert_eq!(&schema, block.schema());

    assert_eq!(3, block.num_rows());
    assert_eq!(3, block.num_columns());

    let chunk: Chunk<ArrayRef> = block.try_into().unwrap();

    // first and last test.
    assert_eq!(3, chunk.len());
    assert_eq!(3, chunk.columns().len());

    let new_block: DataBlock = DataBlock::from_chunk(&schema, &chunk).unwrap();
    assert_eq!(3, new_block.num_rows());
    assert_eq!(3, new_block.num_columns());

    Ok(())
}