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

use common_arrow::arrow::record_batch::RecordBatch;
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
        DataField::new("a", Date16Type::arc()),
        DataField::new("b", Date32Type::arc()),
        DataField::new("c", DateTime32Type::arc(None)),
        DataField::new("c", DateTime64Type::arc(3, None)),
    ]);

    let block = DataBlock::create(schema.clone(), vec![
        Series::from_data(vec![1u16, 2, 3]),
        Series::from_data(vec![1i32, 2, 3]),
        Series::from_data(vec![1u32, 2, 3]),
        Series::from_data(vec![1i64, 2, 3]),
    ]);
    assert_eq!(&schema, block.schema());

    assert_eq!(3, block.num_rows());
    assert_eq!(4, block.num_columns());

    let record_batch: RecordBatch = block.try_into().unwrap();

    // first and last test.
    assert_eq!(3, record_batch.num_rows());
    assert_eq!(4, record_batch.num_columns());

    let new_block: DataBlock = record_batch.try_into().unwrap();
    assert_eq!(3, new_block.num_rows());
    assert_eq!(4, new_block.num_columns());

    let new_schema = new_block.schema();

    assert_eq!(new_schema, &schema);
    Ok(())
}
