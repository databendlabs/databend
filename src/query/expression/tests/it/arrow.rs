// Copyright 2022 Datafuse Labs.
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

use std::sync::Arc;

use arrow_array::ArrayRef;
use arrow_array::DictionaryArray;
use arrow_array::Int32Array;
use arrow_array::StringArray;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::Scalar;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Int32Type;
use databend_common_expression::types::Int64Type;
use databend_common_expression::types::NumberDataType;

#[test]
fn test_from_arrow_rs() {
    let array = Arc::new(StringArray::from(vec![
        Some("foo"),
        Some("bar"),
        None,
        Some("baz"),
    ])) as ArrayRef;

    let array_k = Int32Array::from(vec![0, 3, 2, 2]);
    let array_v = Arc::new(StringArray::from(vec![
        Some("foo"),
        Some("bar"),
        None,
        Some("baz"),
    ])) as ArrayRef;

    let array_dict = Arc::new(DictionaryArray::new(array_k, array_v)) as _;

    for arr in [array, array_dict] {
        let column = Column::from_arrow_rs(arr.clone(), &DataType::String.wrap_nullable()).unwrap();
        let c = column.into_arrow_rs();
        assert_eq!(c.len(), arr.len());
    }
}

fn null_const_round_trip_block(num_rows: usize) -> DataBlock {
    let nullable_int = DataType::Number(NumberDataType::Int64).wrap_nullable();
    DataBlock::new(
        vec![
            Int32Type::from_data((0..num_rows as i32).collect::<Vec<_>>()).into(),
            BlockEntry::new_const_column(nullable_int.clone(), Scalar::Null, num_rows),
            BlockEntry::new_const_column(DataType::String.wrap_nullable(), Scalar::Null, num_rows),
            // Some NULLs, but not all.
            Int64Type::from_opt_data((0..num_rows as i64).map(|i| (i > 0).then_some(i)).collect())
                .into(),
            // A nullable column that is all NULL as a full column.
            BlockEntry::new_const_column(nullable_int, Scalar::Null, num_rows)
                .to_column()
                .into(),
        ],
        num_rows,
    )
}

#[test]
fn test_from_record_batch_keep_null_const() -> Result<()> {
    let block = null_const_round_trip_block(4);
    let schema = block.infer_schema();
    let batch = block.clone().to_record_batch_with_dataschema(&schema)?;

    // The default conversion keeps every column full.
    let full = DataBlock::from_record_batch(&schema, &batch)?;
    assert!(
        full.columns()
            .iter()
            .all(|entry| entry.as_column().is_some())
    );

    let kept = DataBlock::from_record_batch_keep_null_const(&schema, &batch)?;
    assert_eq!(kept.num_rows(), 4);
    for (offset, is_const) in [false, true, true, false, true].into_iter().enumerate() {
        let entry = kept.get_by_offset(offset);
        assert_eq!(entry.as_scalar().is_some(), is_const, "column {offset}");
        if is_const {
            assert_eq!(entry.as_scalar(), Some(&Scalar::Null));
        }
        assert_eq!(entry.data_type(), block.get_by_offset(offset).data_type());
        assert_eq!(entry.to_column(), block.get_by_offset(offset).to_column());
    }
    Ok(())
}

#[test]
fn test_from_record_batch_keep_null_const_empty() -> Result<()> {
    // No rows: nothing is all NULL, a non-nullable column must stay a column.
    let block = null_const_round_trip_block(0);
    let schema = block.infer_schema();
    let batch = block.to_record_batch_with_dataschema(&schema)?;
    let kept = DataBlock::from_record_batch_keep_null_const(&schema, &batch)?;
    assert_eq!(kept.num_rows(), 0);
    assert!(
        kept.columns()
            .iter()
            .all(|entry| entry.as_column().is_some())
    );
    Ok(())
}
