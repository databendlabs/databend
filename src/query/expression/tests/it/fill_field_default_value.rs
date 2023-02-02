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

use common_arrow::arrow::chunk::Chunk;
use common_arrow::ArrayRef;
use common_exception::Result;
use common_expression::types::number::*;
use common_expression::types::DataType;
use common_expression::types::NumberDataType;
use common_expression::types::StringType;
use common_expression::*;
use pretty_assertions::assert_eq;

use crate::common::new_block;

#[test]
fn test_data_block_create_with_schema_from_chunk() -> Result<()> {
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", DataType::Number(NumberDataType::Int32))
            .with_default_expr(Some("1".to_string())),
        DataField::new("b", DataType::Number(NumberDataType::Int32))
            .with_default_expr(Some("2".to_string())),
        DataField::new("c", DataType::Number(NumberDataType::Float32))
            .with_default_expr(Some("3.0".to_string())),
        DataField::new("d", DataType::Number(NumberDataType::Int32))
            .with_default_expr(Some("4".to_string()))
            .with_default_expr(Some("5".to_string())),
        DataField::new("e", DataType::String).with_default_expr(Some("6e".to_string())),
    ]);

    let num_rows = 3;
    let chunk_block = new_block(&[
        Int32Type::from_data(vec![1i32, 2, 3]),
        Float32Type::from_data(vec![1.0, 2.0, 3.0]),
        StringType::from_data(vec!["x1", "x2", "x3"]),
    ]);

    let chunks: Chunk<ArrayRef> = chunk_block.clone().try_into().unwrap();
    let use_field_default_vals = vec![None, Some(()), None, Some(()), None];
    let new_block: DataBlock = DataBlock::create_with_schema_from_chunk(
        &schema,
        &chunks,
        &use_field_default_vals,
        num_rows,
    )
    .unwrap();
    assert_eq!(3, new_block.num_rows());
    assert_eq!(5, new_block.num_columns());

    let mut chunk_index = 0;
    for (i, block) in new_block.columns().iter().enumerate() {
        let field = schema.fields()[i].clone();
        let column = &new_block.columns()[i];
        assert_eq!(block.data_type, field.data_type().clone());

        if use_field_default_vals[i].is_some() {
            let value = &block.value;
            let column = value.clone().into_column().unwrap();
            assert_eq!(column.len(), num_rows);
            for j in 0..column.len() {
                assert_eq!(
                    column.index(j).unwrap().to_owned(),
                    field.data_type().default_value()
                );
            }
        } else {
            let chunk = &chunk_block.columns()[chunk_index].clone();
            assert_eq!(chunk.data_type, column.data_type);
            assert_eq!(chunk.value, column.value);
            chunk_index += 1;
        }
    }

    Ok(())
}
