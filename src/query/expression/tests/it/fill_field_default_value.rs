// Copyright 2021 Datafuse Labs
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

use std::collections::HashSet;

use databend_common_arrow::arrow::chunk::Chunk;
use databend_common_arrow::ArrayRef;
use databend_common_exception::Result;
use databend_common_expression::types::number::*;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::StringType;
use databend_common_expression::*;
use goldenfile::Mint;

use crate::common::*;

#[test]
fn test_data_block_create_with_default_value_functions() -> Result<()> {
    let mut mint = Mint::new("tests/it/testdata");
    let mut file = mint.new_goldenfile("fill_field_default_value.txt").unwrap();

    // test create_with_default_value_and_chunk
    {
        let schema = DataSchemaRefExt::create(vec![
            DataField::new("a", DataType::Number(NumberDataType::Int32)),
            DataField::new("b", DataType::Number(NumberDataType::Int32)),
            DataField::new("c", DataType::Number(NumberDataType::Float32)),
            DataField::new("d", DataType::Number(NumberDataType::Int32)),
            DataField::new("e", DataType::String),
        ]);

        let num_rows = 3;
        let chunk_block = new_block(&[
            Int32Type::from_data(vec![1i32, 2, 3]),
            Float32Type::from_data(vec![1.0, 2.0, 3.0]),
            StringType::from_data(vec!["x1", "x2", "x3"]),
        ]);

        let chunks: Chunk<ArrayRef> = chunk_block.try_into().unwrap();
        let default_vals = vec![
            None,
            Some(Scalar::Number(NumberScalar::Int32(2))),
            None,
            Some(Scalar::Number(NumberScalar::Int32(4))),
            None,
        ];
        let new_block: DataBlock = DataBlock::create_with_default_value_and_chunk(
            &schema,
            &chunks,
            &default_vals,
            num_rows,
        )
        .unwrap();

        run_take(&mut file, &[0, 1, 2], &new_block);
    }

    // test create_with_default_value
    {
        let fields = vec![
            DataField::new("a", DataType::Number(NumberDataType::Int32))
                .with_default_expr(Some("1".to_string())),
            DataField::new("b", DataType::Number(NumberDataType::Int32)),
            DataField::new("c", DataType::Number(NumberDataType::Float32))
                .with_default_expr(Some("10".to_string())),
            DataField::new("d", DataType::Number(NumberDataType::Int32)),
            DataField::new("e", DataType::String).with_default_expr(Some("ab".to_string())),
        ];
        let schema = DataSchemaRefExt::create(fields);

        let num_rows = 3;
        let default_vals = vec![
            Scalar::Number(NumberScalar::Int32(1)),
            Scalar::Number(NumberScalar::Int32(0)),
            Scalar::Number(NumberScalar::Float32(ordered_float::OrderedFloat(10.0))),
            Scalar::Number(NumberScalar::Int32(0)),
            Scalar::String("ab".into()),
        ];
        let new_block: DataBlock =
            DataBlock::create_with_default_value(&schema, &default_vals[0..], num_rows).unwrap();

        run_take(&mut file, &[0, 1, 2], &new_block);
    }

    // test create_with_default_value_and_block
    {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("a", TableDataType::Number(NumberDataType::Int32)),
            TableField::new("b", TableDataType::Number(NumberDataType::Int32)),
            TableField::new("c", TableDataType::Number(NumberDataType::Float32)),
            TableField::new("d", TableDataType::Number(NumberDataType::Int32)),
            TableField::new("e", TableDataType::String),
        ]);

        let block = new_block(&[
            Int32Type::from_data(vec![1i32, 2, 3]),
            Int32Type::from_data(vec![4i32, 5, 6]),
        ]);

        let default_vals = vec![
            Scalar::Number(NumberScalar::Int32(1)),
            Scalar::Number(NumberScalar::Int32(2)),
            Scalar::Number(NumberScalar::Float32(ordered_float::OrderedFloat(10.0))),
            Scalar::Number(NumberScalar::Int32(3)),
            Scalar::String("ab".into()),
        ];

        let mut block_column_ids = HashSet::new();
        block_column_ids.insert(0);
        block_column_ids.insert(3);

        let new_block: DataBlock = DataBlock::create_with_default_value_and_block(
            &schema,
            &block,
            &block_column_ids,
            &default_vals,
        )
        .unwrap();

        run_take(&mut file, &[0, 1, 2], &new_block);
    }

    Ok(())
}
