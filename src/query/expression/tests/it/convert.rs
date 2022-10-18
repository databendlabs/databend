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

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_expression::converts::can_convert;
use common_expression::converts::from_block;
use common_expression::converts::to_datablock;
use rand::Rng;

fn random_type() -> DataTypeImpl {
    let mut rng = rand::thread_rng();

    loop {
        let datatype = match rng.gen_range(0..21) {
            0 => DataTypeImpl::Null(NullType {}),
            1 => NullableType::new_impl(u64::to_data_type()),
            2 => DataTypeImpl::Boolean(BooleanType {}),
            3 => i8::to_data_type(),
            4 => i16::to_data_type(),
            5 => i32::to_data_type(),
            6 => i64::to_data_type(),
            7 => u8::to_data_type(),
            8 => u16::to_data_type(),
            9 => u32::to_data_type(),
            10 => u64::to_data_type(),
            11 => f32::to_data_type(),
            12 => f64::to_data_type(),

            13 => DataTypeImpl::Date(DateType {}),
            14 => TimestampType::new_impl(),
            15 => DataTypeImpl::String(StringType {}),
            16 => DataTypeImpl::Struct(StructType::create(None, vec![
                f32::to_data_type(),
                Vu8::to_data_type(),
            ])),
            17 => ArrayType::new_impl(f32::to_data_type()),
            18 => DataTypeImpl::VariantArray(VariantArrayType {}),
            19 => DataTypeImpl::VariantObject(VariantObjectType {}),
            _ => DataTypeImpl::Variant(VariantType {}),
        };
        if can_convert(&datatype) {
            return datatype;
        }
    }
}

fn random_schema(num_cols: usize) -> DataSchemaRef {
    let fields = (0..num_cols)
        .map(|i| {
            let name = format!("col_{}", i);
            DataField::new(name.as_str(), random_type())
        })
        .collect();
    Arc::new(DataSchema::new(fields))
}

fn test_convert() {
    let num_cols = 10;
    let num_rows = 32;

    let mut rng = rand::thread_rng();
    let schema = random_schema(num_cols);
    let columns: Vec<ColumnRef> = schema
        .fields()
        .iter()
        .map(|f| {
            let column = f.data_type().create_random_column(num_rows);
            match rng.gen_range(0..=1) {
                0 => column,
                _ => f
                    .data_type()
                    .create_constant_column(&column.get(0), num_rows)
                    .unwrap(),
            }
        })
        .collect();

    let block = DataBlock::create(schema.clone(), columns);

    let chunk = from_block(&block);
    {
        assert_eq!(chunk.num_rows(), num_rows);
        assert_eq!(chunk.num_columns(), num_cols);
    }

    let block2 = to_datablock(&chunk, schema);
    assert_eq!(block, block2);
}

#[test]
fn test_converts() {
    for _ in 0..1000 {
        test_convert();
    }
}
