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

use common_datavalues::prelude::*;
use common_expression::converts::can_convert;
use common_expression::schema::TableSchema as OurTableSchema;
use common_expression::types::DataType as OurDataType;
use common_expression::values::Column as EColumn;
use common_expression::DataBlock;
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
    let num_cols = 20;
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

    let arrow_arrays = columns
        .iter()
        .zip(schema.fields().iter())
        .map(|(c, f)| c.as_arrow_array(f.data_type().clone()))
        .collect::<Vec<_>>();

    let arrow_schema = schema.to_arrow();
    let our_schmea = OurTableSchema::from(&arrow_schema);
    let arrow_schem2 = our_schmea.to_arrow();

    assert_eq!(arrow_schema, arrow_schem2);

    let mut our_columns = Vec::with_capacity(num_cols);
    for (f, arrow_col) in our_schmea.fields().iter().zip(arrow_arrays.iter()) {
        let data_type = f.data_type();
        let data_type = OurDataType::from(data_type);
        let our_column = EColumn::from_arrow(arrow_col.as_ref(), &data_type);
        our_columns.push(our_column);
    }

    let our_block = DataBlock::new_from_columns(our_columns);
    let arrow_arrays2 = our_block
        .columns()
        .iter()
        .map(|c| c.value.clone().into_column().unwrap().as_arrow())
        .collect::<Vec<_>>();

    for (a1, a2) in arrow_arrays.iter().zip(arrow_arrays2.iter()) {
        assert_eq!(a1, a2);
    }
}

#[test]
fn test_converts() {
    for _ in 0..1000 {
        test_convert();
    }
}
