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

use std::fs::File;

use common_arrow::arrow::array::StructArray;
use common_arrow::arrow::datatypes::DataType;
use common_arrow::arrow::error::Result;
use common_arrow::arrow::io::parquet::read::infer_schema;
use common_arrow::arrow::io::parquet::read::n_columns;
use common_arrow::arrow::io::parquet::read::nested_column_iter_to_arrays;
use common_arrow::arrow::io::parquet::read::read_columns;
use common_arrow::arrow::io::parquet::read::read_metadata;
use common_arrow::arrow::io::parquet::read::to_deserializer;
use common_arrow::arrow::io::parquet::read::BasicDecompressor;
use common_arrow::arrow::io::parquet::read::InitNested;
use common_arrow::arrow::io::parquet::read::PageReader;

#[test]
fn test_deserialize_nested_column() -> Result<()> {
    let path = "testing/parquet-testing/data/nested_structs.rust.parquet";
    let mut reader = File::open(path).unwrap();

    let metadata = read_metadata(&mut reader)?;
    let schema = infer_schema(&metadata)?;

    let num_rows = metadata.num_rows;
    let row_group = metadata.row_groups[0].clone();

    let field_columns = schema
        .fields
        .iter()
        .map(|field| read_columns(&mut reader, row_group.columns(), &field.name))
        .collect::<Result<Vec<_>>>()?;

    let fields = schema.fields.clone();
    for (mut columns, field) in field_columns.into_iter().zip(fields.iter()) {
        if let DataType::Struct(inner_fields) = &field.data_type {
            let mut array_iter =
                to_deserializer(columns.clone(), field.clone(), num_rows, None, None)?;
            let array = array_iter.next().transpose()?.unwrap();
            let expected_array = array
                .as_any()
                .downcast_ref::<StructArray>()
                .unwrap()
                .clone();

            // deserialize inner values of struct fields.
            let init = vec![InitNested::Struct(field.is_nullable)];
            let mut values = Vec::with_capacity(inner_fields.len());
            for inner_field in inner_fields {
                let n = n_columns(&inner_field.data_type);
                let inner_columns: Vec<_> = columns.drain(0..n).collect();

                let (nestd_columns, types): (Vec<_>, Vec<_>) = inner_columns
                    .into_iter()
                    .map(|(column_meta, chunk)| {
                        let len = chunk.len();
                        let pages = PageReader::new(
                            std::io::Cursor::new(chunk),
                            column_meta,
                            std::sync::Arc::new(|_, _| true),
                            vec![],
                            len * 2 + 1024,
                        );
                        (
                            BasicDecompressor::new(pages, vec![]),
                            &column_meta.descriptor().descriptor.primitive_type,
                        )
                    })
                    .unzip();

                let mut inner_array_iter = nested_column_iter_to_arrays(
                    nestd_columns,
                    types,
                    inner_field.clone(),
                    init.clone(),
                    None,
                    num_rows,
                )?;
                let inner_array = inner_array_iter.next().transpose()?;
                values.push(inner_array.unwrap());
            }
            let struct_array = StructArray::try_new(field.data_type.clone(), values, None)?;

            assert_eq!(expected_array, struct_array);
        }
    }

    Ok(())
}
