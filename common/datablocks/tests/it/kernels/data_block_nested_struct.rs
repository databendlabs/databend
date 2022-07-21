//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use common_arrow::arrow::io::parquet::write::to_parquet_schema;
use common_datablocks::*;
use common_datavalues::prelude::*;
use common_exception::Result;

#[test]
fn test_data_block_nested() -> Result<()> {
    let data_type = StructType::create(vec!["a".to_owned(), "b".to_owned()], vec![
        i64::to_data_type(),
        f64::to_data_type(),
    ]);

    //   struct {
    //      a : i64,
    //      b: f64,
    //      c: struct {
    //          a: i64,
    //          b: f64
    //      }
    //   }

    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", i64::to_data_type()),
        DataField::new("b", f64::to_data_type()),
        DataField::new("c", DataTypeImpl::Struct(data_type.clone())),
    ]);

    let struct_column: ColumnRef = std::sync::Arc::new(StructColumn::from_data(
        vec![
            Series::from_data(vec![1i64, 2, 3]),
            Series::from_data(vec![1.0f64, 2., 3.]),
        ],
        DataTypeImpl::Struct(data_type),
    ));

    let raw = DataBlock::create(schema, vec![
        Series::from_data(vec![1i64, 2, 3]),
        Series::from_data(vec![1.0f64, 2., 3.]),
        struct_column,
    ]);

    // datablock schema is not flattened
    assert_eq!(raw.num_columns(), 3);

    // arrow schema is not flattened
    let arrow_schema = raw.schema().to_arrow();
    assert_eq!(arrow_schema.fields.len(), 3);

    let parquet_schema = to_parquet_schema(&arrow_schema)?;

    // parquet fields are not flattened
    assert_eq!(parquet_schema.fields().len(), 3);

    // parquet columns are flattened - leaves only
    assert_eq!(parquet_schema.columns().len(), 4);
    assert_eq!(
        parquet_schema
            .columns()
            .iter()
            .map(|v| v.path_in_schema.join("."))
            .collect::<Vec<_>>(),
        vec!["a", "b", "c.a", "c.b"]
            .into_iter()
            .map(|v| v.to_owned())
            .collect::<Vec<_>>(),
    );

    Ok(())
}
