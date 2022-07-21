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

use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::ColumnRef;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataTypeImpl;
use common_datavalues::Series;
use common_datavalues::SeriesFrom;
use common_datavalues::StructColumn;
use common_datavalues::StructType;
use common_datavalues::ToDataType;
use common_exception::Result;
use databend_query::storages::fuse::statistics::traverse;
#[test]
fn test_column_traverse() -> Result<()> {
    let nested_layer_1 = StructType::create(vec!["c".to_owned(), "d".to_owned()], vec![
        i64::to_data_type(),
        f64::to_data_type(),
    ]);

    let nested_layer_0 = StructType::create(vec!["b".to_owned(), "e".to_owned()], vec![
        DataTypeImpl::Struct(nested_layer_1.clone()),
        f64::to_data_type(),
    ]);

    //   sample message
    //
    //   struct {
    //      a: struct {
    //          b: struct {
    //             c: i64,
    //             d: f64,
    //          },
    //          e: f64
    //      }
    //      f : i64,
    //      g: f64,
    //   }
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", DataTypeImpl::Struct(nested_layer_0.clone())),
        DataField::new("f", i64::to_data_type()),
        DataField::new("g", f64::to_data_type()),
    ]);

    // prepare four cols
    let col_c = Series::from_data(vec![1i64, 2, 3]);
    let col_d = Series::from_data(vec![1.0f64, 2., 3.]);
    let col_e = Series::from_data(vec![4.0f64, 5., 6.]);
    let col_f = Series::from_data(vec![7i64, 8, 9]);
    let col_g = Series::from_data(vec![10.0f64, 11., 12.]);
    let col_b: ColumnRef = Arc::new(StructColumn::from_data(
        vec![col_c.clone(), col_d.clone()],
        DataTypeImpl::Struct(nested_layer_1),
    ));
    let col_a: ColumnRef = Arc::new(StructColumn::from_data(
        vec![col_b.clone(), col_e.clone()],
        DataTypeImpl::Struct(nested_layer_0),
    ));

    let raw = DataBlock::create(schema, vec![col_a.clone(), col_f.clone(), col_g.clone()]);

    let cols = traverse::traverse_columns_dfs(raw.columns())?;

    // recall

    //   struct {
    //      a: struct {
    //          b: struct {
    //             c: i64,
    //             d: f64,
    //          },
    //          e: f64
    //      }
    //      f : i64,
    //      g: f64,
    //   }
    assert_eq!(5, cols.len());
    assert_eq!(cols[0], col_c);
    assert_eq!(cols[1], col_d);
    assert_eq!(cols[2], col_e);
    assert_eq!(cols[3], col_f);
    assert_eq!(cols[4], col_g);

    Ok(())
}
