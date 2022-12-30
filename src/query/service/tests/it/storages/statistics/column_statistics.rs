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

use common_exception::Result;
use common_expression::types::number::Float64Type;
use common_expression::types::number::Int64Type;
use common_expression::types::ArgType;
use common_expression::types::DataType;
use common_expression::BlockEntry;
use common_expression::Column;
use common_expression::ColumnFrom;
use common_expression::DataBlock;
use common_expression::Scalar;
use common_expression::Value;
use databend_query::storages::fuse::statistics::gen_columns_statistics;

fn gen_sample_block() -> (DataBlock, Vec<Column>) {
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

    let col_b_type = DataType::Tuple(vec![Int64Type::data_type(), Float64Type::data_type()]);

    let col_a_type = DataType::Tuple(vec![col_b_type, Float64Type::data_type()]);

    // prepare leaves
    let col_c = Column::from_data(vec![1i64, 2, 3]);
    let col_d = Column::from_data(vec![1.0f64, 2., 3.]);
    let col_e = Column::from_data(vec![4.0f64, 5., 6.]);
    let col_f = Column::from_data(vec![7i64, 8, 9]);
    let col_g = Column::from_data(vec![10.0f64, 11., 12.]);

    // inner/root nodes
    let col_b = Column::Tuple {
        fields: vec![col_c.clone(), col_d.clone()],
        len: 3,
    };
    let col_a = Column::Tuple {
        fields: vec![col_b, col_e.clone()],
        len: 3,
    };

    let entries = vec![
        BlockEntry {
            data_type: col_a_type,
            value: Value::Column(col_a),
        },
        BlockEntry {
            data_type: Int64Type::data_type(),
            value: Value::Column(col_f.clone()),
        },
        BlockEntry {
            data_type: Float64Type::data_type(),
            value: Value::Column(col_g.clone()),
        },
    ];
    (DataBlock::new(entries, 3), vec![
        col_c, col_d, col_e, col_f, col_g,
    ])
}

#[test]
fn test_column_statistic() -> Result<()> {
    let (sample_block, sample_cols) = gen_sample_block();
    let col_stats = gen_columns_statistics(&sample_block, None)?;

    assert_eq!(5, col_stats.len());

    (0..5).for_each(|i| {
        let stats = col_stats.get(&(i as u32)).unwrap();
        let column = &sample_cols[i];
        let values: Vec<Scalar> = (0..column.len())
            .map(|i| column.index(i).unwrap().to_owned())
            .collect();
        assert_eq!(
            &stats.min,
            values.iter().min().unwrap(),
            "checking min of col {}",
            i
        );
        assert_eq!(
            &stats.max,
            values.iter().max().unwrap(),
            "checking max of col {}",
            i
        );
    });

    Ok(())
}
