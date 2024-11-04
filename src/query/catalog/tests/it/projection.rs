// Copyright 2023 Datafuse Labs.
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

use std::collections::BTreeMap;

use databend_common_catalog::plan::Projection;
use databend_common_exception::Result;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchema;
use parquet::arrow::arrow_to_parquet_schema;

#[test]
fn test_to_projection_mask() -> Result<()> {
    // Test schema (6 physical columns):
    // a: Int32,            (leave id: 0, path: [0])
    // b: Tuple (
    //    c: Int32,         (leave id: 1, path: [1, 0])
    //    d: Tuple (
    //        e: Int32,     (leave id: 2, path: [1, 1, 0])
    //        f: String,    (leave id: 3, path: [1, 1, 1])
    //    ),
    //    g: String,        (leave id: 4, path: [1, 2])
    // )
    // h: String,           (leave id: 5, path: [2])
    let schema = TableSchema::new(vec![
        TableField::new("a", TableDataType::Number(NumberDataType::Int32)),
        TableField::new("b", TableDataType::Tuple {
            fields_name: vec!["c".to_string(), "d".to_string(), "g".to_string()],
            fields_type: vec![
                TableDataType::Number(NumberDataType::Int32),
                TableDataType::Tuple {
                    fields_name: vec!["e".to_string(), "f".to_string()],
                    fields_type: vec![
                        TableDataType::Number(NumberDataType::Int32),
                        TableDataType::String,
                    ],
                },
                TableDataType::String,
            ],
        }),
        TableField::new("h", TableDataType::String),
    ]);
    let arrow_schema = (&schema).into();
    let schema_desc = arrow_to_parquet_schema(&arrow_schema)?;

    // (Projection, ProjectionMask)
    let test_cases: Vec<(Projection, Vec<usize>)> = vec![
        // Projection::Columns
        (Projection::Columns(vec![0, 2]), vec![0, 5]),
        (Projection::Columns(vec![0, 1]), vec![0, 1, 2, 3, 4]),
        (Projection::Columns(vec![1, 2]), vec![1, 2, 3, 4, 5]),
        // Projection::InnerColumns
        (
            Projection::InnerColumns(BTreeMap::from([(0, vec![1, 0])])),
            vec![1],
        ),
        (
            Projection::InnerColumns(BTreeMap::from([(0, vec![1, 1, 0])])),
            vec![2],
        ),
        (
            Projection::InnerColumns(BTreeMap::from([(0, vec![1, 1, 1])])),
            vec![3],
        ),
        (
            Projection::InnerColumns(BTreeMap::from([(0, vec![1, 1, 0]), (1, vec![1, 1, 1])])),
            vec![2, 3],
        ),
        (
            Projection::InnerColumns(BTreeMap::from([(0, vec![1, 1])])),
            vec![2, 3],
        ),
        (
            Projection::InnerColumns(BTreeMap::from([(0, vec![1, 0]), (1, vec![1, 2])])),
            vec![1, 4],
        ),
        (
            Projection::InnerColumns(BTreeMap::from([
                (0, vec![1, 0]),
                (1, vec![1, 2]),
                (2, vec![1, 1, 1]),
            ])),
            vec![1, 4, 3],
        ),
        (
            Projection::InnerColumns(BTreeMap::from([
                (0, vec![0]),
                (1, vec![1, 1, 0]),
                (2, vec![1, 2]),
                (3, vec![2]),
            ])),
            vec![0, 2, 4, 5],
        ),
    ];

    for (projection, expected_mask) in test_cases.iter() {
        let (mask, _) = projection.to_arrow_projection(&schema_desc);
        for leaf in expected_mask {
            assert!(
                mask.leaf_included(*leaf),
                "mask: {:?}, expected mask: {:?}",
                mask,
                expected_mask
            );
        }
    }

    Ok(())
}
