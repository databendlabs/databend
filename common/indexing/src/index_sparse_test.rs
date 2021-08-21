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
//

use std::collections::HashMap;

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataType;
use common_exception::Result;
use common_planners::col;
use common_planners::lit;
use pretty_assertions::assert_eq;

use crate::IndexSchemaVersion;
use crate::SparseIndex;
use crate::SparseIndexValue;

#[test]
fn test_sparse_index() -> Result<()> {
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("name", DataType::Utf8, true),
        DataField::new("age", DataType::Int32, false),
    ]);

    let block1 = DataBlock::create_by_array(schema.clone(), vec![
        Series::new(vec!["jack", "ace", "bohu"]),
        Series::new(vec![11, 6, 24]),
    ]);

    let block2 = DataBlock::create_by_array(schema.clone(), vec![
        Series::new(vec!["xjack", "xace", "xbohu"]),
        Series::new(vec![11, 6, 24]),
    ]);

    let idx_slice = vec![
        SparseIndex {
            col: "name".to_string(),
            values: vec![
                SparseIndexValue {
                    min: DataValue::Utf8(Some("jack".to_string())),
                    max: DataValue::Utf8(Some("bohu".to_string())),
                    page_no: 0,
                },
                SparseIndexValue {
                    min: DataValue::Utf8(Some("xjack".to_string())),
                    max: DataValue::Utf8(Some("xbohu".to_string())),
                    page_no: 1,
                },
            ],
            version: IndexSchemaVersion::V1,
        },
        SparseIndex {
            col: "age".to_string(),
            values: vec![
                SparseIndexValue {
                    min: DataValue::Int32(Some(11)),
                    max: DataValue::Int32(Some(24)),
                    page_no: 0,
                },
                SparseIndexValue {
                    min: DataValue::Int32(Some(11)),
                    max: DataValue::Int32(Some(24)),
                    page_no: 1,
                },
            ],
            version: IndexSchemaVersion::V1,
        },
    ];

    // Create index.
    {
        let actual =
            SparseIndex::create_index(&["name".to_string(), "age".to_string()], &[block1, block2])?;
        let expected = idx_slice.clone();
        assert_eq!(actual, expected);
    }

    // Apply index.
    {
        let mut idx_map = HashMap::new();
        idx_map.insert("name".to_string(), idx_slice[0].clone());
        idx_map.insert("age".to_string(), idx_slice[1].clone());
        let expr = col("name").eq(lit(24));
        let (actual, _) = SparseIndex::apply_index(idx_map, &expr)?;
        let expected = true;
        assert_eq!(actual, expected);
    }

    Ok(())
}
