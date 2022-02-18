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

use std::collections::HashMap;

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_planners::col;
use common_planners::lit;
use databend_query::storages::index::IndexSchemaVersion;
use databend_query::storages::index::SparseIndex;
use databend_query::storages::index::SparseIndexValue;
use pretty_assertions::assert_eq;

#[test]
fn test_sparse_index() -> Result<()> {
    let schema = DataSchemaRefExt::create(vec![
        DataField::new_nullable("name", Vu8::to_data_type()),
        DataField::new("age", i32::to_data_type()),
    ]);

    let block1 = DataBlock::create(schema.clone(), vec![
        Series::from_data(vec!["jack", "ace", "bohu"]),
        Series::from_data(vec![11, 6, 24]),
    ]);

    let block2 = DataBlock::create(schema, vec![
        Series::from_data(vec!["xjack", "xace", "xbohu"]),
        Series::from_data(vec![11, 6, 24]),
    ]);

    let idx_slice = vec![
        SparseIndex {
            col: "name".to_string(),
            values: vec![
                SparseIndexValue {
                    min: DataValue::String("jack".as_bytes().to_vec()),
                    max: DataValue::String("bohu".as_bytes().to_vec()),
                    page_no: 0,
                },
                SparseIndexValue {
                    min: DataValue::String("xjack".as_bytes().to_vec()),
                    max: DataValue::String("xbohu".as_bytes().to_vec()),
                    page_no: 1,
                },
            ],
            version: IndexSchemaVersion::V1,
        },
        SparseIndex {
            col: "age".to_string(),
            values: vec![
                SparseIndexValue {
                    min: DataValue::Int64(11),
                    max: DataValue::Int64(24),
                    page_no: 0,
                },
                SparseIndexValue {
                    min: DataValue::Int64(11),
                    max: DataValue::Int64(24),
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
