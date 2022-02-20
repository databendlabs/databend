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

use common_base::tokio;
use common_datablocks::*;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_streams::*;
use futures::stream::StreamExt;

#[tokio::test]
async fn test_limitby_stream() -> Result<()> {
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("id", u8::to_data_type()),
        DataField::new("name", Vu8::to_data_type()),
    ]);

    let ids = vec![2u8, 2, 2, 2, 3, 3, 3];
    let names = vec!["2-1", "2-1", "2-1", "2-2", "3-1", "3-1", "3-2"];
    let block0 = DataBlock::create(schema.clone(), vec![
        Series::from_data(ids),
        Series::from_data(names),
    ]);

    let ids = vec![2u8, 2, 3u8, 3];
    let names = vec!["2-2", "2-2", "3-1", "3-2"];
    let block1 = DataBlock::create(schema.clone(), vec![
        Series::from_data(ids),
        Series::from_data(names),
    ]);

    let input = DataBlockStream::create(schema.clone(), None, vec![block0.clone(), block1.clone()]);
    // test with limit = 2
    let mut stream = LimitByStream::try_create(Box::pin(input), 2, vec![
        "id".to_string(),
        "name".to_string(),
    ])
    .unwrap();

    let expected = vec![
        vec![
            "+----+------+",
            "| id | name |",
            "+----+------+",
            "| 2  | 2-1  |",
            "| 2  | 2-1  |",
            "| 2  | 2-2  |",
            "| 3  | 3-1  |",
            "| 3  | 3-1  |",
            "| 3  | 3-2  |",
            "+----+------+",
        ],
        vec![
            "+----+------+",
            "| id | name |",
            "+----+------+",
            "| 2  | 2-2  |",
            "| 3  | 3-2  |",
            "+----+------+",
        ],
    ];

    let mut index = 0usize;
    while let Some(res) = stream.next().await {
        assert!(res.is_ok());
        let data_block = res.unwrap();
        match index {
            0 | 1 => {
                common_datablocks::assert_blocks_sorted_eq(expected[index].clone(), &[data_block])
            }
            _ => panic!(),
        }
        index += 1;
    }

    let input = DataBlockStream::create(schema, None, vec![block0, block1]);
    // test with limit = 1
    let mut stream = LimitByStream::try_create(Box::pin(input), 1, vec![
        "id".to_string(),
        "name".to_string(),
    ])
    .unwrap();

    let expected = vec![
        vec![
            "+----+------+",
            "| id | name |",
            "+----+------+",
            "| 2  | 2-1  |",
            "| 2  | 2-2  |",
            "| 3  | 3-1  |",
            "| 3  | 3-2  |",
            "+----+------+",
        ],
        vec![
            "+----+------+",
            "| id | name |",
            "+----+------+",
            "+----+------+",
        ],
    ];

    let mut index = 0usize;
    while let Some(res) = stream.next().await {
        assert!(res.is_ok());
        let data_block = res.unwrap();
        match index {
            0 | 1 => {
                common_datablocks::assert_blocks_sorted_eq(expected[index].clone(), &[data_block])
            }
            _ => panic!(),
        }
        index += 1;
    }

    Ok(())
}
