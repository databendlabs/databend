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
use common_streams::*;
use futures::stream::StreamExt;

#[tokio::test]
async fn test_datablock_stream() {
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("name", i32::to_data_type()),
        DataField::new("age", Vu8::to_data_type()),
    ]);

    let data_blocks = vec![
        DataBlock::create(schema.clone(), vec![
            Series::from_data(vec!["a1", "a2", "a3"]),
            Series::from_data(vec![1i32, 1, 1]),
        ]),
        DataBlock::create(schema.clone(), vec![
            Series::from_data(vec!["b1", "b2", "b3"]),
            Series::from_data(vec![2i32, 2, 2]),
        ]),
        DataBlock::create(schema.clone(), vec![
            Series::from_data(vec!["c1", "c2", "c3"]),
            Series::from_data(vec![3i32, 3, 3]),
        ]),
    ];

    let expected = vec![
        vec![
            "+------+-----+",
            "| name | age |",
            "+------+-----+",
            "| a1   | 1   |",
            "| a2   | 1   |",
            "| a3   | 1   |",
            "+------+-----+",
        ],
        vec![
            "+------+-----+",
            "| name | age |",
            "+------+-----+",
            "| b1   | 2   |",
            "| b2   | 2   |",
            "| b3   | 2   |",
            "+------+-----+",
        ],
        vec![
            "+------+-----+",
            "| name | age |",
            "+------+-----+",
            "| c1   | 3   |",
            "| c2   | 3   |",
            "| c3   | 3   |",
            "+------+-----+",
        ],
    ];

    let mut stream = DataBlockStream::create(schema, None, data_blocks);
    let mut index = 0_usize;

    while let Some(res) = stream.next().await {
        assert!(res.is_ok());
        let data_block = res.unwrap();
        match index {
            0 | 1 | 2 => assert_blocks_eq(expected[index].clone(), &[data_block]),
            _ => panic!(),
        }
        index += 1;
    }
}
