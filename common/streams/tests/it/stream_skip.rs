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
async fn test_skipstream() {
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("id", i32::to_data_type()),
        DataField::new("name", Vu8::to_data_type()),
    ]);

    // create a data block with 'id' from 0 to 20
    let ids = (0..20).collect::<Vec<i32>>();
    let names = (0..20)
        .map(|n| {
            let str = format!("Alice-{}", n);
            str.into_bytes()
        })
        .collect::<Vec<Vec<u8>>>();
    let block0 = DataBlock::create(schema.clone(), vec![
        Series::from_data(ids),
        Series::from_data(names),
    ]);

    // create a data block with 'id' from 20 to 40
    let ids = (20..40).collect::<Vec<i32>>();
    let names = (20..40)
        .map(|n| {
            let str = format!("Bob-{}", n);
            str.into_bytes()
        })
        .collect::<Vec<Vec<u8>>>();
    let block1 = DataBlock::create(schema.clone(), vec![
        Series::from_data(ids),
        Series::from_data(names),
    ]);

    let stream = DataBlockStream::create(schema, None, vec![block0, block1]);

    // skip the number from 0 to 24 and the return block should be in range [25, 40)
    let mut skip_stream = SkipStream::new(Box::pin(stream), 25);

    let expected = vec![
        "+----+--------+",
        "| id | name   |",
        "+----+--------+",
        "| 25 | Bob-25 |",
        "| 26 | Bob-26 |",
        "| 27 | Bob-27 |",
        "| 28 | Bob-28 |",
        "| 29 | Bob-29 |",
        "| 30 | Bob-30 |",
        "| 31 | Bob-31 |",
        "| 32 | Bob-32 |",
        "| 33 | Bob-33 |",
        "| 34 | Bob-34 |",
        "| 35 | Bob-35 |",
        "| 36 | Bob-36 |",
        "| 37 | Bob-37 |",
        "| 38 | Bob-38 |",
        "| 39 | Bob-39 |",
        "+----+--------+",
    ];

    let mut index = 0;
    while let Some(res) = skip_stream.next().await {
        assert!(res.is_ok());
        let data_bock = res.unwrap();
        match index {
            0 => assert_blocks_eq(expected.clone(), &[data_bock]),
            _ => panic!(),
        }
        index += 1;
    }
}
