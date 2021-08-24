// Copyright 2020 Datafuse Labs.
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

use common_datablocks::*;
use common_datavalues::prelude::*;
use common_runtime::tokio;
use futures::stream::StreamExt;

use crate::*;

#[tokio::test]
async fn test_skipstream() {
    let schema = DataSchemaRefExt::create(vec![DataField::new("num", DataType::Int32, false)]);

    // create a data stream with data from 0 to 40
    let v0 = (0..20).collect::<Vec<i32>>();
    let v1 = (20..40).collect::<Vec<i32>>();
    let block0 = DataBlock::create_by_array(schema.clone(), vec![Series::new(v0)]);
    let block1 = DataBlock::create_by_array(schema.clone(), vec![Series::new(v1)]);
    let stream = DataBlockStream::create(schema, None, vec![block0, block1]);

    // skip the number from 0 to 24 and the return block should be in range [25, 40]
    let mut skip_stream = SkipStream::new(Box::pin(stream), 25);

    let expected = vec![
        "+-----+", "| num |", "+-----+", "| 25  |", "| 26  |", "| 27  |", "| 28  |", "| 29  |",
        "| 30  |", "| 31  |", "| 32  |", "| 33  |", "| 34  |", "| 35  |", "| 36  |", "| 37  |",
        "| 38  |", "| 39  |", "+-----+",
    ];

    let mut index = 0;
    while let Some(res) = skip_stream.next().await {
        assert!(res.is_ok());
        let data_bock = res.unwrap();
        match index {
            0 => assert_blocks_eq(expected.clone(), &[data_bock]),
            _ => assert!(false),
        }
        index += 1;
    }
}
