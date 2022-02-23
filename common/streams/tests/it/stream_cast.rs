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
use common_functions::scalars::CastFunction;
use common_streams::*;
use futures::stream::StreamExt;

#[tokio::test]
async fn test_cast_stream() {
    let input_schema = DataSchemaRefExt::create(vec![
        DataField::new("a", Vu8::to_data_type()),
        DataField::new("b", Vu8::to_data_type()),
    ]);

    let output_schema = DataSchemaRefExt::create(vec![
        DataField::new("c", u8::to_data_type()),
        DataField::new("d", u16::to_data_type()),
    ]);

    // create a data block [(1, 10), (2, 20), (3, 30), (4, 40), (5, 50)]
    let a_array = (1..6)
        .map(|n| n.to_string().into_bytes())
        .collect::<Vec<Vec<u8>>>();

    let b_array = (1..6)
        .map(|n| (n * 10).to_string().into_bytes())
        .collect::<Vec<Vec<u8>>>();

    let block0 = DataBlock::create(input_schema.clone(), vec![
        Series::from_data(a_array),
        Series::from_data(b_array),
    ]);

    // create a data block [(6, 60), (7, 70), (8, 80), (9, 90)]
    let a_array = (6..10)
        .map(|n| n.to_string().into_bytes())
        .collect::<Vec<Vec<u8>>>();

    let b_array = (6..10)
        .map(|n| (n * 10).to_string().into_bytes())
        .collect::<Vec<Vec<u8>>>();

    let block1 = DataBlock::create(input_schema.clone(), vec![
        Series::from_data(a_array),
        Series::from_data(b_array),
    ]);

    let stream = DataBlockStream::create(input_schema.clone(), None, vec![block0, block1]);

    // cast from String to UInt8 and String to UInt16
    let to_uint8 = CastFunction::create("cast", "UInt8").unwrap();
    let to_uint16 = CastFunction::create("cast", "UInt16").unwrap();

    let functions = vec![to_uint8, to_uint16];
    let mut cast_stream =
        CastStream::try_create(Box::pin(stream), output_schema.clone(), functions).unwrap();

    let expected = vec![
        vec![
            "+---+----+",
            "| c | d  |",
            "+---+----+",
            "| 1 | 10 |",
            "| 2 | 20 |",
            "| 3 | 30 |",
            "| 4 | 40 |",
            "| 5 | 50 |",
            "+---+----+",
        ],
        vec![
            "+---+----+",
            "| c | d  |",
            "+---+----+",
            "| 6 | 60 |",
            "| 7 | 70 |",
            "| 8 | 80 |",
            "| 9 | 90 |",
            "+---+----+",
        ],
    ];

    let mut index = 0_usize;

    while let Some(res) = cast_stream.next().await {
        assert!(res.is_ok());
        let data_block = res.unwrap();
        assert_eq!(data_block.schema().clone(), output_schema);
        match index {
            0 | 1 => assert_blocks_eq(expected[index].clone(), &[data_block]),
            _ => panic!(),
        }
        index += 1;
    }
}
