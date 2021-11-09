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

use std::io::Cursor;

use chrono_tz::Tz;
use common_clickhouse_srv::binary::Encoder;
use common_clickhouse_srv::types::*;

#[test]
fn test_write_and_read() {
    let block = Block::<Simple>::new().column("vals", vec![vec![7_u32, 8], vec![9, 1, 2], vec![
        3, 4, 5, 6,
    ]]);

    let mut encoder = Encoder::new();
    block.write(&mut encoder, false);

    let mut reader = Cursor::new(encoder.get_buffer_ref());
    let rblock = Block::load(&mut reader, Tz::Zulu, false).unwrap();

    assert_eq!(block, rblock);
}
