// Copyright 2022 Datafuse Labs.
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

use common_base::tokio;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_io::prelude::*;
use databend_query::sql::statements::ValueSource;

use crate::tests::create_query_context;

#[tokio::test]
async fn test_parse_value_source() -> Result<()> {
    let ctx = create_query_context().await?;
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("name", Vu8::to_data_type()),
        DataField::new("age", u8::to_data_type()),
        DataField::new("country", Vu8::to_data_type()),
        DataField::new("born_time", TimestampType::arc(0, None)),
    ]);

    let parser = ValueSource::new(ctx, schema);
    let s = "('ABC', 30 , 'China', '1992-03-15 00:00:00'),
                    ('XYZ', 11 + 20 , 'Japen', '1991-03-15 00:00:00'), 
                    ('UVW', 32 , CONCAT('Amer', 'ican'), '1990-03-15 00:00:00'), 
                    (CONCAT('G', 'HJ'), 32 , 'American', '1990-03-15 00:00:00')"
        .to_string();
    let bytes = s.as_bytes();
    let cursor = Cursor::new(bytes);
    let mut reader = CpBufferReader::new(Box::new(BufferReader::new(cursor)));
    let block = parser.read(&mut reader).await?;

    common_datablocks::assert_blocks_sorted_eq(
        vec![
            "+------+-----+----------+-----------------+",
            "| name | age | country  | born_time       |",
            "+------+-----+----------+-----------------+",
            "| ABC  | 30  | China    | 700617600000000 |",
            "| GHJ  | 32  | American | 637459200000000 |",
            "| UVW  | 32  | American | 637459200000000 |",
            "| XYZ  | 31  | Japen    | 668995200000000 |",
            "+------+-----+----------+-----------------+",
        ],
        &[block],
    );
    Ok(())
}
