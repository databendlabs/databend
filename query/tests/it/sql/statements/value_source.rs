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

use common_base::tokio;
use common_datavalues::prelude::*;
use common_exception::Result;
use databend_query::sql::statements::ValueSource;

#[tokio::test]
async fn test_parse_value_source() -> Result<()> {
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("name", Vu8::to_data_type()),
        DataField::new("age", u8::to_data_type()),
        DataField::new("country", Vu8::to_data_type()),
        DataField::new("born_time", DateTime32Type::arc(None)),
    ]);

    let parser = ValueSource::new(schema);
    let s = "VALUES ('ABC', 30 , 'China', '1992-03-15 00:00:00'), ('XYZ', 31 , 'Japen', '1991-03-15 00:00:00'), ('UVW', 32 , 'American', '1990-03-15 00:00:00'), ('UVW', 32 , 'American', '1990-03-15 00:00:00')".to_string();
    let block = parser.stream_read(s)?;

    common_datablocks::assert_blocks_sorted_eq(
        vec![
            "+------+-----+----------+-----------+",
            "| name | age | country  | born_time |",
            "+------+-----+----------+-----------+",
            "| ABC  | 30  | China    | 700617600 |",
            "| XYZ  | 31  | Japen    | 668995200 |",
            "| UVW  | 32  | American | 637459200 |",
            "| UVW  | 32  | American | 637459200 |",
            "+------+-----+----------+-----------+",
        ],
        &[block],
    );
    Ok(())
}
