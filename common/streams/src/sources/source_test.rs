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

use common_datablocks::assert_blocks_eq;
use common_datavalues::DataField;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataType;

use crate::CsvSource;
use crate::Source;
use crate::ValueSource;

#[test]
fn test_parse_values() {
    let buffer =
        "(1,  'str',   1) , (-1, ' str ' ,  1.1) , ( 2,  'aa aa', 2.2),  (3, \"33'33\", 3.3)   ";

    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", DataType::Int8, false),
        DataField::new("b", DataType::Utf8, false),
        DataField::new("c", DataType::Float64, false),
    ]);
    let mut values_source = ValueSource::new(buffer.as_bytes(), schema, 10);
    let block = values_source.read().unwrap().unwrap();
    assert_blocks_eq(
        vec![
            "+----+-------+-----+",
            "| a  | b     | c   |",
            "+----+-------+-----+",
            "| 1  | str   | 1   |",
            "| -1 |  str  | 1.1 |",
            "| 2  | aa aa | 2.2 |",
            "| 3  | 33'33 | 3.3 |",
            "+----+-------+-----+",
        ],
        &[block],
    );

    let block = values_source.read().unwrap();
    assert!(block.is_none());
}

#[test]
fn test_parse_csvs() {
    let buffer = "1,\"1\",1.11\n2,\"2\",2\n3,\"3-'3'-3\",3\n";

    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", DataType::Int8, false),
        DataField::new("b", DataType::Utf8, false),
        DataField::new("c", DataType::Float64, false),
    ]);
    let mut values_source = CsvSource::new(buffer.as_bytes(), schema, 10);
    let block = values_source.read().unwrap().unwrap();
    assert_blocks_eq(
        vec![
            "+---+---------+------+",
            "| a | b       | c    |",
            "+---+---------+------+",
            "| 1 | 1       | 1.11 |",
            "| 2 | 2       | 2    |",
            "| 3 | 3-'3'-3 | 3    |",
            "+---+---------+------+",
        ],
        &[block],
    );

    let block = values_source.read().unwrap();
    assert!(block.is_none());
}
