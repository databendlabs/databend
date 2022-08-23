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

use common_base::base::tokio;
use common_datablocks::assert_blocks_eq;
use common_exception::Result;
use common_io::prelude::FormatSettings;
use common_streams::NDJsonSourceBuilder;
use common_streams::Source;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_source_ndjson() -> Result<()> {
    use common_datavalues::prelude::*;

    let schema = DataSchemaRefExt::create(vec![
        DataField::new("x", bool::to_data_type()),
        DataField::new("a", i8::to_data_type()),
        DataField::new("b", Vu8::to_data_type()),
        DataField::new("c", f64::to_data_type()),
        DataField::new("d", ArrayType::new_impl(i64::to_data_type())),
        DataField::new(
            "e",
            StructType::new_impl(None, vec![i64::to_data_type(), Vu8::to_data_type()]),
        ),
        DataField::new("f", VariantValue::to_data_type()),
    ]);

    let bytes = r#"{"x":false, "a":1, "b":"1", "c":1.0, "d":[1,2], "e":{"a":1,"b":"x"}, "f":"abc"}
    {"x":true, "a":2, "b":"2", "c":2.0, "d":[3,4], "e":{"a":2,"b":"y"}, "f":[1,2,3]}
    {"x":false, "a":3, "b":"3", "c":3.0, "d":[5,6], "e":{"a":3,"b":"z"}, "f":{"k":"v"}}
    "#
    .as_bytes();

    let reader = futures::io::Cursor::new(bytes);
    let builder = NDJsonSourceBuilder::create(schema, FormatSettings::default());
    let mut json_source = builder.build(reader).unwrap();
    // expects `page_nums_expects` blocks, and
    while let Some(block) = json_source.read().await? {
        // for each block, the content is the same of `sample_block`
        assert_blocks_eq(
            vec![
                "+-------+---+---+---+--------+--------+-----------+",
                "| x     | a | b | c | d      | e      | f         |",
                "+-------+---+---+---+--------+--------+-----------+",
                "| false | 1 | 1 | 1 | [1, 2] | (1, x) | \"abc\"     |",
                "| true  | 2 | 2 | 2 | [3, 4] | (2, y) | [1,2,3]   |",
                "| false | 3 | 3 | 3 | [5, 6] | (3, z) | {\"k\":\"v\"} |",
                "+-------+---+---+---+--------+--------+-----------+",
            ],
            &[block],
        );
    }

    Ok(())
}
