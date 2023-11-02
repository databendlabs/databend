//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::collections::HashMap;

use common_base::base::tokio;
use common_catalog::plan::PushDownInfo;
use common_catalog::table_args::TableArgs;
use common_exception::Result;
use common_expression::Scalar;
use common_sql::executor::table_read_plan::ToReadDataSourcePlan;
use databend_query::sessions::TableContext;
use databend_query::stream::ReadDataBlockStream;
use databend_query::table_functions::FlattenTable;
use futures::TryStreamExt;
use jsonb::parse_value;
use pretty_assertions::assert_eq;

#[tokio::test(flavor = "multi_thread")]
async fn test_flatten_table() -> Result<()> {
    let tbl_args_cases = generate_table_args();
    for (tbl_args, expected) in tbl_args_cases {
        let (_guard, ctx) = databend_query::test_kits::create_query_context().await?;
        let table = FlattenTable::create("system", "flatten", 1, tbl_args)?;

        let source_plan = table
            .clone()
            .as_table()
            .read_plan(ctx.clone(), Some(PushDownInfo::default()), true)
            .await?;
        ctx.set_partitions(source_plan.parts.clone())?;

        let stream = table
            .as_table()
            .read_data_block_stream(ctx, &source_plan)
            .await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        let block = &result[0];
        assert_eq!(block.num_columns(), 6);

        common_expression::block_debug::assert_blocks_sorted_eq(expected, result.as_slice());
    }

    Ok(())
}

fn generate_table_args() -> Vec<(TableArgs, Vec<&'static str>)> {
    let mut named = HashMap::<String, Scalar>::new();
    let data1 = r#"{"a":1, "b":[77,88], "c": {"d":"X"}}"#;
    let value1 = parse_value(data1.as_bytes()).unwrap();
    let mut input1 = Vec::new();
    value1.write_to_vec(&mut input1);
    named.insert("input".to_string(), Scalar::Variant(input1));
    let tbl_args1 = TableArgs::new_named(named.clone());
    let expected1 = vec![
        "+----------+----------+----------+----------+-----------+-----------------------------------+",
        "| Column 0 | Column 1 | Column 2 | Column 3 | Column 4  | Column 5                          |",
        "+----------+----------+----------+----------+-----------+-----------------------------------+",
        "| 1        | 'a'      | 'a'      | NULL     | 1         | {\"a\":1,\"b\":[77,88],\"c\":{\"d\":\"X\"}} |",
        "| 1        | 'b'      | 'b'      | NULL     | [77,88]   | {\"a\":1,\"b\":[77,88],\"c\":{\"d\":\"X\"}} |",
        "| 1        | 'c'      | 'c'      | NULL     | {\"d\":\"X\"} | {\"a\":1,\"b\":[77,88],\"c\":{\"d\":\"X\"}} |",
        "+----------+----------+----------+----------+-----------+-----------------------------------+",
    ];

    // test path
    named.insert("path".to_string(), Scalar::String("b".as_bytes().to_vec()));
    let tbl_args2 = TableArgs::new_named(named.clone());
    let expected2 = vec![
        "+----------+----------+----------+----------+----------+----------+",
        "| Column 0 | Column 1 | Column 2 | Column 3 | Column 4 | Column 5 |",
        "+----------+----------+----------+----------+----------+----------+",
        "| 1        | NULL     | 'b[0]'   | 0        | 77       | [77,88]  |",
        "| 1        | NULL     | 'b[1]'   | 1        | 88       | [77,88]  |",
        "+----------+----------+----------+----------+----------+----------+",
    ];

    // test recursive
    named.remove(&"path".to_string());
    named.insert("recursive".to_string(), Scalar::Boolean(true));
    let tbl_args3 = TableArgs::new_named(named.clone());
    let expected3 = vec![
        "+----------+----------+----------+----------+-----------+-----------------------------------+",
        "| Column 0 | Column 1 | Column 2 | Column 3 | Column 4  | Column 5                          |",
        "+----------+----------+----------+----------+-----------+-----------------------------------+",
        "| 1        | 'a'      | 'a'      | NULL     | 1         | {\"a\":1,\"b\":[77,88],\"c\":{\"d\":\"X\"}} |",
        "| 1        | 'b'      | 'b'      | NULL     | [77,88]   | {\"a\":1,\"b\":[77,88],\"c\":{\"d\":\"X\"}} |",
        "| 1        | NULL     | 'b[0]'   | 0        | 77        | [77,88]                           |",
        "| 1        | NULL     | 'b[1]'   | 1        | 88        | [77,88]                           |",
        "| 1        | 'c'      | 'c'      | NULL     | {\"d\":\"X\"} | {\"a\":1,\"b\":[77,88],\"c\":{\"d\":\"X\"}} |",
        "| 1        | 'd'      | 'c.d'    | NULL     | \"X\"       | {\"d\":\"X\"}                         |",
        "+----------+----------+----------+----------+-----------+-----------------------------------+",
    ];

    // test object mode
    named.insert(
        "mode".to_string(),
        Scalar::String("object".as_bytes().to_vec()),
    );
    let tbl_args4 = TableArgs::new_named(named.clone());
    let expected4 = vec![
        "+----------+----------+----------+----------+-----------+-----------------------------------+",
        "| Column 0 | Column 1 | Column 2 | Column 3 | Column 4  | Column 5                          |",
        "+----------+----------+----------+----------+-----------+-----------------------------------+",
        "| 1        | 'a'      | 'a'      | NULL     | 1         | {\"a\":1,\"b\":[77,88],\"c\":{\"d\":\"X\"}} |",
        "| 1        | 'b'      | 'b'      | NULL     | [77,88]   | {\"a\":1,\"b\":[77,88],\"c\":{\"d\":\"X\"}} |",
        "| 1        | 'c'      | 'c'      | NULL     | {\"d\":\"X\"} | {\"a\":1,\"b\":[77,88],\"c\":{\"d\":\"X\"}} |",
        "| 1        | 'd'      | 'c.d'    | NULL     | \"X\"       | {\"d\":\"X\"}                         |",
        "+----------+----------+----------+----------+-----------+-----------------------------------+",
    ];

    // test array mode
    named.insert(
        "mode".to_string(),
        Scalar::String("array".as_bytes().to_vec()),
    );
    let tbl_args5 = TableArgs::new_named(named.clone());
    let expected5 = vec![
        "+----------+----------+----------+----------+----------+----------+",
        "| Column 0 | Column 1 | Column 2 | Column 3 | Column 4 | Column 5 |",
        "+----------+----------+----------+----------+----------+----------+",
        "+----------+----------+----------+----------+----------+----------+",
    ];

    // test outer
    let data2 = r#"[]"#;
    let value2 = parse_value(data2.as_bytes()).unwrap();
    let mut input2 = Vec::new();
    value2.write_to_vec(&mut input2);
    named.remove(&"mode".to_string());
    named.insert("input".to_string(), Scalar::Variant(input2));
    named.insert("outer".to_string(), Scalar::Boolean(false));
    let tbl_args6 = TableArgs::new_named(named.clone());
    let expected6 = vec![
        "+----------+----------+----------+----------+----------+----------+",
        "| Column 0 | Column 1 | Column 2 | Column 3 | Column 4 | Column 5 |",
        "+----------+----------+----------+----------+----------+----------+",
        "+----------+----------+----------+----------+----------+----------+",
    ];

    named.insert("outer".to_string(), Scalar::Boolean(true));
    let tbl_args7 = TableArgs::new_named(named);
    let expected7 = vec![
        "+----------+----------+----------+----------+----------+----------+",
        "| Column 0 | Column 1 | Column 2 | Column 3 | Column 4 | Column 5 |",
        "+----------+----------+----------+----------+----------+----------+",
        "| 1        | NULL     | NULL     | NULL     | NULL     | NULL     |",
        "+----------+----------+----------+----------+----------+----------+",
    ];

    vec![
        (tbl_args1, expected1),
        (tbl_args2, expected2),
        (tbl_args3, expected3),
        (tbl_args4, expected4),
        (tbl_args5, expected5),
        (tbl_args6, expected6),
        (tbl_args7, expected7),
    ]
}
