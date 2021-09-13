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

use std::env;

use common_datablocks::assert_blocks_sorted_eq;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_planners::*;
use common_runtime::tokio;
use futures::TryStreamExt;

use crate::datasources::database::local::CsvTable;

#[tokio::test]
async fn test_csv_table() -> Result<()> {
    let options: TableOptions = [(
        "location".to_string(),
        env::current_dir()?
            .join("../tests/data/sample.csv")
            .display()
            .to_string(),
    )]
    .iter()
    .cloned()
    .collect();

    let ctx = crate::tests::try_create_context()?;
    let table = CsvTable::try_create(
        "default".into(),
        "test_csv".into(),
        DataSchemaRefExt::create(vec![DataField::new("column1", DataType::UInt64, false)]),
        options,
    )?;

    let scan_plan = &ScanPlan {
        schema_name: "".to_string(),
        table_schema: DataSchemaRefExt::create(vec![]),
        table_id: 0,
        table_version: None,
        table_args: None,
        projected_schema: DataSchemaRefExt::create(vec![DataField::new(
            "column1",
            DataType::UInt64,
            false,
        )]),
        push_downs: Extras::default(),
    };
    let partitions = ctx.get_settings().get_max_threads()? as usize;
    let source_plan = table.read_plan(ctx.clone(), scan_plan, partitions)?;
    ctx.try_set_partitions(source_plan.parts.clone())?;

    let stream = table.read(ctx, &source_plan).await?;
    let result = stream.try_collect::<Vec<_>>().await?;
    let block = &result[0];
    assert_eq!(block.num_columns(), 1);

    let expected = vec![
        "+---------+",
        "| column1 |",
        "+---------+",
        "| 1       |",
        "| 2       |",
        "| 3       |",
        "| 4       |",
        "| 5       |",
        "| 6       |",
        "+---------+",
    ];
    common_datablocks::assert_blocks_sorted_eq(expected, result.as_slice());

    Ok(())
}

#[tokio::test]
async fn test_csv_table_parse_error() -> Result<()> {
    let options: TableOptions = [(
        "location".to_string(),
        env::current_dir()?
            .join("../tests/data/sample.csv")
            .display()
            .to_string(),
    )]
    .iter()
    .cloned()
    .collect();

    let ctx = crate::tests::try_create_context()?;
    let table = CsvTable::try_create(
        "default".into(),
        "test_csv".into(),
        DataSchemaRefExt::create(vec![
            DataField::new("column1", DataType::UInt64, false),
            DataField::new("column2", DataType::UInt64, false),
            DataField::new("column3", DataType::UInt64, false),
            DataField::new("column4", DataType::UInt64, false),
        ]),
        options,
    )?;
    let scan_plan = &ScanPlan {
        schema_name: "".to_string(),
        table_id: 0,
        table_version: None,
        table_schema: DataSchemaRefExt::create(vec![]),
        table_args: None,
        projected_schema: DataSchemaRefExt::create(vec![DataField::new(
            "column2",
            DataType::UInt64,
            false,
        )]),
        push_downs: Extras::default(),
    };
    let partitions = ctx.get_settings().get_max_threads()? as usize;
    let source_plan = table.read_plan(ctx.clone(), scan_plan, partitions)?;
    ctx.try_set_partitions(source_plan.parts.clone())?;

    let stream = table.read(ctx, &source_plan).await?;
    let result = stream.try_collect::<Vec<_>>().await;
    // integer parse error will result to Null value
    assert_eq!(false, result.is_err());
    assert_blocks_sorted_eq(
        vec![
            "+---------+---------+---------+---------+",
            "| column1 | column2 | column3 | column4 |",
            "+---------+---------+---------+---------+",
            "| 1       | NULL    | 100     | NULL    |",
            "| 2       | NULL    | 80      | NULL    |",
            "| 3       | NULL    | 60      | NULL    |",
            "| 4       | NULL    | 70      | NULL    |",
            "| 5       | NULL    | 55      | NULL    |",
            "| 6       | NULL    | 99      | NULL    |",
            "+---------+---------+---------+---------+",
        ],
        &result.unwrap(),
    );
    Ok(())
}
