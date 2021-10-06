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
//

use std::env;

use common_base::tokio;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_meta_api_vo::TableInfo;
use common_planners::*;
use futures::TryStreamExt;

use crate::datasources::table::parquet::parquet_table::ParquetTable;

#[tokio::test]
async fn test_parquet_table() -> Result<()> {
    let options: TableOptions = [(
        "location".to_string(),
        env::current_dir()?
            .join("../tests/data/alltypes_plain.parquet")
            .display()
            .to_string(),
    )]
    .iter()
    .cloned()
    .collect();

    let ctx = crate::tests::try_create_context()?;
    let tbl_info = TableInfo {
        db: "default".to_string(),
        table_id: 0,
        name: "test_parquet".to_string(),
        schema: DataSchemaRefExt::create(vec![DataField::new("id", DataType::Int32, false)]),
        engine: "test_parquet".into(),
        options: options,
    };
    let table = ParquetTable::try_create(tbl_info)?;

    let source_plan = table.read_plan(
        ctx.clone(),
        None,
        Some(ctx.get_settings().get_max_threads()? as usize),
    )?;

    let stream = table.read(ctx, &source_plan).await?;
    let blocks = stream.try_collect::<Vec<_>>().await?;
    let rows: usize = blocks.iter().map(|block| block.num_rows()).sum();

    assert_eq!(rows, 8);
    Ok(())
}
