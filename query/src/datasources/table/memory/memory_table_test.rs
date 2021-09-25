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

use std::sync::Arc;

use common_datablocks::assert_blocks_sorted_eq;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_infallible::Mutex;
use common_planners::*;
use common_base::tokio;
use futures::TryStreamExt;

use crate::datasources::table::memory::memory_table::MemoryTable;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_memorytable() -> Result<()> {
    let ctx = crate::tests::try_create_context()?;
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", DataType::UInt64, false),
        DataField::new("b", DataType::UInt64, false),
    ]);
    let table = MemoryTable::try_create(
        "default".into(),
        "a".into(),
        schema.clone(),
        TableOptions::default(),
    )?;

    // append data.
    {
        let block = DataBlock::create_by_array(schema.clone(), vec![
            Series::new(vec![1u64, 2]),
            Series::new(vec![11u64, 22]),
        ]);
        let block2 = DataBlock::create_by_array(schema.clone(), vec![
            Series::new(vec![4u64, 3]),
            Series::new(vec![33u64, 33]),
        ]);
        let blocks = vec![block, block2];

        let input_stream = futures::stream::iter::<Vec<DataBlock>>(blocks.clone());
        let insert_plan = InsertIntoPlan {
            db_name: "default".to_string(),
            tbl_name: "a".to_string(),
            tbl_id: 0,
            schema,
            input_stream: Arc::new(Mutex::new(Some(Box::pin(input_stream)))),
        };
        table.append_data(ctx.clone(), insert_plan).await.unwrap();
    }

    // read.
    {
        let source_plan = table.read_plan(
            ctx.clone(),
            &ScanPlan::empty(),
            ctx.get_settings().get_max_threads()? as usize,
        )?;
        ctx.try_set_partitions(source_plan.parts.clone())?;
        assert_eq!(table.engine(), "Memory");

        let stream = table.read(ctx.clone(), &source_plan).await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        assert_blocks_sorted_eq(
            vec![
                "+---+----+",
                "| a | b  |",
                "+---+----+",
                "| 1 | 11 |",
                "| 2 | 22 |",
                "| 3 | 33 |",
                "| 4 | 33 |",
                "+---+----+",
            ],
            &result,
        );
    }

    // truncate.
    {
        let truncate_plan = TruncateTablePlan {
            db: "default".to_string(),
            table: "a".to_string(),
        };
        table.truncate(ctx.clone(), truncate_plan).await?;

        let source_plan = table.read_plan(
            ctx.clone(),
            &ScanPlan::empty(),
            ctx.get_settings().get_max_threads()? as usize,
        )?;
        let stream = table.read(ctx, &source_plan).await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        assert_blocks_sorted_eq(vec!["++", "++"], &result);
    }

    Ok(())
}
