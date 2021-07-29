// Copyright 2020-2021 The Datafuse Authors.
//
// SPDX-License-Identifier: Apache-2.0.

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_infallible::Mutex;
use common_planners::*;
use common_runtime::tokio;
use futures::TryStreamExt;

use crate::datasources::local::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_null_table() -> Result<()> {
    let ctx = crate::tests::try_create_context()?;
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", DataType::UInt64, false),
        DataField::new("b", DataType::UInt64, false),
    ]);
    let table = NullTable::try_create(
        "default".into(),
        "a".into(),
        DataSchemaRefExt::create(vec![DataField::new("a", DataType::UInt64, false)]).into(),
        TableOptions::default(),
    )?;

    // append data.
    {
        let block = DataBlock::create_by_array(schema.clone(), vec![
            Series::new(vec![1u64, 2]),
            Series::new(vec![11u64, 22]),
        ]);
        let blocks = vec![block];

        let input_stream = futures::stream::iter::<Vec<DataBlock>>(blocks.clone());
        let insert_plan = InsertIntoPlan {
            db_name: "default".to_string(),
            tbl_name: "a".to_string(),
            schema: schema.clone(),
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
        assert_eq!(table.engine(), "Null");

        let stream = table.read(ctx.clone(), &source_plan).await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        let block = &result[0];
        assert_eq!(block.num_columns(), 1);
    }

    // truncate.
    {
        let truncate_plan = TruncateTablePlan {
            db: "default".to_string(),
            table: "a".to_string(),
        };
        table.truncate(ctx.clone(), truncate_plan).await?;
    }

    Ok(())
}
