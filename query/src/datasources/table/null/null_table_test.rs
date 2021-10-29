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

use common_base::tokio;
use common_context::IOContext;
use common_context::TableDataContext;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_infallible::Mutex;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;
use common_planners::*;
use futures::TryStreamExt;

use crate::catalogs::ToReadDataSourcePlan;
use crate::datasources::table::null::null_table::NullTable;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_null_table() -> Result<()> {
    let ctx = crate::tests::try_create_context()?;
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", DataType::UInt64, false),
        DataField::new("b", DataType::UInt64, false),
    ]);
    let table = NullTable::try_create(
        TableInfo {
            desc: "'default'.'a'".into(),
            name: "a".into(),
            ident: Default::default(),

            meta: TableMeta {
                schema: DataSchemaRefExt::create(vec![DataField::new(
                    "a",
                    DataType::UInt64,
                    false,
                )]),
                engine: "Null".to_string(),
                options: TableOptions::default(),
            },
        },
        Arc::new(TableDataContext::default()),
    )?;

    let io_ctx = ctx.get_single_node_table_io_context()?;
    let io_ctx = Arc::new(io_ctx);

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
            tbl_id: 0,
            schema: schema.clone(),
            input_stream: Arc::new(Mutex::new(Some(Box::pin(input_stream)))),
        };
        table
            .append_data(io_ctx.clone(), insert_plan)
            .await
            .unwrap();
    }

    // read.
    {
        let source_plan = table.read_plan(
            io_ctx.clone(),
            None,
            Some(io_ctx.get_max_threads() as usize),
        )?;
        assert_eq!(table.engine(), "Null");

        let stream = table.read(io_ctx.clone(), &source_plan).await?;
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
        table.truncate(io_ctx, truncate_plan).await?;
    }

    Ok(())
}
