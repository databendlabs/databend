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

use common_base::tokio;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;
use common_planners::*;
use databend_query::storages::null::NullTable;
use databend_query::storages::ToReadDataSourcePlan;
use futures::TryStreamExt;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_null_table() -> Result<()> {
    let ctx = crate::tests::create_query_context()?;
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", u64::to_data_type()),
        DataField::new("b", u64::to_data_type()),
    ]);
    let table = NullTable::try_create(crate::tests::create_storage_context()?, TableInfo {
        desc: "'default'.'a'".into(),
        name: "a".into(),
        ident: Default::default(),

        meta: TableMeta {
            schema: DataSchemaRefExt::create(vec![DataField::new("a", u64::to_data_type())]),
            engine: "Null".to_string(),
            options: TableOptions::default(),
            ..Default::default()
        },
    })?;

    // append data.
    {
        let block = DataBlock::create(schema.clone(), vec![
            Series::from_data(vec![1u64, 2]),
            Series::from_data(vec![11u64, 22]),
        ]);

        let blocks = vec![Ok(block)];

        let input_stream = futures::stream::iter::<Vec<Result<DataBlock>>>(blocks.clone());
        table
            .append_data(ctx.clone(), Box::pin(input_stream))
            .await
            .unwrap();
    }

    // read.
    {
        let source_plan = table.read_plan(ctx.clone(), None).await?;
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
            purge: false,
        };
        table.truncate(ctx, truncate_plan).await?;
    }

    Ok(())
}
