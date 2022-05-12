// Copyright 2021 Datafuse Labs.
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

use std::sync::Arc;

use common_base::base::tokio;
use common_datablocks::assert_blocks_sorted_eq;
use common_datablocks::DataBlock;
use common_datavalues::prelude::Series;
use common_datavalues::prelude::SeriesFrom;
use common_datavalues::DataField;
use common_datavalues::DataSchema;
use common_datavalues::ToDataType;
use common_exception::Result;
use common_meta_types::UserIdentity;
use common_planners::ReadDataSourcePlan;
use common_planners::SourceInfo;
use databend_query::storages::result::ResultQueryInfo;
use databend_query::storages::result::ResultTable;
use databend_query::storages::result::ResultTableWriter;
use databend_query::storages::Table;
use futures::TryStreamExt;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_result_table() -> Result<()> {
    let ctx = crate::tests::create_query_context().await?;
    ctx.get_settings().set_max_threads(2)?;

    let schema = Arc::new(DataSchema::new(vec![DataField::new(
        "number",
        u32::to_data_type(),
    )]));

    let query_id = "query_1";

    // Insert.
    {
        let query_info = ResultQueryInfo {
            query_id: query_id.to_string(),
            schema: schema.clone(),
            user: UserIdentity {
                username: "u1".to_string(),
                hostname: "h1".to_string(),
            },
        };
        let mut writer = ResultTableWriter::new(ctx.clone(), query_info).await?;

        let block = DataBlock::create(schema.clone(), vec![Series::from_data(vec![1u32])]);
        let block2 = DataBlock::create(schema.clone(), vec![Series::from_data(vec![2u32])]);
        let block3 = DataBlock::create(schema.clone(), vec![Series::from_data(vec![3u32])]);
        let blocks = vec![Ok(block), Ok(block2), Ok(block3)];
        let input_stream = futures::stream::iter::<Vec<Result<DataBlock>>>(blocks.clone());
        writer.write_stream(Box::pin(input_stream)).await?;
    }

    // read
    {
        let table: Arc<dyn Table> = ResultTable::try_get(ctx.clone(), query_id).await?;

        let (stats, parts) = table.read_partitions(ctx.clone(), None).await?;
        assert_eq!(stats.read_rows, 3);
        ctx.try_set_partitions(parts)?;
        let stream = table
            .read(ctx.clone(), &ReadDataSourcePlan {
                catalog: "".to_string(),
                source_info: SourceInfo::TableSource(Default::default()),
                scan_fields: None,
                parts: Default::default(),
                statistics: Default::default(),
                description: "".to_string(),
                tbl_args: None,
                push_downs: None,
            })
            .await?;

        let result = stream.try_collect::<Vec<_>>().await?;
        assert_blocks_sorted_eq(
            vec![
                "+--------+",
                "| number |",
                "+--------+",
                "| 1      |",
                "| 2      |",
                "| 3      |",
                "+--------+",
            ],
            &result,
        );
    }

    Ok(())
}
