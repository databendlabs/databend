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
use common_datablocks::assert_blocks_sorted_eq;
use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;
use common_planners::*;
use databend_query::storages::memory::MemoryTable;
use databend_query::storages::ToReadDataSourcePlan;
use futures::TryStreamExt;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_memorytable() -> Result<()> {
    let ctx = crate::tests::create_query_context()?;
    let schema = DataSchemaRefExt::create(vec![
        DataField::new("a", u32::to_data_type()),
        DataField::new("b", u64::to_data_type()),
    ]);
    let table = MemoryTable::try_create(crate::tests::create_storage_context()?, TableInfo {
        desc: "'default'.'a'".into(),
        name: "a".into(),
        ident: Default::default(),
        meta: TableMeta {
            schema: schema.clone(),
            engine: "Memory".to_string(),
            options: TableOptions::default(),
            ..Default::default()
        },
    })?;

    // append data.
    {
        let block = DataBlock::create(schema.clone(), vec![
            Series::from_data(vec![1u32, 2]),
            Series::from_data(vec![11u64, 22]),
        ]);
        let block2 = DataBlock::create(schema.clone(), vec![
            Series::from_data(vec![4u32, 3]),
            Series::from_data(vec![33u64, 33]),
        ]);
        let blocks = vec![Ok(block), Ok(block2)];

        let input_stream = futures::stream::iter::<Vec<Result<DataBlock>>>(blocks.clone());
        let r = table
            .append_data(ctx.clone(), Box::pin(input_stream))
            .await
            .unwrap();
        // with overwrite false
        table
            .commit_insertion(ctx.clone(), r.try_collect().await?, false)
            .await?;
    }

    // read tests
    {
        let push_downs_vec: Vec<Option<Extras>> =
            vec![Some(vec![0usize]), Some(vec![1usize]), None]
                .into_iter()
                .map(|x| {
                    x.map(|x| Extras {
                        projection: Some(x),
                        filters: vec![],
                        limit: None,
                        order_by: vec![],
                    })
                })
                .collect();
        let expected_datablocks_vec = vec![
            vec![
                "+---+", //
                "| a |", //
                "+---+", //
                "| 1 |", //
                "| 2 |", //
                "| 3 |", //
                "| 4 |", //
                "+---+", //
            ],
            vec![
                "+----+", //
                "| b  |", //
                "+----+", //
                "| 11 |", //
                "| 22 |", //
                "| 33 |", //
                "| 33 |", //
                "+----+", //
            ],
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
        ];
        let expected_statistics_vec = vec![
            Statistics::new_estimated(4usize, 16usize, 0, 0),
            Statistics::new_estimated(4usize, 32usize, 0, 0),
            Statistics::new_exact(4usize, 48usize, 2, 2),
        ];

        for i in 0..push_downs_vec.len() {
            let push_downs = push_downs_vec[i].as_ref().cloned();
            let expected_datablocks = expected_datablocks_vec[i].clone();
            let expected_statistics = expected_statistics_vec[i].clone();
            let source_plan = table.read_plan(ctx.clone(), push_downs).await?;
            ctx.try_set_partitions(source_plan.parts.clone())?;
            assert_eq!(table.engine(), "Memory");
            assert!(table.benefit_column_prune());

            let stream = table.read(ctx.clone(), &source_plan).await?;
            let result = stream.try_collect::<Vec<_>>().await?;
            assert_blocks_sorted_eq(expected_datablocks, &result);

            // statistics
            assert_eq!(expected_statistics, source_plan.statistics);
        }
    }

    // overwrite
    {
        let block = DataBlock::create(schema.clone(), vec![
            Series::from_data(vec![5u64, 6]),
            Series::from_data(vec![55u64, 66]),
        ]);
        let block2 = DataBlock::create(schema.clone(), vec![
            Series::from_data(vec![7u64, 8]),
            Series::from_data(vec![77u64, 88]),
        ]);
        let blocks = vec![Ok(block), Ok(block2)];

        let input_stream = futures::stream::iter::<Vec<Result<DataBlock>>>(blocks.clone());
        let r = table
            .append_data(ctx.clone(), Box::pin(input_stream))
            .await
            .unwrap();
        // with overwrite = true
        table
            .commit_insertion(ctx.clone(), r.try_collect().await?, true)
            .await?;
    }

    // read overwrite
    {
        let source_plan = table.read_plan(ctx.clone(), None).await?;
        ctx.try_set_partitions(source_plan.parts.clone())?;
        assert_eq!(table.engine(), "Memory");

        let stream = table.read(ctx.clone(), &source_plan).await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        assert_blocks_sorted_eq(
            vec![
                "+---+----+",
                "| a | b  |",
                "+---+----+",
                "| 5 | 55 |",
                "| 6 | 66 |",
                "| 7 | 77 |",
                "| 8 | 88 |",
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
            purge: false,
        };
        table.truncate(ctx.clone(), truncate_plan).await?;

        let source_plan = table.read_plan(ctx.clone(), None).await?;
        let stream = table.read(ctx, &source_plan).await?;
        let result = stream.try_collect::<Vec<_>>().await?;
        assert_blocks_sorted_eq(vec!["++", "++"], &result);
    }

    Ok(())
}
