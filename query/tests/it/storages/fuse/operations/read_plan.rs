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

use std::collections::HashMap;
use std::iter::Iterator;

use common_base::tokio;
use common_datavalues::DataValue;
use common_exception::Result;
use common_planners::Extras;
use databend_query::interpreters::CreateTableInterpreter;
use databend_query::storages::fuse::meta::BlockMeta;
use databend_query::storages::fuse::meta::ColumnMeta;
use databend_query::storages::fuse::meta::Compression;
use databend_query::storages::fuse::FuseTable;
use databend_query::storages::index::ColumnStatistics;
use futures::TryStreamExt;

use crate::storages::fuse::table_test_fixture::TestFixture;

#[test]
fn test_to_partitions() -> Result<()> {
    // setup
    let num_of_col = 10;
    let num_of_block = 5;

    let col_stats_gen = |col_size| ColumnStatistics {
        min: DataValue::Int64(1),
        max: DataValue::Int64(2),
        null_count: 0,
        in_memory_size: col_size as u64,
    };

    let col_metas_gen = || ColumnMeta {
        offset: 0,
        len: 0,
        num_values: 0,
    };

    let cols_stats = (0..num_of_col)
        .into_iter()
        .map(|col_id| (col_id as u32, col_stats_gen(col_id)))
        .collect::<HashMap<_, _>>();

    let cols_metas = (0..num_of_col)
        .into_iter()
        .map(|col_id| (col_id as u32, col_metas_gen()))
        .collect::<HashMap<_, _>>();

    let block_meta = BlockMeta {
        row_count: 0,
        block_size: cols_stats
            .iter()
            .map(|(_, col_stats)| col_stats.in_memory_size)
            .sum(),
        file_size: 0,
        col_stats: cols_stats.clone(),
        col_metas: cols_metas,
        cluster_stats: HashMap::new(),
        location: ("".to_owned(), 0),
        compression: Compression::Lz4Raw,
    };

    let blocks_metas = (0..num_of_block)
        .into_iter()
        .map(|_| block_meta.clone())
        .collect::<Vec<_>>();

    // CASE I:  no projection
    let (s, _) = FuseTable::to_partitions(&blocks_metas, None);
    let expected_block_size: u64 = cols_stats
        .iter()
        .map(|(_, col_stats)| col_stats.in_memory_size)
        .sum();
    assert_eq!(expected_block_size * num_of_block, s.read_bytes as u64);

    // CASE II: col pruning
    // projection which keeps the odd ones
    let proj = (0..num_of_col)
        .into_iter()
        .filter(|v| v & 1 != 0)
        .collect::<Vec<usize>>();

    // for each block, the block size we expects (after pruning)
    let expected_block_size: u64 = cols_stats
        .iter()
        .filter(|(cid, _)| proj.contains(&(**cid as usize)))
        .map(|(_, col_stats)| col_stats.in_memory_size)
        .sum();

    // kick off
    let push_down = Some(Extras {
        projection: Some(proj),
        filters: vec![],
        limit: None,
        order_by: vec![],
    });
    let (stats, _) = FuseTable::to_partitions(&blocks_metas, push_down);
    assert_eq!(expected_block_size * num_of_block, stats.read_bytes as u64);
    Ok(())
}

#[tokio::test]
async fn test_fuse_table_exact_statistic() -> Result<()> {
    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();

    let create_table_plan = fixture.default_crate_table_plan();
    let interpreter = CreateTableInterpreter::try_create(ctx.clone(), create_table_plan)?;
    interpreter.execute(None).await?;

    let mut table = fixture.latest_default_table().await?;

    // basic append and read
    {
        let num_blocks = 2;
        let rows_per_block = 2;
        let value_start_from = 1;
        let stream =
            TestFixture::gen_sample_blocks_stream_ex(num_blocks, rows_per_block, value_start_from);

        let r = table.append_data(ctx.clone(), stream).await?;
        table
            .commit_insertion(ctx.clone(), r.try_collect().await?, false)
            .await?;

        table = fixture.latest_default_table().await?;

        let push_downs = Extras {
            projection: Some(vec![]),
            filters: vec![],
            limit: None,
            order_by: vec![],
        };
        let (stats, parts) = table.read_partitions(ctx.clone(), Some(push_downs)).await?;
        assert_eq!(stats.read_rows, num_blocks * rows_per_block);
        assert!(parts.is_empty());
    }

    Ok(())
}
