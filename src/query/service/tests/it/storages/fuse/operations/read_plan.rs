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
use std::iter::Iterator;
use std::sync::Arc;

use chrono::Utc;
use common_arrow::arrow::datatypes::DataType as ArrowType;
use common_arrow::arrow::datatypes::Field as ArrowField;
use common_base::base::tokio;
use common_catalog::plan::Projection;
use common_catalog::plan::PushDownInfo;
use common_exception::Result;
use common_expression::ColumnId;
use common_expression::FieldIndex;
use common_expression::Scalar;
use common_storage::ColumnNode;
use common_storage::ColumnNodes;
use common_storages_fuse::FusePartInfo;
use databend_query::storages::fuse::FuseTable;
use databend_query::test_kits::table_test_fixture::TestFixture;
use futures::TryStreamExt;
use storages_common_table_meta::meta;
use storages_common_table_meta::meta::BlockMeta;
use storages_common_table_meta::meta::ColumnMeta;
use storages_common_table_meta::meta::ColumnStatistics;

#[test]
fn test_to_partitions() -> Result<()> {
    // setup
    let num_of_col = 10;
    let num_of_block = 5;

    let col_stats_gen = |col_size| {
        ColumnStatistics::new(
            Scalar::from(1i64),
            Scalar::from(2i64),
            0,
            col_size as u64,
            None,
        )
    };

    let col_metas_gen = |col_size| {
        ColumnMeta::Parquet(meta::SingleColumnMeta {
            offset: 0,
            len: col_size as u64,
            num_values: 0,
        })
    };

    let col_nodes_gen = |field_index| ColumnNode {
        field: ArrowField::new("".to_string(), ArrowType::Int64, false),
        is_nested: false,
        leaf_indices: vec![field_index],
        leaf_column_ids: vec![field_index as ColumnId],
        children: None,
    };

    // generates fake data.
    // for simplicity, we set `in_memory_size` and the `len` to the value of `col_id`
    let cols_stats = (0..num_of_col)
        .map(|field_index| (field_index as ColumnId, col_stats_gen(field_index)))
        .collect::<HashMap<_, _>>();

    let cols_metas = (0..num_of_col)
        .map(|field_index| (field_index as ColumnId, col_metas_gen(field_index)))
        .collect::<HashMap<_, _>>();

    let cluster_stats = None;
    let location = ("".to_owned(), 0);
    let block_size = cols_stats
        .values()
        .map(|col_stats| col_stats.in_memory_size)
        .sum();

    let bloom_filter_location = None;
    let bloom_filter_size = 0;
    let block_meta = Arc::new(BlockMeta::new(
        0,
        block_size,
        0,
        cols_stats,
        cols_metas.clone(),
        cluster_stats,
        location,
        bloom_filter_location,
        bloom_filter_size,
        meta::Compression::Lz4Raw,
        Some(Utc::now()),
    ));

    let blocks_metas = (0..num_of_block)
        .map(|_| (None, block_meta.clone()))
        .collect::<Vec<_>>();

    let column_nodes = (0..num_of_col).map(col_nodes_gen).collect::<Vec<_>>();

    let column_nodes = ColumnNodes { column_nodes };

    // CASE I:  no projection
    let (s, parts) = FuseTable::to_partitions(None, &blocks_metas, None, &column_nodes, None, None);
    assert_eq!(parts.len(), num_of_block as usize);
    let expected_block_size: u64 = cols_metas
        .values()
        .map(|col_meta| col_meta.offset_length().1)
        .sum();
    assert_eq!(expected_block_size * num_of_block, s.read_bytes as u64);

    // CASE II: col pruning
    // projection which keeps the odd ones
    let field_indices = (0..num_of_col)
        .filter(|v| v & 1 != 0)
        .collect::<Vec<usize>>();
    let proj = Projection::Columns(field_indices.clone());

    // for each block, the block size we expects (after pruning)
    let expected_block_size: u64 = cols_metas
        .iter()
        .filter(|(cid, _)| field_indices.contains(&(**cid as FieldIndex)))
        .map(|(_, col_meta)| col_meta.offset_length().1)
        .sum();

    // kick off
    let push_down = Some(PushDownInfo {
        projection: Some(proj),
        ..Default::default()
    });

    let (stats, parts) =
        FuseTable::to_partitions(None, &blocks_metas, None, &column_nodes, None, push_down);
    assert_eq!(parts.len(), num_of_block as usize);
    assert_eq!(expected_block_size * num_of_block, stats.read_bytes as u64);

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_fuse_table_exact_statistic() -> Result<()> {
    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();

    fixture.create_default_table().await?;

    let mut table = fixture.latest_default_table().await?;

    // basic append and read
    {
        let num_blocks = 2;
        let rows_per_block = 2;
        let value_start_from = 1;
        let stream =
            TestFixture::gen_sample_blocks_stream_ex(num_blocks, rows_per_block, value_start_from);

        let blocks = stream.try_collect().await?;
        fixture
            .append_commit_blocks(table.clone(), blocks, false, true)
            .await?;

        table = fixture.latest_default_table().await?;

        let proj = Projection::Columns(vec![]);
        let push_downs = PushDownInfo {
            projection: Some(proj),
            ..Default::default()
        };
        let (stats, parts) = table
            .read_partitions(ctx.clone(), Some(push_downs), true)
            .await?;
        assert_eq!(stats.read_rows, num_blocks * rows_per_block);
        assert!(!parts.is_empty());

        let part = parts.partitions[0].clone();
        let fuse_part = FusePartInfo::from_part(&part)?;
        assert!(fuse_part.create_on.is_some())
    }
    Ok(())
}
