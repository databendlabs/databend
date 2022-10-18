//  Copyright 2022 Datafuse Labs.
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

use std::collections::BTreeMap;
use std::collections::HashMap;
use std::sync::Arc;

use common_base::base::tokio;
use common_catalog::table::Table;
use common_catalog::table_mutator::TableMutator;
use common_datablocks::pretty_format_blocks;
use common_datablocks::DataBlock;
use common_datavalues::DataSchema;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataValue;
use common_exception::ErrorCode;
use common_exception::Result;
use common_fuse_meta::caches::CacheManager;
use common_fuse_meta::meta::BlockMeta;
use common_fuse_meta::meta::ClusterStatistics;
use common_fuse_meta::meta::SegmentInfo;
use common_fuse_meta::meta::Statistics;
use common_fuse_meta::meta::TableSnapshot;
use common_fuse_meta::meta::Versioned;
use common_storages_fuse::FuseTable;
use common_streams::SendableDataBlockStream;
use databend_query::sessions::QueryContext;
use databend_query::sessions::TableContext;
use databend_query::storages::fuse::io::SegmentWriter;
use databend_query::storages::fuse::io::TableMetaLocationGenerator;
use databend_query::storages::fuse::operations::CompactSegmentMutator;
use futures_util::TryStreamExt;
use uuid::Uuid;

use crate::storages::fuse::table_test_fixture::execute_command;
use crate::storages::fuse::table_test_fixture::execute_query;
use crate::storages::fuse::table_test_fixture::expects_ok;
use crate::storages::fuse::table_test_fixture::TestFixture;

#[tokio::test]
async fn test_compact_segment_normal_case() -> Result<()> {
    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();

    // setup
    let qry = format!("create table t(c int)  block_per_segment=10");
    execute_command(ctx.clone(), qry.as_str()).await?;

    let catalog = ctx.get_catalog("default")?;

    let num_inserts = 9;
    append_rows(ctx.clone(), num_inserts).await?;

    // check count
    let count_qry = "select count(*) from t";
    let stream = execute_query(fixture.ctx(), count_qry).await?;
    assert_eq!(9, check_count(stream).await?);

    // compact segment
    let table = catalog
        .get_table(ctx.get_tenant().as_str(), "default", "t")
        .await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let segment_only = true;
    let mut pipeline = common_pipeline_core::Pipeline::create();
    let mutator = fuse_table
        .compact(ctx.clone(), segment_only, &mut pipeline)
        .await?;
    assert!(mutator.is_some());
    let mut mutator = mutator.unwrap();
    mutator.try_commit(table.get_table_info()).await?;

    // check segment count
    let qry = format!("select segment_count as count from fuse_snapshot('default', 't') limit 1",);
    let stream = execute_query(fixture.ctx(), qry.as_str()).await?;
    // after compact, in our case, there should be only 1 segment left
    assert_eq!(1, check_count(stream).await?);

    // check block count
    let qry = format!("select block_count as count from fuse_snapshot('default', 't') limit 1",);
    let stream = execute_query(fixture.ctx(), qry.as_str()).await?;
    assert_eq!(num_inserts as u64, check_count(stream).await?);
    Ok(())
}

#[tokio::test]
async fn test_compact_segment_resolvable_conflict() -> Result<()> {
    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();

    // setup
    let create_tbl_command = format!("create table t(c int)  block_per_segment=10");
    execute_command(ctx.clone(), create_tbl_command.as_str()).await?;

    let catalog = ctx.get_catalog("default")?;

    let num_inserts = 9;
    append_rows(ctx.clone(), num_inserts).await?;

    // check count
    let count_qry = "select count(*) from t";
    let stream = execute_query(fixture.ctx(), count_qry).await?;
    assert_eq!(9, check_count(stream).await?);

    // compact segment
    let table = catalog
        .get_table(ctx.get_tenant().as_str(), "default", "t")
        .await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let segment_only = true;
    let mut pipeline = common_pipeline_core::Pipeline::create();
    let mutator = fuse_table
        .compact(ctx.clone(), segment_only, &mut pipeline)
        .await?;
    assert!(mutator.is_some());
    let mut mutator = mutator.unwrap();

    // before commit compact segments, gives 9 append commits
    let num_inserts = 9;
    append_rows(ctx.clone(), num_inserts).await?;

    mutator.try_commit(table.get_table_info()).await?;

    // check segment count
    let count_seg =
        format!("select segment_count as count from fuse_snapshot('default', 't') limit 1",);
    let stream = execute_query(fixture.ctx(), count_seg.as_str()).await?;
    // after compact, in our case, there should be only 1 + num_inserts segments left
    // during compact retry, newly appended segments will NOT be compacted again
    assert_eq!(1 + num_inserts as u64, check_count(stream).await?);

    // check block count
    let count_block =
        format!("select block_count as count from fuse_snapshot('default', 't') limit 1",);
    let stream = execute_query(fixture.ctx(), count_block.as_str()).await?;
    assert_eq!(num_inserts as u64 * 2, check_count(stream).await?);
    Ok(())
}

#[tokio::test]
async fn test_compact_segment_unresolvable_conflict() -> Result<()> {
    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();

    // setup
    let create_tbl_command = format!("create table t(c int)  block_per_segment=10");
    execute_command(ctx.clone(), create_tbl_command.as_str()).await?;

    let catalog = ctx.get_catalog("default")?;

    let num_inserts = 9;
    append_rows(ctx.clone(), num_inserts).await?;

    // check count
    let count_qry = "select count(*) from t";
    let stream = execute_query(fixture.ctx(), count_qry).await?;
    assert_eq!(num_inserts as u64, check_count(stream).await?);

    // try compact segment
    let table = catalog
        .get_table(ctx.get_tenant().as_str(), "default", "t")
        .await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let segment_only = true;
    let mut pipeline = common_pipeline_core::Pipeline::create();
    let mutator = fuse_table
        .compact(ctx.clone(), segment_only, &mut pipeline)
        .await?;
    assert!(mutator.is_some());
    let mut mutator = mutator.unwrap();

    {
        // inject a unresolvable commit
        compact_segment(ctx.clone(), table.as_ref()).await?;
    }

    // the compact operation committed latter should failed
    let r = mutator.try_commit(table.get_table_info()).await;
    assert!(r.is_err());
    assert_eq!(r.err().unwrap().code(), ErrorCode::storage_other_code());

    Ok(())
}

async fn append_rows(ctx: Arc<QueryContext>, n: usize) -> Result<()> {
    let qry = format!("insert into t values(1)");
    for _ in 0..n {
        execute_command(ctx.clone(), qry.as_str()).await?;
    }
    Ok(())
}

async fn check_count(result_stream: SendableDataBlockStream) -> Result<u64> {
    let blocks: Vec<DataBlock> = result_stream.try_collect().await?;
    let formatted = pretty_format_blocks(&blocks).unwrap();
    blocks[0].column(0).get_u64(0)
}

async fn compact_segment(ctx: Arc<QueryContext>, table: &dyn Table) -> Result<()> {
    let fuse_table = FuseTable::try_from_table(table)?;
    let segment_only = true;
    let mut pipeline = common_pipeline_core::Pipeline::create();
    let mut mutator = fuse_table
        .compact(ctx, segment_only, &mut pipeline)
        .await?
        .unwrap();
    mutator.try_commit(table.get_table_info()).await
}
