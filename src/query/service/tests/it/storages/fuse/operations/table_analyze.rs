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
use std::sync::Arc;

use common_base::base::tokio;
use common_catalog::table::Table;
use common_exception::Result;
use common_expression::types::number::NumberScalar;
use common_expression::Scalar;
use common_storages_fuse::io::MetaReaders;
use common_storages_fuse::statistics::reducers::merge_statistics_mut;
use common_storages_fuse::FuseTable;
use databend_query::sessions::QueryContext;
use databend_query::sessions::TableContext;
use databend_query::sql::plans::Plan;
use databend_query::sql::Planner;
use databend_query::test_kits::table_test_fixture::analyze_table;
use databend_query::test_kits::table_test_fixture::do_deletion;
use databend_query::test_kits::table_test_fixture::do_update;
use databend_query::test_kits::table_test_fixture::execute_command;
use databend_query::test_kits::table_test_fixture::execute_query;
use databend_query::test_kits::table_test_fixture::TestFixture;
use databend_query::test_kits::utils::query_count;
use storages_common_cache::LoadParams;
use storages_common_table_meta::meta::SegmentInfo;
use storages_common_table_meta::meta::Statistics;

#[tokio::test(flavor = "multi_thread")]
async fn test_table_modify_column_ndv_statistics() -> Result<()> {
    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();

    // setup
    let create_tbl_command = "create table t(c int)";
    execute_command(ctx.clone(), create_tbl_command).await?;

    let catalog = ctx.get_catalog("default").await?;

    let num_inserts = 3;
    append_rows(ctx.clone(), num_inserts).await?;
    let statistics_sql = "analyze table default.t";
    execute_command(ctx.clone(), statistics_sql).await?;

    let table = catalog
        .get_table(ctx.get_tenant().as_str(), "default", "t")
        .await?;

    // check count
    let count_qry = "select count(*) from t";
    let stream = execute_query(fixture.ctx(), count_qry).await?;
    assert_eq!(num_inserts, query_count(stream).await? as usize);

    let expected = HashMap::from([(0, num_inserts as u64)]);
    check_column_ndv_statistics(table.clone(), expected.clone()).await?;

    // append the same values again, and ndv does changed.
    append_rows(ctx.clone(), num_inserts).await?;
    execute_command(ctx.clone(), statistics_sql).await?;

    // check count
    let count_qry = "select count(*) from t";
    let stream = execute_query(fixture.ctx(), count_qry).await?;
    assert_eq!(num_inserts * 2, query_count(stream).await? as usize);

    check_column_ndv_statistics(table.clone(), expected.clone()).await?;

    // delete
    let query = "delete from default.t where c=1";
    let mut planner = Planner::new(ctx.clone());
    let (plan, _) = planner.plan_sql(query).await?;
    if let Plan::Delete(delete) = plan {
        do_deletion(ctx.clone(), *delete).await?;
    }
    execute_command(ctx.clone(), statistics_sql).await?;

    // check count: delete not affect counts
    check_column_ndv_statistics(table.clone(), expected).await?;

    Ok(())
}

#[tokio::test(flavor = "multi_thread")]
async fn test_table_update_analyze_statistics() -> Result<()> {
    let fixture = TestFixture::new().await;
    let ctx = fixture.ctx();

    // create table
    fixture.create_default_table().await?;
    let db_name = fixture.default_db_name();
    let tb_name = fixture.default_table_name();

    // insert
    for i in 0..3 {
        let qry = format!("insert into {}.{}(id) values({})", db_name, tb_name, i);
        execute_command(ctx.clone(), &qry).await?;
    }

    // update
    let query = format!("update {}.{} set id = 3 where id = 0", db_name, tb_name);
    let mut planner = Planner::new(ctx.clone());
    let (plan, _) = planner.plan_sql(&query).await?;
    if let Plan::Update(update) = plan {
        let table = fixture.latest_default_table().await?;
        do_update(ctx.clone(), table, *update).await?;
    }

    // check summary after update
    let table = fixture.latest_default_table().await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let after_update = fuse_table.read_table_snapshot().await?.unwrap();
    let base_summary = after_update.summary.clone();
    let id_stats = base_summary.col_stats.get(&0).unwrap();
    assert_eq!(id_stats.max(), &Scalar::Number(NumberScalar::Int32(3)));
    assert_eq!(id_stats.min(), &Scalar::Number(NumberScalar::Int32(0)));

    // get segments summary
    let mut segment_summary = Statistics::default();
    let segment_reader = MetaReaders::segment_info_reader(
        ctx.get_data_operator()?.operator(),
        TestFixture::default_table_schema(),
    );
    for segment in after_update.segments.iter() {
        let param = LoadParams {
            location: segment.0.clone(),
            len_hint: None,
            ver: segment.1,
            put_cache: false,
        };
        let compact_segment = segment_reader.read(&param).await?;
        let segment_info = SegmentInfo::try_from(compact_segment.as_ref())?;
        merge_statistics_mut(
            &mut segment_summary,
            &segment_info.summary,
            fuse_table.cluster_key_id(),
        );
    }

    // analyze
    analyze_table(&fixture).await?;

    // check summary after analyze
    let table = fixture.latest_default_table().await?;
    let fuse_table = FuseTable::try_from_table(table.as_ref())?;
    let after_analyze = fuse_table.read_table_snapshot().await?.unwrap();
    let last_summary = after_analyze.summary.clone();
    let id_stats = last_summary.col_stats.get(&0).unwrap();
    assert_eq!(id_stats.max(), &Scalar::Number(NumberScalar::Int32(3)));
    assert_eq!(id_stats.min(), &Scalar::Number(NumberScalar::Int32(1)));

    assert_eq!(segment_summary, last_summary);

    Ok(())
}

async fn check_column_ndv_statistics(
    table: Arc<dyn Table>,
    expected: HashMap<u32, u64>,
) -> Result<()> {
    let provider = table.column_statistics_provider().await?;

    for (i, num) in expected.iter() {
        let stat = provider.column_statistics(*i);
        assert!(stat.is_some());

        assert_eq!(stat.unwrap().number_of_distinct_values, *num);
    }

    Ok(())
}

async fn append_rows(ctx: Arc<QueryContext>, n: usize) -> Result<()> {
    for i in 0..n {
        let qry = format!("insert into t values({})", i);
        execute_command(ctx.clone(), &qry).await?;
    }
    Ok(())
}
