// Copyright 2021 Datafuse Labs
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

use databend_common_base::base::tokio;
use databend_common_catalog::table_context::TableContextProgress;
use databend_common_catalog::table_context::TableContextSettings;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_query::test_kits::TestFixture;
use futures_util::TryStreamExt;

async fn run(
    fixture: &TestFixture,
    sql: &str,
    enabled: &str,
    threads: u64,
) -> Result<(Vec<Vec<Scalar>>, usize)> {
    let ctx = fixture.new_query_ctx().await?;
    ctx.get_settings().set_max_threads(threads)?;
    ctx.get_settings()
        .set_setting("enable_heavy_hitter_aggregate".into(), enabled.into())?;
    let blocks = TestFixture::execute_query_with_ctx(ctx.clone(), sql)
        .await?
        .try_collect::<Vec<DataBlock>>()
        .await?;
    let mut rows = vec![];
    for block in blocks {
        for row in 0..block.num_rows() {
            rows.push(
                block
                    .columns()
                    .iter()
                    .map(|column| column.index(row).unwrap().to_owned())
                    .collect(),
            );
        }
    }
    Ok((rows, ctx.get_scan_progress_value().rows))
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_adaptive_topk_count_scans_once_or_falls_back() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    let db = fixture.default_db_name();
    fixture
        .execute_command(&format!("create table {db}.hot(k bigint null)"))
        .await?;
    fixture.execute_command(&format!(
        "insert into {db}.hot select if(number % 4 = 0, null, if(number % 4 = 1, 0, number)) from numbers(100000)"
    )).await?;
    fixture
        .execute_command(&format!(
            "create table {db}.uniq(k bigint not null) row_per_block = 10000"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "insert into {db}.uniq select number from numbers(1000000)"
        ))
        .await?;

    for threads in [1, 4] {
        // Both hot groups have exact counts and tie. The outer TopN, not the
        // bounded proof, must apply the secondary key and OFFSET.
        for suffix in ["limit 1", "limit 1 offset 1", "limit 2"] {
            let sql = format!(
                "select k, count(*) c from {db}.hot group by k order by c desc, k {suffix}"
            );
            let baseline = run(&fixture, &sql, "0", threads).await?;
            let adaptive = run(&fixture, &sql, "2", threads).await?;
            assert_eq!(baseline.0, adaptive.0);
            assert_eq!(baseline.1, 100000);
            assert_eq!(
                adaptive.1, 100000,
                "proven counts must not start a second scan"
            );
        }
        let sql =
            format!("select k, count(*) c from {db}.uniq group by k order by c desc, k limit 3");
        let baseline = run(&fixture, &sql, "0", threads).await?;
        let adaptive = run(&fixture, &sql, "2", threads).await?;
        assert_eq!(baseline.0, adaptive.0);
        assert!(
            adaptive.1 > 1000000 && adaptive.1 <= 2000000,
            "failed proof must run a full exact fallback, read {}",
            adaptive.1
        );
        if threads == 1 {
            assert!(
                adaptive.1 < 2000000,
                "an impossible proof must stop the first pass early"
            );
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_adaptive_topk_count_validates_inexact_candidates() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    let db = fixture.default_db_name();
    fixture
        .execute_command(&format!(
            "create table {db}.late_hot(k bigint null) row_per_block = 1000000"
        ))
        .await?;
    // One stored block with a long unique prefix loses exact-count knowledge
    // before the winner first appears. NULL remains exactly known and avoids
    // a prefix-only cost rejection. The winner must be validated by a rescan.
    let writer = fixture.new_query_ctx().await?;
    writer.get_settings().set_max_threads(1)?;
    writer.get_settings().set_max_block_size(1000000)?;
    TestFixture::execute_query_with_ctx(
        writer,
        &format!(
            "insert into {db}.late_hot select if(number % 5 = 0, null, \
         if(number < 300000, number, 999999999)) from numbers(500000)"
        ),
    )
    .await?
    .try_collect::<Vec<DataBlock>>()
    .await?;
    for threads in [1, 4] {
        for suffix in ["limit 1", "limit 2", "limit 1 offset 1"] {
            let sql = format!(
                "select k,count(*) c from {db}.late_hot group by k order by c desc,k {suffix}"
            );
            let baseline = run(&fixture, &sql, "0", threads).await?;
            let adaptive = run(&fixture, &sql, "2", threads).await?;
            assert_eq!(
                adaptive.0, baseline.0,
                "inexact candidate counts must be validated"
            );
            assert_eq!(
                adaptive.1, 1000000,
                "candidate validation must reuse the complete first pass: {threads} workers, {suffix}"
            );
        }
    }
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_adaptive_topk_count_stale_frequency_hint_is_cost_only() -> Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    let table = format!("{}.stale_frequency", fixture.default_db_name());
    fixture
        .execute_command(&format!(
            "create table {table}(k bigint not null) row_per_block = 10000 \
         analyze_frequency_columns = 'k' analyze_top_n_size = 10"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "insert into {table} select if(number % 5 = 0, number, 0) from numbers(100000)"
        ))
        .await?;
    let analyzer = fixture.new_query_ctx().await?;
    analyzer.get_settings().set_enable_table_snapshot_stats(1)?;
    TestFixture::execute_query_with_ctx(analyzer, &format!("analyze table {table}"))
        .await?
        .try_collect::<Vec<DataBlock>>()
        .await?;
    // The only hot key is absent in every appended block, so prefixes may
    // reject direct exact counts. Stale frequencies allow a complete summary,
    // but do not authorize excluding any key or reusing the old count.
    fixture
        .execute_command(&format!(
            "insert into {table} select number + 100000 from numbers(100000)"
        ))
        .await?;
    for threads in [1, 4] {
        let sql = format!("select k,count(*) c from {table} group by k order by c desc,k limit 1");
        let baseline = run(&fixture, &sql, "0", threads).await?;
        let adaptive = run(&fixture, &sql, "2", threads).await?;
        assert_eq!(adaptive.0, baseline.0);
        assert!(
            matches!(adaptive.1, 200000 | 400000),
            "a positive cost hint should complete the summary, read {}",
            adaptive.1
        );
    }
    // Now a new key wins. ANALYZE still identifies the old hot key; using
    // statistics or a prefix as the candidate proof would lose this winner.
    fixture
        .execute_command(&format!(
            "insert into {table} select 999999999 from numbers(200000)"
        ))
        .await?;
    for threads in [1, 4] {
        for limit in [1, 2] {
            let sql = format!(
                "select k,count(*) c from {table} group by k order by c desc,k limit {limit}"
            );
            assert_eq!(
                run(&fixture, &sql, "2", threads).await?.0,
                run(&fixture, &sql, "0", threads).await?.0
            );
        }
    }
    Ok(())
}
