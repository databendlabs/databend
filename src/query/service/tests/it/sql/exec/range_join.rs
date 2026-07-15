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

use databend_common_expression::DataBlock;
use databend_common_expression::ScalarRef;
use databend_common_expression::types::number::NumberScalar;
use databend_query::test_kits::TestFixture;
use futures_util::TryStreamExt;

async fn explain_text(fixture: &TestFixture, query: &str) -> anyhow::Result<String> {
    let blocks = fixture
        .execute_query(&format!("EXPLAIN {query}"))
        .await?
        .try_collect::<Vec<DataBlock>>()
        .await?;
    let block = DataBlock::concat(&blocks).expect("concat explain blocks");
    let column = block.get_by_offset(0);
    let mut lines = Vec::with_capacity(block.num_rows());
    for row in 0..block.num_rows() {
        if let Some(ScalarRef::String(line)) = column.index(row) {
            lines.push(line.to_string());
        }
    }
    Ok(lines.join("\n"))
}

fn extract_one_u64(blocks: Vec<DataBlock>) -> u64 {
    let block = DataBlock::concat(&blocks).expect("concat result blocks");
    assert_eq!(block.num_rows(), 1);
    match block.get_by_offset(0).index(0) {
        Some(ScalarRef::Number(NumberScalar::UInt64(v))) => v,
        other => panic!("unexpected count scalar: {other:?}"),
    }
}

/// An inequality join with a precise-one-row aggregate side should use CROSS JOIN + FILTER
/// instead of RangeJoin. Exercise both equivalent predicate operand orders and verify the
/// selected physical operator and query result.
#[tokio::test(flavor = "multi_thread")]
async fn test_inequality_join_with_scalar_side_avoids_range_join() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    let db = fixture.default_db_name();

    fixture
        .execute_command(&format!(
            "CREATE TABLE {db}.big(ts INT) AS SELECT number FROM numbers(1000)"
        ))
        .await?;
    fixture
        .execute_command(&format!(
            "CREATE TABLE {db}.threshold(ts INT) AS SELECT number FROM numbers(1000)"
        ))
        .await?;

    // Scalar subquery on the right side of the inequality.
    let q_right = format!(
        "SELECT count(*) FROM {db}.big b \
         WHERE b.ts >= (SELECT min(ts) FROM {db}.threshold)"
    );
    // Equivalent predicate with the scalar subquery written on the left.
    let q_left = format!(
        "SELECT count(*) FROM {db}.big b \
         WHERE (SELECT min(ts) FROM {db}.threshold) <= b.ts"
    );

    for query in [&q_right, &q_left] {
        let plan = explain_text(&fixture, query).await?;
        assert!(
            !plan.contains("RangeJoin"),
            "single-row inequality side must not produce a RangeJoin, plan was:\n{plan}"
        );
    }

    // Both forms must produce the correct result: all 1000 rows match `ts >= min(ts)`.
    for query in [&q_right, &q_left] {
        let blocks = fixture
            .execute_query(query)
            .await?
            .try_collect::<Vec<DataBlock>>()
            .await?;
        assert_eq!(extract_one_u64(blocks), 1000, "query: {query}");
    }

    Ok(())
}

/// Force the exact CROSS JOIN + FILTER shape that used to report a false scalar-subquery
/// cardinality violation. The pushed-down expression is true for all four rows, but its
/// estimated selectivity places the four-row table on the build side and the exact-one-row
/// scalar aggregate on the probe side. The CROSS JOIN + FILTER execution must not treat the
/// four cross-product candidates as four scalar-subquery rows.
#[tokio::test(flavor = "multi_thread")]
async fn test_scalar_aggregate_probe_cross_join_filter() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    let db = fixture.default_db_name();

    fixture
        .execute_command(&format!(
            "CREATE TABLE {db}.four_rows(ts INT) AS SELECT number FROM numbers(4)"
        ))
        .await?;

    let query = format!(
        "SELECT count(*) \
         FROM (SELECT ts FROM {db}.four_rows WHERE length(to_string(ts)) > 0) b \
         WHERE b.ts >= (SELECT min(number) FROM numbers(1))"
    );

    let plan = explain_text(&fixture, &query).await?;
    assert!(plan.contains("HashJoin"), "plan was:\n{plan}");
    assert!(plan.contains("TableScan(Build)"), "plan was:\n{plan}");
    assert!(plan.contains("AggregateFinal(Probe)"), "plan was:\n{plan}");

    let blocks = fixture
        .execute_query(&query)
        .await?
        .try_collect::<Vec<DataBlock>>()
        .await?;
    assert_eq!(extract_one_u64(blocks), 4);

    Ok(())
}

fn extract_two_u64(blocks: Vec<DataBlock>) -> (u64, u64) {
    let block = DataBlock::concat(&blocks).expect("concat blocks");
    assert_eq!(block.num_rows(), 1, "unexpected rows: {}", block.num_rows());
    assert!(
        block.num_columns() >= 2,
        "expected at least two columns, got {}",
        block.num_columns()
    );

    let first = block
        .get_by_offset(0)
        .index(0)
        .expect("scalar at row 0, col 0");
    let second = block
        .get_by_offset(1)
        .index(0)
        .expect("scalar at row 0, col 1");

    let to_u64 = |value: ScalarRef<'_>, col: usize| -> u64 {
        match value {
            ScalarRef::Number(NumberScalar::UInt64(v)) => v,
            ScalarRef::Number(NumberScalar::UInt32(v)) => v as u64,
            ScalarRef::Number(NumberScalar::Int64(v)) => v as u64,
            other => panic!("unexpected scalar type for col {col}: {other:?}"),
        }
    };

    (to_u64(first, 0), to_u64(second, 1))
}

#[tokio::test(flavor = "multi_thread")]
async fn test_iejoin_outer_join_with_empty_input() -> anyhow::Result<()> {
    let fixture = TestFixture::setup().await?;
    fixture.create_default_database().await?;
    let db = fixture.default_db_name();

    fixture
        .execute_command(&format!("CREATE TABLE {db}.left_values(x INT)"))
        .await?;
    fixture
        .execute_command(&format!("CREATE TABLE {db}.right_values(x INT)"))
        .await?;
    fixture
        .execute_command(&format!("INSERT INTO {db}.left_values VALUES (1), (2)"))
        .await?;
    fixture
        .execute_command(&format!("INSERT INTO {db}.right_values VALUES (1), (2)"))
        .await?;

    let left_join_query = format!(
        "SELECT COUNT(*), COUNT(b.x) \
         FROM {db}.left_values a \
         LEFT JOIN (SELECT * FROM {db}.right_values WHERE 1 = 0) b \
         ON a.x BETWEEN b.x AND b.x"
    );
    let left_join_blocks = fixture
        .execute_query(&left_join_query)
        .await?
        .try_collect::<Vec<DataBlock>>()
        .await?;
    assert_eq!(extract_two_u64(left_join_blocks), (2, 0));

    let right_join_query = format!(
        "SELECT COUNT(*), COUNT(a.x) \
         FROM (SELECT * FROM {db}.left_values WHERE 1 = 0) a \
         RIGHT JOIN {db}.right_values b \
         ON a.x BETWEEN b.x AND b.x"
    );
    let right_join_blocks = fixture
        .execute_query(&right_join_query)
        .await?
        .try_collect::<Vec<DataBlock>>()
        .await?;
    assert_eq!(extract_two_u64(right_join_blocks), (2, 0));

    Ok(())
}
