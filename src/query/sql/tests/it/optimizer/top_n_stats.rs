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

use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::FunctionContext;
use databend_common_sql::optimizer::ir::RelExpr;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::optimizer::ir::StatContext;
use databend_common_sql::optimizer::ir::StatInfo;
use databend_common_sql::optimizer::ir::Statistics;
use databend_common_sql::plans::DummyTableScan;
use databend_common_sql::plans::TopN;

const PARTIAL: Option<bool> = Some(false);
const FINAL_STAGES: [Option<bool>; 2] = [None, Some(true)];

/// Input statistics of a TopN: estimate, conservative bound, precise rows.
struct Input(f64, f64, Option<u64>);

/// Derives the statistics of `TopN(limit, offset)` over an input with fixed
/// statistics.
fn derive(
    after_exchange: Option<bool>,
    limit: usize,
    offset: usize,
    input: Input,
) -> Result<Arc<StatInfo>> {
    let Input(cardinality, max_cardinality, precise_cardinality) = input;
    let input = SExpr::create(
        DummyTableScan::default(),
        vec![],
        None,
        None,
        Some(Arc::new(StatInfo {
            cardinality,
            max_cardinality,
            statistics: Statistics {
                precise_cardinality,
                ..Default::default()
            },
        })),
    );
    let top_n = SExpr::create_unary(
        TopN {
            items: vec![],
            limit,
            offset,
            lazy_columns: Default::default(),
            after_exchange,
        },
        input,
    );
    RelExpr::with_s_expr(&top_n)
        .derive_cardinality(&StatContext::without_settings(FunctionContext::default()))
}

fn assert_stats(stats: &StatInfo, cardinality: f64, max_cardinality: f64, precise: Option<u64>) {
    assert_eq!(
        (
            stats.cardinality,
            stats.max_cardinality,
            stats.statistics.precise_cardinality
        ),
        (cardinality, max_cardinality, precise)
    );
}

#[test]
fn test_top_n_bound_keeps_input_bound_up_to_row_limit() -> Result<()> {
    // The estimate is 40 rows, but up to 1000 rows may reach the TopN.
    // The partial stage keeps up to limit + offset = 50 candidates.
    let partial = derive(PARTIAL, 20, 30, Input(40.0, 1000.0, None))?;
    assert_stats(&partial, 40.0, 50.0, None);

    // The final stage skips 30 rows and returns at most 20.
    for stage in FINAL_STAGES {
        let stats = derive(stage, 20, 30, Input(40.0, 1000.0, None))?;
        assert_stats(&stats, 10.0, 20.0, None);
    }

    // A bound below the estimate does not lower the TopN bound below its
    // estimate.
    for stage in FINAL_STAGES {
        let stats = derive(stage, 20, 30, Input(45.0, 0.0, None))?;
        assert_stats(&stats, 15.0, 15.0, None);
    }

    // A saturated candidate capacity keeps the input bound.
    let saturated = derive(PARTIAL, usize::MAX, 30, Input(40.0, 1000.0, None))?;
    assert_stats(&saturated, 40.0, 1000.0, None);
    Ok(())
}

#[test]
fn test_top_n_bound_separates_proven_empty_from_estimated_empty() -> Result<()> {
    // An input estimated at 0 rows is not proven empty: the TopN keeps a
    // positive bound and no precise cardinality.
    let partial = derive(PARTIAL, 20, 30, Input(0.0, 1000.0, None))?;
    assert_stats(&partial, 0.0, 50.0, None);
    for stage in FINAL_STAGES {
        let stats = derive(stage, 20, 30, Input(0.0, 1000.0, None))?;
        assert_stats(&stats, 0.0, 20.0, None);
    }

    // An input bound within the offset gives a final bound of 0, but it is
    // still an estimate.
    for stage in FINAL_STAGES {
        let stats = derive(stage, 20, 30, Input(10.0, 25.0, None))?;
        assert_stats(&stats, 0.0, 0.0, None);
    }

    // A proven-empty input stays proven empty in every stage.
    for stage in [None, PARTIAL, Some(true)] {
        let stats = derive(stage, 20, 30, Input(0.0, 0.0, Some(0)))?;
        assert_stats(&stats, 0.0, 0.0, Some(0));
    }

    // 25 precise rows: the partial stage keeps all of them, and the final
    // stage skips all of them, which proves its output empty.
    let partial = derive(PARTIAL, 20, 30, Input(25.0, 25.0, Some(25)))?;
    assert_stats(&partial, 25.0, 25.0, Some(25));
    for stage in FINAL_STAGES {
        let stats = derive(stage, 20, 30, Input(25.0, 25.0, Some(25)))?;
        assert_stats(&stats, 0.0, 0.0, Some(0));
    }

    // 60 precise rows: the final stage returns exactly rows 31..50.
    for stage in FINAL_STAGES {
        let stats = derive(stage, 20, 30, Input(60.0, 60.0, Some(60)))?;
        assert_stats(&stats, 20.0, 20.0, Some(20));
    }

    // LIMIT 0 returns no rows, whatever the input.
    for stage in FINAL_STAGES {
        let stats = derive(stage, 0, 30, Input(40.0, 1000.0, None))?;
        assert_stats(&stats, 0.0, 0.0, Some(0));
    }
    Ok(())
}
