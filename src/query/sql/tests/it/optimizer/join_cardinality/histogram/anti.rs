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

use databend_common_exception::Result;
use databend_common_sql::plans::JoinType;

use super::JoinTestCase;
use super::TableStats;
use super::large_dense_partial_overlap_case;
use super::no_overlap_case;
use super::overlap_case;
use super::run_join_cases;
use super::sql_input;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_anti_join_cardinality_golden() -> Result<()> {
    run_join_cases(
        "anti.txt",
        "anti_join_cardinality",
        "ANTI joins are produced from SQL and preserve the optimizer-internal fixed side cardinality.",
        vec![
            overlap_case(
                "left_anti_join_overlap",
                JoinType::LeftAnti,
                sql_input(
                    "right_anti_join",
                    "SELECT * FROM l RIGHT ANTI JOIN r ON l.k = r.k",
                ),
            ),
            large_dense_partial_overlap_case(
                "left_anti_join_large_synthetic_partial_overlap",
                "A dense partial overlap combines histogram row mass with full support occupancy in the overlapping range.",
                JoinType::LeftAnti,
                sql_input(
                    "right_anti_join",
                    "SELECT * FROM l RIGHT ANTI JOIN r ON l.k = r.k",
                ),
            ),
            no_overlap_case(
                "left_anti_join_no_overlap",
                JoinType::LeftAnti,
                sql_input(
                    "right_anti_join",
                    "SELECT * FROM l RIGHT ANTI JOIN r ON l.k = r.k",
                ),
            ),
            JoinTestCase {
                name: "left_anti_join_dense_synthetic_full_overlap_reserve",
                description: "Dense synthetic histograms estimate full overlap across the same range, but cannot prove value-set coverage, so ANTI retains a conservative reserve.",
                expected_join_type: JoinType::LeftAnti,
                input: sql_input(
                    "left_anti_join",
                    "SELECT * FROM l LEFT ANTI JOIN r ON l.k = r.k",
                ),
                left: TableStats {
                    rows: 1_000_000,
                    column_json: r#"{"min": 0, "max": 9999, "ndv": 9000, "null_count": 0}"#,
                    histogram_json: None,
                },
                right: TableStats {
                    rows: 1_000_000,
                    column_json: r#"{"min": 0, "max": 9999, "ndv": 9000, "null_count": 0}"#,
                    histogram_json: None,
                },
            },
            no_overlap_case(
                "right_anti_join_no_overlap",
                JoinType::RightAnti,
                sql_input(
                    "left_anti_join",
                    "SELECT * FROM l LEFT ANTI JOIN r ON l.k = r.k",
                ),
            ),
        ],
    )
    .await
}
