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
use super::full_mixed_right_stats;
use super::large_dense_partial_overlap_case;
use super::no_overlap_case;
use super::overlap_case;
use super::overlap_left_stats;
use super::run_join_cases;
use super::sql_input;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_outer_join_cardinality_golden() -> Result<()> {
    run_join_cases(
        "outer.txt",
        "outer_join_cardinality_and_nullable_stats",
        "LEFT, RIGHT, FULL, and ANY variants preserve their designated side while exposing optimizer-side join commutation.",
        vec![
            no_overlap_case(
                "left_join_no_overlap",
                JoinType::Left,
                sql_input("right_join", "SELECT * FROM l RIGHT JOIN r ON l.k = r.k"),
            ),
            overlap_case(
                "left_any_join_overlap",
                JoinType::LeftAny,
                sql_input(
                    "left_any_join",
                    "SELECT * FROM l LEFT ANY JOIN r ON l.k = r.k",
                ),
            ),
            no_overlap_case(
                "right_join_no_overlap",
                JoinType::Right,
                sql_input("left_join", "SELECT * FROM l LEFT JOIN r ON l.k = r.k"),
            ),
            overlap_case(
                "right_any_join_overlap",
                JoinType::RightAny,
                sql_input(
                    "right_any_join",
                    "SELECT * FROM l RIGHT ANY JOIN r ON l.k = r.k",
                ),
            ),
            overlap_case(
                "full_join_overlap",
                JoinType::Full,
                sql_input("full_join", "SELECT * FROM l FULL JOIN r ON l.k = r.k"),
            ),
            large_dense_partial_overlap_case(
                "full_join_large_synthetic_partial_overlap",
                "Two one-million-row inputs use 100-bucket synthetic histograms with half-overlapping dense ranges, exposing pair rows plus unmatched NULL extensions.",
                JoinType::Full,
                sql_input("full_join", "SELECT * FROM l FULL JOIN r ON l.k = r.k"),
            ),
            JoinTestCase {
                name: "full_join_one_sided_unmatched_rows",
                description: "The inner estimate lies between the two input cardinalities, so FULL preserves unmatched rows from only the larger side.",
                expected_join_type: JoinType::Full,
                input: sql_input("full_join", "SELECT * FROM l FULL JOIN r ON l.k = r.k"),
                left: overlap_left_stats(),
                right: full_mixed_right_stats(),
            },
            no_overlap_case(
                "full_join_no_overlap",
                JoinType::Full,
                sql_input("full_join", "SELECT * FROM l FULL JOIN r ON l.k = r.k"),
            ),
        ],
    )
    .await
}
