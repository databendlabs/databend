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
use super::large_dense_partial_overlap_case;
use super::no_overlap_case;
use super::overlap_case;
use super::overlap_left_stats;
use super::run_join_cases;
use super::selective_semi_right_stats;
use super::sql_input;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_semi_join_cardinality_golden() -> Result<()> {
    run_join_cases(
        "semi.txt",
        "semi_join_cardinality_and_histogram_finish",
        "SEMI joins cap cardinality with the preserved side and keep only that side's usable histogram.",
        vec![
            overlap_case(
                "left_semi_join_overlap",
                JoinType::LeftSemi,
                sql_input(
                    "right_semi_join",
                    "SELECT * FROM l RIGHT SEMI JOIN r ON l.k = r.k",
                ),
            ),
            JoinTestCase {
                name: "left_semi_join_selective_overlap",
                description: "A selective point histogram preserves the matched row mass of the points present on both sides.",
                expected_join_type: JoinType::LeftSemi,
                input: sql_input(
                    "right_semi_join",
                    "SELECT * FROM l RIGHT SEMI JOIN r ON l.k = r.k",
                ),
                left: overlap_left_stats(),
                right: selective_semi_right_stats(),
            },
            large_dense_partial_overlap_case(
                "left_semi_join_large_synthetic_partial_overlap",
                "A dense partial overlap combines histogram row mass with full support occupancy in the overlapping range.",
                JoinType::LeftSemi,
                sql_input(
                    "right_semi_join",
                    "SELECT * FROM l RIGHT SEMI JOIN r ON l.k = r.k",
                ),
            ),
            no_overlap_case(
                "left_semi_join_no_overlap",
                JoinType::LeftSemi,
                sql_input(
                    "right_semi_join",
                    "SELECT * FROM l RIGHT SEMI JOIN r ON l.k = r.k",
                ),
            ),
            overlap_case(
                "right_semi_join_overlap",
                JoinType::RightSemi,
                sql_input(
                    "left_semi_join",
                    "SELECT * FROM l LEFT SEMI JOIN r ON l.k = r.k",
                ),
            ),
            no_overlap_case(
                "exists_no_overlap",
                JoinType::RightSemi,
                sql_input(
                    "exists",
                    "SELECT * FROM l WHERE EXISTS (SELECT 1 FROM r WHERE l.k = r.k)",
                ),
            ),
        ],
    )
    .await
}
