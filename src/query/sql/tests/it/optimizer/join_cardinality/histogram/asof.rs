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

use super::no_overlap_case;
use super::overlap_case;
use super::partial_overlap_case;
use super::run_join_cases;
use super::sql_input;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_asof_join_cardinality_golden() -> Result<()> {
    run_join_cases(
        "asof.txt",
        "asof_join_cardinality_and_join_key_stats",
        "ASOF join variants expose equality-key histogram estimates and the cardinality policy of each directional join type; the inequality is not separately estimated here.",
        vec![
            overlap_case(
                "asof_join_overlap",
                JoinType::Asof,
                sql_input(
                    "asof_join",
                    "SELECT * FROM l ASOF JOIN r ON l.k = r.k AND l.t >= r.t",
                ),
            ),
            partial_overlap_case(
                "left_asof_join_partial_overlap",
                JoinType::LeftAsof,
                sql_input(
                    "asof_left_join",
                    "SELECT * FROM l ASOF LEFT JOIN r ON l.k = r.k AND l.t >= r.t",
                ),
            ),
            no_overlap_case(
                "left_asof_join_no_overlap",
                JoinType::LeftAsof,
                sql_input(
                    "asof_left_join",
                    "SELECT * FROM l ASOF LEFT JOIN r ON l.k = r.k AND l.t >= r.t",
                ),
            ),
            partial_overlap_case(
                "right_asof_join_partial_overlap",
                JoinType::RightAsof,
                sql_input(
                    "asof_right_join",
                    "SELECT * FROM l ASOF RIGHT JOIN r ON l.k = r.k AND l.t >= r.t",
                ),
            ),
            no_overlap_case(
                "right_asof_join_no_overlap",
                JoinType::RightAsof,
                sql_input(
                    "asof_right_join",
                    "SELECT * FROM l ASOF RIGHT JOIN r ON l.k = r.k AND l.t >= r.t",
                ),
            ),
            overlap_case(
                "full_asof_join_overlap",
                JoinType::FullAsof,
                sql_input(
                    "asof_full_join",
                    "SELECT * FROM l ASOF FULL JOIN r ON l.k = r.k AND l.t >= r.t",
                ),
            ),
            no_overlap_case(
                "full_asof_join_no_overlap",
                JoinType::FullAsof,
                sql_input(
                    "asof_full_join",
                    "SELECT * FROM l ASOF FULL JOIN r ON l.k = r.k AND l.t >= r.t",
                ),
            ),
        ],
    )
    .await
}
