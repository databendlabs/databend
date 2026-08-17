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

use super::overlap_case;
use super::run_join_cases;
use super::sql_input;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_mark_join_cardinality_golden() -> Result<()> {
    run_join_cases(
        "mark.txt",
        "mark_join_cardinality",
        "MARK joins are derived by the optimizer from quantified subqueries rather than constructed as test inputs.",
        vec![
            overlap_case(
                "right_mark_from_any_overlap",
                JoinType::RightMark,
                sql_input(
                    "right_mark_from_any_projection",
                    "SELECT r.k = ANY (SELECT l.k FROM l) FROM r",
                ),
            ),
            overlap_case(
                "left_mark_from_any_overlap",
                JoinType::LeftMark,
                sql_input(
                    "left_mark_from_any_projection",
                    "SELECT l.k = ANY (SELECT r.k FROM r) FROM l",
                ),
            ),
        ],
    )
    .await
}
