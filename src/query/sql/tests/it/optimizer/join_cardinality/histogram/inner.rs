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
use super::overlap_right_stats;
use super::run_join_cases;
use super::sql_input;

#[tokio::test(flavor = "multi_thread", worker_threads = 1)]
async fn test_inner_join_cardinality_golden() -> Result<()> {
    run_join_cases(
        "inner.txt",
        "inner_join_cardinality_and_join_key_stats",
        "CROSS, INNER, and INNER ANY expose Cartesian, overlapping, and disjoint histogram behavior through optimized SQL plans.",
        vec![
            JoinTestCase {
                name: "cross_join",
                description: "A CROSS join has no equality condition, so its output is the Cartesian product and retains input join-key statistics.",
                expected_join_type: JoinType::Cross,
                input: sql_input("cross_join", "SELECT * FROM l CROSS JOIN r"),
                left: overlap_left_stats(),
                right: overlap_right_stats(),
            },
            overlap_case(
                "inner_join_overlap",
                JoinType::Inner,
                sql_input("inner_join", "SELECT * FROM l INNER JOIN r ON l.k = r.k"),
            ),
            JoinTestCase {
                name: "inner_join_missing_left_histogram_input",
                description: "The left persisted histogram is absent, so Scan follows its existing synthetic-histogram fallback from min/max/NDV before join estimation.",
                expected_join_type: JoinType::Inner,
                input: sql_input(
                    "inner_join",
                    "SELECT * FROM l INNER JOIN r ON l.k = r.k",
                ),
                left: overlap_left_stats().without_histogram_input(),
                right: overlap_right_stats(),
            },
            JoinTestCase {
                name: "inner_join_missing_right_histogram_input",
                description: "The right persisted histogram is absent, so Scan follows its existing synthetic-histogram fallback from min/max/NDV before join estimation.",
                expected_join_type: JoinType::Inner,
                input: sql_input(
                    "inner_join",
                    "SELECT * FROM l INNER JOIN r ON l.k = r.k",
                ),
                left: overlap_left_stats(),
                right: overlap_right_stats().without_histogram_input(),
            },
            JoinTestCase {
                name: "inner_join_missing_both_histogram_inputs",
                description: "Both persisted histograms are absent, so each Scan independently follows the existing min/max/NDV synthetic-histogram fallback.",
                expected_join_type: JoinType::Inner,
                input: sql_input(
                    "inner_join",
                    "SELECT * FROM l INNER JOIN r ON l.k = r.k",
                ),
                left: overlap_left_stats().without_histogram_input(),
                right: overlap_right_stats().without_histogram_input(),
            },
            large_dense_partial_overlap_case(
                "inner_join_large_synthetic_partial_overlap",
                "Both Scans synthesize 100 buckets from one million rows and 10,000 dense values; only half of each value range overlaps.",
                JoinType::Inner,
                sql_input(
                    "inner_join",
                    "SELECT * FROM l INNER JOIN r ON l.k = r.k",
                ),
            ),
            large_dense_partial_overlap_case(
                "inner_join_large_ndv_fallback_partial_overlap",
                "The same large dense inputs use derived join keys, naturally dropping both synthetic histograms and exposing the NDV fallback for comparison.",
                JoinType::Inner,
                sql_input(
                    "inner_join_with_both_derived_keys",
                    "SELECT * FROM l INNER JOIN r ON l.k + 0 = r.k + 0",
                ),
            ),
            JoinTestCase {
                name: "inner_join_derived_left_key_without_histogram",
                description: "A derived left join key keeps bounds and NDV but has no typed histogram, so join estimation naturally falls back to NDV.",
                expected_join_type: JoinType::Inner,
                input: sql_input(
                    "inner_join_with_derived_left_key",
                    "SELECT * FROM l INNER JOIN r ON l.k + 0 = r.k",
                ),
                left: overlap_left_stats(),
                right: overlap_right_stats(),
            },
            JoinTestCase {
                name: "inner_join_derived_right_key_without_histogram",
                description: "A derived right join key keeps bounds and NDV but has no typed histogram, so join estimation naturally falls back to NDV.",
                expected_join_type: JoinType::Inner,
                input: sql_input(
                    "inner_join_with_derived_right_key",
                    "SELECT * FROM l INNER JOIN r ON l.k = r.k + 0",
                ),
                left: overlap_left_stats(),
                right: overlap_right_stats(),
            },
            JoinTestCase {
                name: "inner_join_both_derived_keys_without_histograms",
                description: "Both derived join keys keep bounds and NDV but lose typed histograms, exercising the two-sided NDV fallback through SQL.",
                expected_join_type: JoinType::Inner,
                input: sql_input(
                    "inner_join_with_both_derived_keys",
                    "SELECT * FROM l INNER JOIN r ON l.k + 0 = r.k + 0",
                ),
                left: overlap_left_stats(),
                right: overlap_right_stats(),
            },
            no_overlap_case(
                "inner_join_no_overlap",
                JoinType::Inner,
                sql_input("inner_join", "SELECT * FROM l INNER JOIN r ON l.k = r.k"),
            ),
            overlap_case(
                "inner_any_join_overlap",
                JoinType::InnerAny,
                sql_input(
                    "inner_any_join",
                    "SELECT * FROM l INNER ANY JOIN r ON l.k = r.k",
                ),
            ),
        ],
    )
    .await
}
