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

mod rule_commute_join;
mod rule_eliminate_eval_scalar;
mod rule_eliminate_sort;
mod rule_eliminate_union;
mod rule_filter_nulls;
mod rule_merge_eval_scalar;
mod rule_normalize_scalar;
mod rule_push_down_prewhere;
mod rule_push_down_sort_expression;
mod rule_push_down_sort_scan;

pub use rule_commute_join::RuleCommuteJoin;
pub use rule_eliminate_eval_scalar::RuleEliminateEvalScalar;
pub use rule_eliminate_sort::RuleEliminateSort;
pub use rule_eliminate_union::RuleEliminateUnion;
pub use rule_filter_nulls::RuleFilterNulls;
pub use rule_merge_eval_scalar::RuleMergeEvalScalar;
pub use rule_normalize_scalar::RuleNormalizeScalarFilter;
pub use rule_push_down_prewhere::RulePushDownPrewhere;
pub use rule_push_down_sort_expression::RulePushDownSortEvalScalar;
pub use rule_push_down_sort_scan::RulePushDownSortScan;
