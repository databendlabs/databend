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

#![allow(incomplete_features)]
#![feature(deref_patterns)]

use ctor::ctor;
use databend_common_expression::FunctionRegistry;
use unicase::Ascii;

use crate::aggregates::registry::AGGR_REGISTRY;

pub mod aggregates;
mod cast_rules;
pub mod scalars;
pub mod srfs;

pub fn is_builtin_function(name: &str) -> bool {
    let name = Ascii::new(name);
    BUILTIN_FUNCTIONS.contains(name.into_inner())
        || AGGR_REGISTRY.contains(name.into_inner())
        || GENERAL_WINDOW_FUNCTIONS.contains(&name)
        || GENERAL_LAMBDA_FUNCTIONS.contains(&name)
        || GENERAL_SEARCH_FUNCTIONS.contains(&name)
        || ASYNC_FUNCTIONS.contains(&name)
}

/// Whether a query calling `name` may have its logical plan cached by the planner.
///
/// The plan of search function, async function and udf contains some arguments defined in meta,
/// which may be modified by user at any time. Those functions are not suitable for caching.
///
/// The query-time functions in [`PLAN_CACHEABLE_QUERY_TIME_FUNCTIONS`] are also eligible:
/// the type checker keeps them symbolic in the logical plan, and the physical plan folds them
/// with the function context of the current query. Planning paths that turn them into a value
/// stored in the logical plan mark that plan as not cacheable. Other non-deterministic builtins
/// keep their existing policy.
pub fn is_cacheable_function(name: &str) -> bool {
    let ascii_name = Ascii::new(name);
    (BUILTIN_FUNCTIONS.contains(ascii_name.into_inner())
        && (!BUILTIN_FUNCTIONS
            .get_property(name)
            .unwrap()
            .non_deterministic
            || PLAN_CACHEABLE_QUERY_TIME_FUNCTIONS.contains(&ascii_name)))
        || AGGR_REGISTRY.contains(ascii_name.into_inner())
        || GENERAL_WINDOW_FUNCTIONS.contains(&ascii_name)
        || GENERAL_LAMBDA_FUNCTIONS.contains(&ascii_name)
}

/// Non-deterministic builtins that read only the query start time and the session time zone
/// from the function context. Their value is the same for every row of one query, and a cached
/// logical plan that keeps them symbolic is folded again for each query.
pub const PLAN_CACHEABLE_QUERY_TIME_FUNCTIONS: [Ascii<&str>; 7] = [
    Ascii::new("now"),
    Ascii::new("current_timestamp"),
    Ascii::new("current_time"),
    Ascii::new("today"),
    Ascii::new("current_date"),
    Ascii::new("yesterday"),
    Ascii::new("tomorrow"),
];

#[ctor]
pub static BUILTIN_FUNCTIONS: FunctionRegistry = builtin_functions();

pub const ASYNC_FUNCTIONS: [Ascii<&str>; 4] = [
    Ascii::new("nextval"),
    Ascii::new("dict_get"),
    Ascii::new("read_file"),
    Ascii::new("sleep"),
];

pub const GENERAL_WITHIN_GROUP_FUNCTIONS: [Ascii<&str>; 5] = [
    Ascii::new("array_agg"),
    Ascii::new("group_concat"),
    Ascii::new("list"),
    Ascii::new("listagg"),
    Ascii::new("string_agg"),
];

pub const GENERAL_WINDOW_FUNCTIONS: [Ascii<&str>; 13] = [
    Ascii::new("row_number"),
    Ascii::new("rank"),
    Ascii::new("dense_rank"),
    Ascii::new("percent_rank"),
    Ascii::new("lag"),
    Ascii::new("lead"),
    Ascii::new("first_value"),
    Ascii::new("first"),
    Ascii::new("last_value"),
    Ascii::new("last"),
    Ascii::new("nth_value"),
    Ascii::new("ntile"),
    Ascii::new("cume_dist"),
];

pub const RANK_WINDOW_FUNCTIONS: [&str; 5] =
    ["first_value", "first", "last_value", "last", "nth_value"];

pub const GENERAL_LAMBDA_FUNCTIONS: [Ascii<&str>; 17] = [
    Ascii::new("array_transform"),
    Ascii::new("array_apply"),
    Ascii::new("array_map"),
    Ascii::new("array_filter"),
    Ascii::new("array_reduce"),
    Ascii::new("json_array_transform"),
    Ascii::new("json_array_apply"),
    Ascii::new("json_array_map"),
    Ascii::new("json_array_filter"),
    Ascii::new("json_array_reduce"),
    Ascii::new("map_filter"),
    Ascii::new("map_transform_keys"),
    Ascii::new("map_transform_values"),
    Ascii::new("json_map_filter"),
    Ascii::new("json_map_transform_keys"),
    Ascii::new("json_map_transform_values"),
    Ascii::new("json_path_transform"),
];

pub const GENERAL_SEARCH_FUNCTIONS: [Ascii<&str>; 3] = [
    Ascii::new("match"),
    Ascii::new("query"),
    Ascii::new("score"),
];

// The function names and the number of arguments that support spatial index
pub const SPATIAL_INDEX_FUNCTIONS: [(Ascii<&str>, usize); 6] = [
    (Ascii::new("st_contains"), 2),
    (Ascii::new("st_intersects"), 2),
    (Ascii::new("st_within"), 2),
    (Ascii::new("st_dwithin"), 3),
    (Ascii::new("st_covers"), 2),
    (Ascii::new("st_coveredby"), 2),
];

fn builtin_functions() -> FunctionRegistry {
    let mut registry = FunctionRegistry::empty();

    cast_rules::register(&mut registry);
    scalars::register(&mut registry);
    srfs::register(&mut registry);

    registry.check_ambiguity();
    registry
}
