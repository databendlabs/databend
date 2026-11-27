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
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use unicase::Ascii;

use crate::aggregates::registry::AGGR_REGISTRY;

pub mod aggregates;
mod cast_rules;
mod casts;
pub mod scalars;
pub mod srfs;
pub mod topk_count;

pub fn is_builtin_function(name: &str) -> bool {
    let name = Ascii::new(name);
    BUILTIN_FUNCTIONS.contains(name.into_inner())
        || AGGR_REGISTRY.contains(name.into_inner())
        || GENERAL_WINDOW_FUNCTIONS.contains(&name)
        || GENERAL_LAMBDA_FUNCTIONS.contains(&name)
        || GENERAL_SEARCH_FUNCTIONS.contains(&name)
        || ASYNC_FUNCTIONS.contains(&name)
}

// The plan of search function, async function and udf contains some arguments defined in meta,
// which may be modified by user at any time. Those functions are not not suitable for caching.
// Non-deterministic builtins such as `now()` are cacheable: the planner does not cache a plan
// built while one of them was evaluated.
pub fn is_cacheable_function(name: &str) -> bool {
    let name = Ascii::new(name);
    BUILTIN_FUNCTIONS.contains(name.into_inner())
        || AGGR_REGISTRY.contains(name.into_inner())
        || GENERAL_WINDOW_FUNCTIONS.contains(&name)
        || GENERAL_LAMBDA_FUNCTIONS.contains(&name)
}

#[ctor]
pub static BUILTIN_FUNCTIONS: FunctionRegistry = builtin_functions();

pub const ASYNC_FUNCTIONS: [Ascii<&str>; 4] = [
    Ascii::new("nextval"),
    Ascii::new("dict_get"),
    Ascii::new("read_file"),
    Ascii::new("sleep"),
];

/// The functions that convert a value to a type, with the type, except `to_decimal`, whose
/// decimal size a constant cannot hold. `to_x(a)` of a value of type X keeps the value: `to_x`
/// has no overload for its own type, and the SQL binder binds the call as `a`. A cast to X runs
/// `to_x` unless it is registered with another function.
pub const CONVERSION_FUNCTIONS: &[(&str, DataType)] = &[
    ("to_boolean", DataType::Boolean),
    ("to_string", DataType::String),
    ("to_binary", DataType::Binary),
    ("to_date", DataType::Date),
    ("to_timestamp", DataType::Timestamp),
    ("to_timestamp_tz", DataType::TimestampTz),
    ("to_interval", DataType::Interval),
    ("to_variant", DataType::Variant),
    ("to_bitmap", DataType::Bitmap),
    ("to_geometry", DataType::Geometry),
    ("to_geography", DataType::Geography),
    ("to_uint8", DataType::Number(NumberDataType::UInt8)),
    ("to_uint16", DataType::Number(NumberDataType::UInt16)),
    ("to_uint32", DataType::Number(NumberDataType::UInt32)),
    ("to_uint64", DataType::Number(NumberDataType::UInt64)),
    ("to_int8", DataType::Number(NumberDataType::Int8)),
    ("to_int16", DataType::Number(NumberDataType::Int16)),
    ("to_int32", DataType::Number(NumberDataType::Int32)),
    ("to_int64", DataType::Number(NumberDataType::Int64)),
    ("to_float32", DataType::Number(NumberDataType::Float32)),
    ("to_float64", DataType::Number(NumberDataType::Float64)),
];

/// The try functions that take a value of their own type, with the type: the SQL binder binds
/// `try_to_x(a)` of a value of type X as `a` of type `X NULL`. The try functions have no
/// overload for their own type; this only keeps the results of earlier versions, which ran
/// `try_to_x(a)` as `TRY_CAST(a AS X)` and accepted these through an implicit argument cast that
/// they dropped. The other try functions reject a value of their own type.
pub const TRY_CONVERSIONS_TO_OWN_TYPE: &[(&str, DataType)] = &[
    ("try_to_string", DataType::String),
    ("try_to_date", DataType::Date),
    ("try_to_uint8", DataType::Number(NumberDataType::UInt8)),
    ("try_to_uint16", DataType::Number(NumberDataType::UInt16)),
    ("try_to_uint32", DataType::Number(NumberDataType::UInt32)),
    ("try_to_uint64", DataType::Number(NumberDataType::UInt64)),
    ("try_to_int8", DataType::Number(NumberDataType::Int8)),
    ("try_to_int16", DataType::Number(NumberDataType::Int16)),
    ("try_to_int32", DataType::Number(NumberDataType::Int32)),
    ("try_to_int64", DataType::Number(NumberDataType::Int64)),
    ("try_to_float32", DataType::Number(NumberDataType::Float32)),
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
    casts::register(&mut registry);
    srfs::register(&mut registry);

    registry.check_ambiguity();
    registry
}
