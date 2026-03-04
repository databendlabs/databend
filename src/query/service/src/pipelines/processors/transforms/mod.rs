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

// Logs from this module will show up as "[TRANSFORMS] ...".
databend_common_tracing::register_module_tag!("[TRANSFORMS]");

pub mod aggregator;
mod broadcast;
mod hash_join;
pub mod hash_join_table;
mod materialized_cte;
mod new_hash_join;
pub(crate) mod range_join;
mod runtime_pool;
pub mod sort;
mod transform_async_function;
mod transform_branched_async_function;
mod transform_cache_scan;
mod transform_dictionary;
mod transform_expression_scan;

mod transform_recursive_cte_scan;
mod transform_recursive_cte_source;
mod transform_resort_addon;
mod transform_resort_addon_without_source_schema;
mod transform_srf;
mod transform_udf_script;
mod transform_udf_server;
mod window;

pub use broadcast::BroadcastSinkProcessor;
pub use broadcast::BroadcastSourceProcessor;
pub use hash_join::*;
pub use materialized_cte::CTESource;
pub use materialized_cte::MaterializedCteSink;
pub use new_hash_join::Join;
pub use new_hash_join::TransformHashJoin;
pub use new_hash_join::*;
pub use sort::*;
pub(crate) use transform_async_function::AutoIncrementNextValFetcher;
pub(crate) use transform_async_function::ReadFileContext;
pub use transform_async_function::SequenceCounters;
pub(crate) use transform_async_function::SequenceNextValFetcher;
pub use transform_async_function::TransformAsyncFunction;
pub use transform_branched_async_function::AsyncFunctionBranch;
pub use transform_branched_async_function::TransformBranchedAsyncFunction;
pub use transform_cache_scan::CacheSourceState;
pub use transform_cache_scan::HashJoinCacheState;
pub use transform_cache_scan::NewHashJoinCacheState;
pub use transform_cache_scan::TransformCacheScan;
pub use transform_expression_scan::TransformExpressionScan;
pub use transform_recursive_cte_scan::TransformRecursiveCteScan;
pub use transform_recursive_cte_source::TransformRecursiveCteSource;
pub use transform_resort_addon::TransformResortAddOn;
pub use transform_resort_addon_without_source_schema::TransformResortAddOnWithoutSourceSchema;
pub use transform_resort_addon_without_source_schema::build_expression_transform;
pub use transform_srf::TransformSRF;
pub use transform_udf_script::TransformUdfScript;
pub use transform_udf_server::TransformUdfServer;
pub use window::*;
