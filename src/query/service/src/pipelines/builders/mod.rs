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

// Logs from this module will show up as "[BUILD-PIPELINES] ...".
databend_common_tracing::register_module_tag!("[BUILD-PIPELINES]");

mod builder_aggregate;
mod builder_append_table;

mod builder_fill_missing_columns;
mod builder_join;

mod builder_copy_into_table;
mod builder_mutation;
mod builder_on_finished;
mod builder_project;
mod builder_replace_into;
mod builder_sort;
mod merge_into_join_optimizations;
mod transform_builder;

pub use builder_replace_into::RawValueSource;
pub use builder_replace_into::ValueSource;
pub use builder_sort::SortPipelineBuilder;
