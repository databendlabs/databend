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

// exports components as pipeline processors

mod processor_broadcast;
mod sink_commit;
mod transform_append;
mod transform_merge_into_mutation_aggregator;
mod transform_mutation_aggregator;

use common_expression::FieldIndex;
use common_expression::TableField;
pub use processor_broadcast::*;
pub use sink_commit::CommitSink;
pub use transform_append::AppendTransform;
pub use transform_merge_into_mutation_aggregator::*;
pub use transform_mutation_aggregator::*;

#[derive(Clone)]
pub struct OnConflictField {
    pub table_field: TableField,
    pub field_index: FieldIndex,
}
