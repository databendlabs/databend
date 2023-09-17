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

mod processor_matched_split;
mod processor_merge_into_not_matched;
mod processor_merge_into_split;
mod transform_matched_mutation_aggregator;
pub use processor_matched_split::MatchedSplitProcessor;
pub(crate) use processor_matched_split::RowIdKind;
pub use processor_merge_into_not_matched::MergeIntoNotMatchedProcessor;
pub use processor_merge_into_split::MergeIntoSplitProcessor;
