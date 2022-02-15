// Copyright 2022 Datafuse Labs.
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

mod aggregator_final;
mod aggregator_params;
mod aggregator_partial;
mod aggregator_single_key;

pub use aggregator_final::FinalAggregator;
pub use aggregator_final::KeysU16FinalAggregator;
pub use aggregator_final::KeysU32FinalAggregator;
pub use aggregator_final::KeysU64FinalAggregator;
pub use aggregator_final::KeysU8FinalAggregator;
pub use aggregator_final::SerializerFinalAggregator;
pub use aggregator_params::AggregatorParams;
pub use aggregator_params::AggregatorTransformParams;
pub use aggregator_partial::KeysU16PartialAggregator;
pub use aggregator_partial::KeysU32PartialAggregator;
pub use aggregator_partial::KeysU64PartialAggregator;
pub use aggregator_partial::KeysU8PartialAggregator;
pub use aggregator_partial::PartialAggregator;
pub use aggregator_partial::SerializerPartialAggregator;
pub use aggregator_single_key::FinalSingleKeyAggregator;
pub use aggregator_single_key::PartialSingleKeyAggregator;
pub use aggregator_single_key::SingleKeyAggregator;
