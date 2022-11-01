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
use common_datablocks::HashMethodKeysU128;
use common_datablocks::HashMethodKeysU16;
use common_datablocks::HashMethodKeysU256;
use common_datablocks::HashMethodKeysU32;
use common_datablocks::HashMethodKeysU512;
use common_datablocks::HashMethodKeysU64;
use common_datablocks::HashMethodKeysU8;
use common_datablocks::HashMethodSerializer;

use crate::pipelines::processors::transforms::aggregator::aggregator_final_parallel::ParallelFinalAggregator;

pub type KeysU8FinalAggregator<const HAS_AGG: bool> =
    ParallelFinalAggregator<HAS_AGG, HashMethodKeysU8>;
pub type KeysU16FinalAggregator<const HAS_AGG: bool> =
    ParallelFinalAggregator<HAS_AGG, HashMethodKeysU16>;
pub type KeysU32FinalAggregator<const HAS_AGG: bool> =
    ParallelFinalAggregator<HAS_AGG, HashMethodKeysU32>;
pub type KeysU64FinalAggregator<const HAS_AGG: bool> =
    ParallelFinalAggregator<HAS_AGG, HashMethodKeysU64>;
pub type KeysU128FinalAggregator<const HAS_AGG: bool> =
    ParallelFinalAggregator<HAS_AGG, HashMethodKeysU128>;
pub type KeysU256FinalAggregator<const HAS_AGG: bool> =
    ParallelFinalAggregator<HAS_AGG, HashMethodKeysU256>;
pub type KeysU512FinalAggregator<const HAS_AGG: bool> =
    ParallelFinalAggregator<HAS_AGG, HashMethodKeysU512>;

pub type SerializerFinalAggregator<const HAS_AGG: bool> =
    ParallelFinalAggregator<HAS_AGG, HashMethodSerializer>;
