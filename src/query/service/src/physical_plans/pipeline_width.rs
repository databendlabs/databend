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

//! Plan shapes whose queries run best with a narrower pipeline, for
//! `enable_auto_pipeline_width`. A rule inspects the whole local plan and
//! returns a cap on `max_threads`, or `None`. Rules only lower the width, so
//! when several match the smallest cap wins; none performs IO or chooses the
//! executor.

use std::num::NonZeroUsize;

use databend_common_catalog::plan::PartInfoType;
use databend_common_storages_fuse::operations::read::scan_block_bound;

use super::FinalTopNPlan;
use super::PhysicalPlan;
use super::PhysicalPlanCast;
use super::RowFetch;
use super::index_top_n::index_top_k;

const RULES: &[fn(&PhysicalPlan) -> Option<NonZeroUsize>] = &[bounded_index_top_k];

pub(crate) fn auto_pipeline_width(plan: &PhysicalPlan) -> Option<NonZeroUsize> {
    RULES.iter().filter_map(|rule| rule(plan)).min()
}

/// Candidates per block and blocks per scan covered by the search benchmark
/// behind this rule (four-block FTS and vector Top-100). Larger scans keep
/// their width until measured.
const MAX_CANDIDATES_PER_BLOCK: usize = 100;
const MAX_BLOCKS: usize = 4;

/// An FTS or vector Top-K that merges index candidates from a few blocks, with
/// at most a row fetch above it. The scan must be lazy: its blocks are pruned
/// while the pipeline runs, so the cap covers the search; the one-block
/// `IndexTopNSource` path has eager parts and needs no cap.
fn bounded_index_top_k(plan: &PhysicalPlan) -> Option<NonZeroUsize> {
    let input = match RowFetch::from_physical_plan(plan) {
        Some(fetch) if !fetch.enable_block_id_repartition => &fetch.input,
        Some(_) => return None,
        None => plan,
    };
    let top_k = index_top_k(FinalTopNPlan::from_physical_plan(input)?)?;
    let source = &top_k.scan.source;
    let bounded = top_k.per_block_candidates <= MAX_CANDIDATES_PER_BLOCK
        && source.parts.partitions_type() == PartInfoType::LazyLevel
        && matches!(scan_block_bound(source), Some(2..=MAX_BLOCKS));
    bounded.then_some(NonZeroUsize::MIN)
}
