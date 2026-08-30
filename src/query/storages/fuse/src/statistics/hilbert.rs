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

//! Exact two-dimensional clustering diagnostics.
//!
//! The pipeline encodes and ranks both endpoint dimensions, chooses an outer sweep axis, then
//! optionally partitions that axis for parallel execution. Each partition recompresses its inner
//! coordinates, while rectangles crossing boundaries are initialized as active in later
//! partitions. Pair ownership follows the later rectangle start, preventing duplicate counts.

use std::ops::Range;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::types::BinaryColumn;
use databend_common_expression::types::DataType;
use rayon::prelude::*;

use super::endpoint_sort;

const MAX_SWEEP_PARTITIONS: usize = 8;
const MIN_RECTS_PER_SWEEP_PARTITION: usize = 4096;
// A rectangle crossing partition boundaries is copied into each affected partition. Allow at most
// 10% extra copies so parallelism cannot cause disproportionate CPU and memory amplification.
const MAX_EXTRA_SWEEP_REPLICAS_PERCENT: usize = 10;

/// A closed 2D range. Production sweeps use dense endpoint ranks rather than original values.
#[derive(Clone, Copy)]
struct Rect<T> {
    x_min: T,
    x_max: T,
    y_min: T,
    y_max: T,
}

/// One dimension after coordinate compression.
/// `ranks[block]` stores `[min, max]`. `starts` and `ends` contain block IDs ordered by their
/// corresponding endpoint and let the sweep consume events without sorting them again.
struct RankedDimension {
    count: usize,
    ranks: Vec<[u32; 2]>,
    starts: Vec<u32>,
    ends: Vec<u32>,
}

/// Convert sorted comparable endpoints into dense ranks while retaining start/end event order.
/// `order` must be a permutation of endpoint row IDs sorted by `keys`. Under the caller-provided
/// alternating layout, row-ID parity assigns each endpoint its block minimum or maximum role.
fn rank_dimension(keys: &BinaryColumn, order: Vec<u32>) -> RankedDimension {
    let mut ranks = vec![[0u32; 2]; keys.len() / 2];
    let mut starts = Vec::with_capacity(ranks.len());
    let mut ends = Vec::with_capacity(ranks.len());
    // Endpoint-count validation guarantees at most u32::MAX rows, so the largest zero-based dense
    // rank also fits in u32.
    let mut rank = 0u32;
    let mut previous = None;

    for endpoint in order {
        if previous.is_some_and(|previous| {
            // SAFETY: both ids originate from 0..keys.len().
            (unsafe { keys.index_unchecked(previous as usize) })
                != unsafe { keys.index_unchecked(endpoint as usize) }
        }) {
            rank += 1;
        }
        previous = Some(endpoint);

        let block = endpoint as usize / 2;
        let min_or_max = endpoint as usize & 1;
        ranks[block][min_or_max] = rank;
        if min_or_max == 0 {
            starts.push(block as u32);
        } else {
            ends.push(block as u32);
        }
    }

    RankedDimension {
        count: usize::from(previous.is_some()) + rank as usize,
        ranks,
        starts,
        ends,
    }
}

/// Lazy segment tree over compressed inner-axis coordinates.
/// Range additions activate or deactivate a rectangle interval; the root always stores the
/// maximum number of simultaneously active rectangles.
struct RangeAddMaxTree {
    max: Vec<i32>,
    lazy: Vec<i32>,
    leaf_count: usize,
}

impl RangeAddMaxTree {
    fn new(len: usize) -> Self {
        let leaf_count = len.max(1).next_power_of_two();
        Self {
            max: vec![0; leaf_count * 2],
            lazy: vec![0; leaf_count * 2],
            leaf_count,
        }
    }

    fn add(&mut self, left: usize, right: usize, delta: i32) {
        self.add_inner(left, right, delta, 1, 0..self.leaf_count);
    }

    fn max(&self) -> usize {
        self.max[1].max(0) as usize
    }

    fn add_inner(
        &mut self,
        left: usize,
        right: usize,
        delta: i32,
        node: usize,
        node_range: Range<usize>,
    ) {
        if right <= node_range.start || node_range.end <= left {
            return;
        }
        if left <= node_range.start && node_range.end <= right {
            self.max[node] += delta;
            self.lazy[node] += delta;
            return;
        }
        let mid = node_range.start + node_range.len() / 2;
        self.add_inner(left, right, delta, node * 2, node_range.start..mid);
        self.add_inner(left, right, delta, node * 2 + 1, mid..node_range.end);
        self.max[node] = self.lazy[node] + self.max[node * 2].max(self.max[node * 2 + 1]);
    }
}

/// Active inner-axis intervals used to count rectangle intersections during the outer-axis sweep.
/// Each Fenwick node stores counts for interval minima and maxima in one allocation.
struct ActiveIntervals {
    endpoints: Vec<[u32; 2]>,
}

impl ActiveIntervals {
    const MIN: usize = 0;
    const MAX: usize = 1;

    fn new(len: usize) -> Self {
        Self {
            endpoints: vec![[0; 2]; len + 1],
        }
    }

    fn add(&mut self, min: usize, max: usize, delta: i32) {
        self.add_endpoint(min, Self::MIN, delta);
        self.add_endpoint(max, Self::MAX, delta);
    }

    fn add_endpoint(&mut self, index: usize, endpoint: usize, delta: i32) {
        let mut pos = index + 1;
        while pos < self.endpoints.len() {
            if delta >= 0 {
                self.endpoints[pos][endpoint] += delta as u32;
            } else {
                self.endpoints[pos][endpoint] -= delta.unsigned_abs();
            }
            pos += pos & pos.wrapping_neg();
        }
    }

    fn prefix_sum(&self, end: usize, endpoint: usize) -> usize {
        let mut pos = end;
        let mut sum = 0u32;
        while pos > 0 {
            sum += self.endpoints[pos][endpoint];
            pos &= pos - 1;
        }
        sum as usize
    }
}

/// A sweep event at one outer-axis coordinate, carrying its closed inner-axis interval.
#[derive(Clone, Copy)]
struct SweepEvent {
    outer: u32,
    inner_min: u32,
    inner_max: u32,
}

/// Ordered event access shared by global and partition-local sweeps.
/// The generic interface is monomorphized, so the hot sweep loop does not pay dynamic dispatch.
trait SweepEvents {
    fn start(&self, position: usize) -> Option<SweepEvent>;
    fn end(&self, position: usize) -> Option<SweepEvent>;
}

/// Zero-copy event view used when the planner keeps a single global partition.
struct RankedEvents<'a> {
    boxes: &'a [Rect<u32>],
    starts: &'a [u32],
    ends: &'a [u32],
}

impl SweepEvents for RankedEvents<'_> {
    fn start(&self, position: usize) -> Option<SweepEvent> {
        self.starts.get(position).map(|index| {
            let item = self.boxes[*index as usize];
            SweepEvent {
                outer: item.x_min,
                inner_min: item.y_min,
                inner_max: item.y_max,
            }
        })
    }

    fn end(&self, position: usize) -> Option<SweepEvent> {
        self.ends.get(position).map(|index| {
            let item = self.boxes[*index as usize];
            SweepEvent {
                outer: item.x_max,
                inner_min: item.y_min,
                inner_max: item.y_max,
            }
        })
    }
}

/// Owned events whose inner coordinates were recompressed for one partition.
/// Unlike `RankedEvents`, these events cannot borrow global coordinates because every partition has
/// its own dense inner-rank domain.
struct LocalEvents {
    starts: Vec<SweepEvent>,
    ends: Vec<SweepEvent>,
}

impl SweepEvents for LocalEvents {
    fn start(&self, position: usize) -> Option<SweepEvent> {
        self.starts.get(position).copied()
    }

    fn end(&self, position: usize) -> Option<SweepEvent> {
        self.ends.get(position).copied()
    }
}

/// All state required to sweep one outer-axis partition.
/// `initial_active` contains rectangles that started in an earlier partition. They contribute to
/// depth and to pairs with local starts, but are never paired with each other again. The two maxima
/// are hard upper bounds that permit exact early termination.
struct SweepPartition<E> {
    initial_active: Vec<[u32; 2]>,
    events: E,
    inner_count: usize,
    max_depth: usize,
    max_overlap_pairs: u64,
}

fn next_event_outer(start: Option<SweepEvent>, end: Option<SweepEvent>) -> Option<u32> {
    // The caller consumes starts before ends when both occur at this coordinate, preserving closed
    // rectangle semantics while this helper only selects the next coordinate to visit.
    match (start, end) {
        (Some(start), Some(end)) => Some(start.outer.min(end.outer)),
        (Some(start), None) => Some(start.outer),
        (None, Some(end)) => Some(end.outer),
        (None, None) => None,
    }
}

/// Return the maximum stabbing depth in one partition using range-add/range-max updates.
fn sweep_max_depth<E: SweepEvents>(partition: &SweepPartition<E>) -> usize {
    let mut tree = RangeAddMaxTree::new(partition.inner_count);
    for interval in &partition.initial_active {
        // Convert the stored inclusive interval to the tree's half-open update convention.
        tree.add(interval[0] as usize, interval[1] as usize + 1, 1);
    }
    let mut max_depth = tree.max();
    let mut start_pos = 0usize;
    let mut end_pos = 0usize;
    let mut next_start = partition.events.start(start_pos);
    let mut next_end = partition.events.end(end_pos);

    while let Some(outer) = next_event_outer(next_start, next_end) {
        while let Some(event) = next_start {
            if event.outer != outer {
                break;
            }
            // Segment-tree updates are half-open, so add one to the inclusive maximum rank.
            tree.add(event.inner_min as usize, event.inner_max as usize + 1, 1);
            start_pos += 1;
            next_start = partition.events.start(start_pos);
        }
        // Starts precede ends so closed rectangles touching at `outer` overlap.
        max_depth = max_depth.max(tree.max());
        if max_depth == partition.max_depth {
            break;
        }
        while let Some(event) = next_end {
            if event.outer != outer {
                break;
            }
            // Remove the same inclusive interval via the tree's half-open update convention.
            tree.add(event.inner_min as usize, event.inner_max as usize + 1, -1);
            end_pos += 1;
            next_end = partition.events.end(end_pos);
        }
    }
    max_depth
}

/// Count intersecting rectangle pairs whose later start belongs to this partition.
fn sweep_overlap_pairs<E: SweepEvents>(partition: &SweepPartition<E>) -> u64 {
    let mut active = ActiveIntervals::new(partition.inner_count);
    for interval in &partition.initial_active {
        active.add(interval[0] as usize, interval[1] as usize, 1);
    }
    let mut overlap_pairs = 0u64;
    let mut start_pos = 0usize;
    let mut end_pos = 0usize;
    let mut next_start = partition.events.start(start_pos);
    let mut next_end = partition.events.end(end_pos);

    while let Some(outer) = next_event_outer(next_start, next_end) {
        while let Some(event) = next_start {
            if event.outer != outer {
                break;
            }
            let min = event.inner_min as usize;
            let max = event.inner_max as usize;
            overlap_pairs = overlap_pairs.saturating_add(
                (active.prefix_sum(max + 1, ActiveIntervals::MIN)
                    - active.prefix_sum(min, ActiveIntervals::MAX)) as u64,
            );
            active.add(min, max, 1);
            start_pos += 1;
            next_start = partition.events.start(start_pos);
        }
        // Initial intervals are not paired with each other here: every such pair was counted in
        // the partition where its later rectangle started.
        // `max_overlap_pairs` includes every pair this partition can own. Equality proves that all
        // such pairs intersect, so remaining end events cannot affect the final count.
        if overlap_pairs == partition.max_overlap_pairs {
            break;
        }
        while let Some(event) = next_end {
            if event.outer != outer {
                break;
            }
            active.add(event.inner_min as usize, event.inner_max as usize, -1);
            end_pos += 1;
            next_end = partition.events.end(end_pos);
        }
    }
    overlap_pairs
}

/// Globally ranked rectangles after choosing which dimension is the outer sweep axis.
/// `starts` and `ends` are ordered block IDs for that axis. `inner_count` sizes the global trees
/// used by the single-partition fallback.
struct RankedSweep {
    boxes: Vec<Rect<u32>>,
    starts: Vec<u32>,
    ends: Vec<u32>,
    outer_count: usize,
    inner_count: usize,
}

/// Reject caller-assigned ranges whose maximum sorts before their minimum on one axis.
fn validate_ranked_ranges(dimension: &RankedDimension, axis: char) -> Result<()> {
    for (block, [min, max]) in dimension.ranks.iter().copied().enumerate() {
        if min > max {
            return Err(ErrorCode::Internal(format!(
                "Hilbert clustering information block {block} has an {axis}-axis maximum endpoint before its minimum"
            )));
        }
    }
    Ok(())
}

/// Choose the cheaper sweep orientation and normalize it to outer/inner coordinates.
fn arrange_sweep(x: RankedDimension, y: RankedDimension) -> RankedSweep {
    let blocks = x.ranks.len() as u128;
    let x_cost = x.count as u128 + blocks * (y.count.max(1).ilog2() as u128 + 1);
    let y_cost = y.count as u128 + blocks * (x.count.max(1).ilog2() as u128 + 1);
    let use_x_outer = y_cost.saturating_mul(10) >= x_cost.saturating_mul(9);

    if use_x_outer {
        let boxes = x
            .ranks
            .into_iter()
            .zip(y.ranks)
            .map(|(x, y)| Rect {
                x_min: x[0],
                x_max: x[1],
                y_min: y[0],
                y_max: y[1],
            })
            .collect();
        RankedSweep {
            boxes,
            starts: x.starts,
            ends: x.ends,
            outer_count: x.count,
            inner_count: y.count,
        }
    } else {
        // Swapping physical dimensions changes only their generic outer/inner roles; rectangle
        // identity and closed-range semantics remain unchanged.
        let boxes = x
            .ranks
            .into_iter()
            .zip(y.ranks)
            .map(|(x, y)| Rect {
                x_min: y[0],
                x_max: y[1],
                y_min: x[0],
                y_max: x[1],
            })
            .collect();
        RankedSweep {
            boxes,
            starts: y.starts,
            ends: y.ends,
            outer_count: y.count,
            inner_count: x.count,
        }
    }
}

/// Pick quantile boundaries from ordered rectangle starts.
/// Duplicate start coordinates collapse naturally, so the actual partition count may be lower than
/// requested. Boundaries define half-open outer ranges except rectangle overlap remains closed.
fn partition_boundaries(
    boxes: &[Rect<u32>],
    starts: &[u32],
    outer_count: usize,
    requested_partitions: usize,
) -> Vec<u32> {
    debug_assert!(!boxes.is_empty() && !starts.is_empty());
    let requested_partitions = requested_partitions.max(1).min(starts.len());
    let mut boundaries = Vec::with_capacity(requested_partitions + 1);
    boundaries.push(boxes[starts[0] as usize].x_min);
    for partition in 1..requested_partitions {
        let position = starts.len() * partition / requested_partitions;
        let boundary = boxes[starts[position] as usize].x_min;
        // The smallest start was inserted before this loop, so the boundary list is non-empty.
        if boundary > *boundaries.last().expect("first boundary exists")
            && boundary < outer_count as u32
        {
            boundaries.push(boundary);
        }
    }
    // `outer_count` is one past the largest dense rank, so it forms an exclusive sentinel that no
    // rectangle endpoint can reach and closes the final half-open partition.
    boundaries.push(outer_count as u32);
    boundaries
}

/// Return the half-open partition containing `coordinate`; exact boundaries belong to the right.
/// Minima use this result as their sole start partition. Maxima use it as their inclusive last
/// partition, which replicates a boundary-ending rectangle to the right for closed-range contact.
/// `boundaries` must be strictly increasing and end above every rectangle endpoint.
fn partition_index(boundaries: &[u32], coordinate: u32) -> usize {
    boundaries[1..].partition_point(|boundary| *boundary <= coordinate)
}

/// A feasible partition layout plus exact capacities for each partition's member list.
struct SweepPlan {
    boundaries: Vec<u32>,
    relevant_counts: Vec<usize>,
}

/// Build a plan and reject it as soon as cross-boundary copies exceed the configured budget.
/// `boundaries` must contain at least one partition, start no later than any rectangle minimum, and
/// end above every rectangle maximum. `None` means this candidate is too expensive, not that
/// diagnostics cannot run; callers retain a narrower valid plan, with the single-partition plan as
/// the unconditional fallback.
fn build_sweep_plan(
    boxes: &[Rect<u32>],
    boundaries: Vec<u32>,
    extra_replica_limit: usize,
) -> Option<SweepPlan> {
    let partition_count = boundaries.len() - 1;
    let mut starts = vec![0usize; partition_count];
    let mut ends = vec![0usize; partition_count];
    let mut extra_replicas = 0usize;
    for item in boxes {
        let first = partition_index(&boundaries, item.x_min);
        let last = partition_index(&boundaries, item.x_max);
        starts[first] += 1;
        ends[last] += 1;
        // The first membership is required work; every additionally spanned partition is one
        // replica charged against the plan's CPU and memory budget.
        extra_replicas = extra_replicas.saturating_add(last - first);
        if extra_replicas > extra_replica_limit {
            return None;
        }
    }

    let mut active = 0usize;
    let mut relevant_counts = Vec::with_capacity(partition_count);
    for partition in 0..partition_count {
        active += starts[partition];
        relevant_counts.push(active);
        // Remove rectangles only after recording their last partition. A rectangle whose maximum
        // equals an internal boundary is therefore present on both sides; a rectangle starting at
        // that boundary belongs only to the right partition. Their closed-range contact is observed
        // together on the right without duplicating ownership of the newly started rectangle.
        active -= ends[partition];
    }
    debug_assert_eq!(active, 0);

    Some(SweepPlan {
        boundaries,
        relevant_counts,
    })
}

/// Select the widest useful plan allowed by thread count, input size, and replica budget.
/// `boxes` and its start-order permutation must be non-empty. The result is always executable: if
/// no multi-partition candidate is affordable, the function returns a single partition containing
/// every rectangle.
fn choose_sweep_plan(
    boxes: &[Rect<u32>],
    starts: &[u32],
    outer_count: usize,
    max_threads: usize,
) -> SweepPlan {
    let max_partitions = max_threads
        .clamp(1, MAX_SWEEP_PARTITIONS)
        .min((boxes.len() / MIN_RECTS_PER_SWEEP_PARTITION).max(1));
    // A single partition is always valid and provides the fallback for small or highly overlapping
    // inputs. Wider candidates replace it only when their actual deduplicated boundary count grows.
    let mut best = SweepPlan {
        boundaries: partition_boundaries(boxes, starts, outer_count, 1),
        relevant_counts: vec![boxes.len()],
    };

    let extra_replica_limit = boxes.len().saturating_mul(MAX_EXTRA_SWEEP_REPLICAS_PERCENT) / 100;
    // Powers of two keep each candidate's boundaries nested in the next one. Therefore replica
    // count is monotone, and the first over-budget candidate safely terminates the search.
    for requested in [2, 4, 8] {
        if requested > max_partitions {
            continue;
        }
        let Some(candidate) = build_sweep_plan(
            boxes,
            partition_boundaries(boxes, starts, outer_count, requested),
            extra_replica_limit,
        ) else {
            // Each wider candidate only adds boundaries, so replication is monotone.
            break;
        };
        if candidate.boundaries.len() > best.boundaries.len() {
            best = candidate;
        }
    }
    best
}

/// Translate a known coordinate into its dense partition-local rank.
fn local_rank(coordinates: &[u32], coordinate: u32) -> u32 {
    // Partition construction inserted every member endpoint before sorting and deduplication, so
    // this lower-bound lookup must find an exact match. Local distinct-coordinate count cannot
    // exceed the validated global endpoint count, making the u32 conversion lossless.
    let rank = coordinates.partition_point(|value| *value < coordinate);
    debug_assert_eq!(coordinates.get(rank), Some(&coordinate));
    rank as u32
}

/// Build one partition and recompress its inner coordinates to minimize tree memory and depth.
fn build_sweep_partition(
    boxes: &[Rect<u32>],
    members: Vec<u32>,
    outer_left: u32,
    outer_right: u32,
) -> SweepPartition<LocalEvents> {
    let max_depth = members.len();
    let mut coordinates = Vec::with_capacity(members.len().saturating_mul(2));
    for index in &members {
        let item = boxes[*index as usize];
        coordinates.push(item.y_min);
        coordinates.push(item.y_max);
    }
    coordinates.sort_unstable();
    coordinates.dedup();

    let mut initial_active = Vec::new();
    let mut starts = Vec::new();
    let mut ends = Vec::new();
    for index in members {
        let item = boxes[index as usize];
        let inner_min = local_rank(&coordinates, item.y_min);
        let inner_max = local_rank(&coordinates, item.y_max);
        // Intervals carried from the left have no local start event. A minimum equal to `outer_left`
        // remains a local start, allowing it to claim pairs with carried intervals that end exactly
        // on this boundary. Intervals reaching `outer_right` have no local end; they stay active
        // through this partition's final event and are carried into the next partition, which owns
        // evaluation at the half-open right boundary.
        if item.x_min < outer_left {
            initial_active.push([inner_min, inner_max]);
        } else {
            starts.push(SweepEvent {
                outer: item.x_min,
                inner_min,
                inner_max,
            });
        }
        if item.x_max < outer_right {
            ends.push(SweepEvent {
                outer: item.x_max,
                inner_min,
                inner_max,
            });
        }
    }
    ends.sort_unstable_by_key(|event| event.outer);
    // Only pairs involving a local start can be newly counted here: initial × starts plus all pairs
    // among starts. Reaching this bound means no later event can increase the result.
    let initial_count = initial_active.len() as u64;
    let start_count = starts.len() as u64;
    let max_overlap_pairs = initial_count
        .saturating_mul(start_count)
        .saturating_add(start_count.saturating_mul(start_count.saturating_sub(1)) / 2);

    SweepPartition {
        initial_active,
        events: LocalEvents { starts, ends },
        inner_count: coordinates.len(),
        max_depth,
        max_overlap_pairs,
    }
}

/// Execute one zero-copy global sweep or build and process local partitions in parallel.
/// Returns `(global_max_depth, unordered_overlap_pair_count)`. Partition-local pair counts are
/// additive because each pair belongs to the partition containing its later outer-axis start.
fn execute_partitioned_sweep(ranked: &RankedSweep, plan: SweepPlan) -> Result<(usize, u64)> {
    if plan.boundaries.len() == 2 {
        // Avoid member replication, event materialization, and local coordinate compression when
        // partitioning is not profitable. Both diagnostics still run concurrently on ranked data.
        // Endpoint-count validation bounds block count below 2^31, so n * (n - 1) / 2 fits u64.
        let max_pairs = ranked.boxes.len() as u64 * ranked.boxes.len().saturating_sub(1) as u64 / 2;
        let partition = SweepPartition {
            initial_active: Vec::new(),
            events: RankedEvents {
                boxes: &ranked.boxes,
                starts: &ranked.starts,
                ends: &ranked.ends,
            },
            inner_count: ranked.inner_count,
            max_depth: ranked.boxes.len(),
            max_overlap_pairs: max_pairs,
        };
        return Ok(rayon::join(
            || sweep_max_depth(&partition),
            || sweep_overlap_pairs(&partition),
        ));
    }

    let SweepPlan {
        boundaries,
        relevant_counts,
        ..
    } = plan;
    let mut members = relevant_counts
        .into_iter()
        .map(Vec::with_capacity)
        .collect::<Vec<_>>();
    // Iterate globally start-sorted IDs and append each rectangle to every intersected partition.
    // This preserves local start order; the rectangle emits a start only in its first partition and
    // is initially active thereafter, so each pair is owned where its later rectangle starts.
    for index in &ranked.starts {
        let item = ranked.boxes[*index as usize];
        let first = partition_index(&boundaries, item.x_min);
        let last = partition_index(&boundaries, item.x_max);
        for partition in &mut members[first..=last] {
            partition.push(*index);
        }
    }

    let result = {
        let partitions = members
            .into_par_iter()
            .enumerate()
            .map(|(partition, members)| {
                build_sweep_partition(
                    &ranked.boxes,
                    members,
                    boundaries[partition],
                    boundaries[partition + 1],
                )
            })
            .collect::<Vec<_>>();

        // Sweeps borrow immutable partition state, so depth and overlap diagnostics can traverse the
        // same partitions concurrently. Internal boundary points are evaluated by the right-hand
        // partition via its initial active set; global depth is therefore the maximum of disjoint
        // half-open partition results, while pair ownership remains disjoint and additive.
        rayon::join(
            || {
                partitions
                    .par_iter()
                    .map(sweep_max_depth)
                    .max()
                    .unwrap_or(0)
            },
            || {
                partitions
                    .par_iter()
                    .map(sweep_overlap_pairs)
                    // Saturate consistently with each local sweep rather than wrapping the public
                    // aggregate if a future wider input domain exceeds u64 pair counts.
                    .reduce(|| 0, u64::saturating_add)
            },
        )
    };
    Ok(result)
}

/// Exact partitioned diagnostics for two alternating min/max endpoint columns.
/// `builders[0]` and `builders[1]` are the two MBR dimensions. Each must contain rows in
/// `[block_0_min, block_0_max, block_1_min, block_1_max, ...]` order, with the corresponding type
/// at the same dimension index. Wrong dimension/type counts, unequal row counts, odd cardinality,
/// or a maximum sorting before its assigned minimum return `ErrorCode::Internal`.
pub(crate) fn hilbert_diagnostics(
    builders: Vec<ColumnBuilder>,
    key_types: &[DataType],
    max_threads: usize,
) -> Result<(usize, u64)> {
    let [x_builder, y_builder]: [ColumnBuilder; 2] =
        builders.try_into().map_err(|builders: Vec<_>| {
            ErrorCode::Internal(format!(
                "Hilbert clustering information requires two endpoint columns, got {}",
                builders.len()
            ))
        })?;
    if key_types.len() != 2 {
        return Err(ErrorCode::Internal(format!(
            "Hilbert clustering information requires two endpoint types, got {}",
            key_types.len()
        )));
    }

    endpoint_sort::with_request_pool(max_threads, move |threads| {
        let (x, y) = rayon::join(
            || endpoint_sort::sort_endpoints(vec![x_builder], &key_types[..1]),
            || endpoint_sort::sort_endpoints(vec![y_builder], &key_types[1..]),
        );
        let (x, y) = (x?, y?);
        if x.keys.len() != y.keys.len() {
            return Err(ErrorCode::Internal(format!(
                "Hilbert endpoint dimensions have different lengths: {} and {}",
                x.keys.len(),
                y.keys.len()
            )));
        }
        if x.keys.is_empty() {
            return Ok((0, 0));
        }

        let (x, y) = rayon::join(
            || rank_dimension(&x.keys, x.order),
            || rank_dimension(&y.keys, y.order),
        );
        validate_ranked_ranges(&x, 'x')?;
        validate_ranked_ranges(&y, 'y')?;
        let ranked = arrange_sweep(x, y);
        let plan = choose_sweep_plan(&ranked.boxes, &ranked.starts, ranked.outer_count, threads);
        execute_partitioned_sweep(&ranked, plan)
    })
}

#[cfg(test)]
mod tests {
    use databend_common_expression::Scalar;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::number::NumberScalar;

    use super::*;

    fn naive_diagnostics(rects: &[Rect<u8>]) -> (usize, u64) {
        let pairs = (0..rects.len())
            .flat_map(|left| ((left + 1)..rects.len()).map(move |right| (left, right)))
            .filter(|(left, right)| {
                let a = rects[*left];
                let b = rects[*right];
                a.x_min <= b.x_max && b.x_min <= a.x_max && a.y_min <= b.y_max && b.y_min <= a.y_max
            })
            .count() as u64;
        let depth = (0..=u8::MAX)
            .flat_map(|x| (0..=u8::MAX).map(move |y| (x, y)))
            .map(|(x, y)| {
                rects
                    .iter()
                    .filter(|rect| {
                        rect.x_min <= x && x <= rect.x_max && rect.y_min <= y && y <= rect.y_max
                    })
                    .count()
            })
            .max()
            .unwrap_or(0);
        (depth, pairs)
    }

    fn assert_matches_naive(rects: &[Rect<u8>]) {
        let ty = DataType::Number(NumberDataType::UInt8);
        let mut builders = [
            ColumnBuilder::with_capacity(&ty, rects.len() * 2),
            ColumnBuilder::with_capacity(&ty, rects.len() * 2),
        ];
        for rect in rects {
            for (builder, [min, max]) in builders
                .iter_mut()
                .zip([[rect.x_min, rect.x_max], [rect.y_min, rect.y_max]])
            {
                builder.push(Scalar::Number(NumberScalar::UInt8(min)).as_ref());
                builder.push(Scalar::Number(NumberScalar::UInt8(max)).as_ref());
            }
        }
        assert_eq!(
            naive_diagnostics(rects),
            hilbert_diagnostics(builders.into(), &[ty.clone(), ty], 8).unwrap()
        );
    }

    #[test]
    fn test_hilbert_public_contract() {
        let ty = DataType::Number(NumberDataType::Int32);
        let scalar = |value| Scalar::Number(NumberScalar::Int32(value));
        let mut builders = [
            ColumnBuilder::with_capacity(&ty, 4),
            ColumnBuilder::with_capacity(&ty, 4),
        ];
        for [x_min, x_max, y_min, y_max] in [[0, 1, 0, 1], [1, 2, 1, 2]] {
            for (builder, [min, max]) in builders.iter_mut().zip([[x_min, x_max], [y_min, y_max]]) {
                builder.push(scalar(min).as_ref());
                builder.push(scalar(max).as_ref());
            }
        }
        assert_eq!(
            hilbert_diagnostics(builders.into(), &[ty.clone(), ty.clone()], 4).unwrap(),
            (2, 1)
        );
        assert!(hilbert_diagnostics(Vec::new(), &[ty.clone(), ty.clone()], 1).is_err());

        let mut x = ColumnBuilder::with_capacity(&ty, 2);
        x.push(scalar(2).as_ref());
        x.push(scalar(1).as_ref());
        let mut y = ColumnBuilder::with_capacity(&ty, 2);
        y.push(scalar(0).as_ref());
        y.push(scalar(1).as_ref());
        assert!(hilbert_diagnostics(vec![x, y], &[ty.clone(), ty], 1).is_err());
    }

    #[test]
    fn test_hilbert_random_differential() {
        let mut state = 0x9e3779b97f4a7c15u64;
        let rects = (0..96)
            .map(|_| {
                state = state
                    .wrapping_add(0x9e3779b97f4a7c15)
                    .wrapping_mul(0xbf58476d1ce4e5b9);
                let x_min = (state % 224) as u8;
                let x_max = x_min + ((state >> 8) % 16) as u8;
                state ^= state >> 27;
                let y_min = (state % 224) as u8;
                let y_max = y_min + ((state >> 8) % 16) as u8;
                Rect {
                    x_min,
                    x_max,
                    y_min,
                    y_max,
                }
            })
            .collect::<Vec<_>>();
        assert_matches_naive(&rects);
        assert_matches_naive(&[
            Rect {
                x_min: 0,
                x_max: 1,
                y_min: 0,
                y_max: 1,
            },
            Rect {
                x_min: 1,
                x_max: 2,
                y_min: 1,
                y_max: 2,
            },
            Rect {
                x_min: 1,
                x_max: 1,
                y_min: 1,
                y_max: 1,
            },
        ]);
    }

    #[test]
    fn test_hilbert_adaptive_partition() {
        let ty = DataType::Number(NumberDataType::UInt32);
        let mut builders = [
            ColumnBuilder::with_capacity(&ty, 20_000),
            ColumnBuilder::with_capacity(&ty, 20_000),
        ];
        for coordinate in 0..10_000u32 {
            for builder in &mut builders {
                builder.push(Scalar::Number(NumberScalar::UInt32(coordinate)).as_ref());
                builder.push(Scalar::Number(NumberScalar::UInt32(coordinate + 1)).as_ref());
            }
        }
        assert_eq!(
            hilbert_diagnostics(builders.into(), &[ty.clone(), ty], 8).unwrap(),
            (2, 9_999)
        );
    }
}
