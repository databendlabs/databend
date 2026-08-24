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

use std::array::from_fn;
use std::collections::HashMap;
use std::iter::once;
use std::sync::Arc;

use databend_common_base::base::WatchNotify;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::LUT;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::TypedRangeBounds;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::UInt32Type;
use databend_common_pipeline::basic::Exchange;
use databend_storages_common_table_meta::table::HILBERT_CLUSTER_DIMENSIONS;
use parking_lot::Mutex;
use rand::SeedableRng;
use rand::rngs::SmallRng;
use rand::seq::SliceRandom;

// The coordinate uses 12 coarse bits and reserves the all-ones 16-bit value for NULL.
const MAX_COARSE_RANGES: usize = 4095;
// Four low bits allow each coarse bucket to expose at most 16 local quantile ranges.
const FINE_BITS: u32 = 4;
// One joint sample is reused for both dimensions, Hilbert exchange bounds, and skew detection.
// Keep the final plan bounded at 100K rows to control owned Scalar memory and plan-build CPU.
const MAX_PLAN_SAMPLES: usize = 100_000;
// Initial 2x sampling reduces rescans under moderate worker skew without tripling reservoir memory;
// workers that remain under-sampled are safely rescanned from the buffered input.
const SAMPLE_OVERSUBSCRIPTION: usize = 2;
// Empirical support threshold for local refinement. With a 100K sample and 4095 coarse ranges,
// uniform data has about 24 samples per bucket and therefore exposes about two fine ranges. This
// prevents individual samples from creating fine boundaries while retaining useful local detail.
const FINE_EFFECTIVE_SAMPLES_PER_RANGE: usize = 12;
#[derive(Debug)]
struct DimensionBounds {
    coarse: TypedRangeBounds,
    fine: TypedRangeBounds,
    fine_offsets: Vec<u32>,
    max_coarse_rank: u32,
}

impl DimensionBounds {
    fn coordinate(&self, value: ScalarRef<'_>) -> u16 {
        if matches!(value, ScalarRef::Null) {
            // Ordinary coordinates stop at 0xffef, leaving the top value as an independent NULL.
            return u16::MAX;
        }
        let coarse = self.coarse.lower_bound(value.clone());
        self.encode_ranks(value, coarse)
    }

    fn encode_ranks(&self, value: ScalarRef<'_>, coarse: u32) -> u16 {
        let range = self.fine_offsets[coarse as usize] as usize
            ..self.fine_offsets[coarse as usize + 1] as usize;
        let fine = self.fine.lower_bound_range(value, range.clone());
        // Stretch however many fine ranges this bucket supports across the low nibble.
        // Equal values share a bound rank, so this never invents order among duplicates.
        let fine = if range.is_empty() {
            0
        } else {
            (fine * ((1 << FINE_BITS) - 1) + range.len() as u32 / 2) / range.len() as u32
        };
        (self.scale(coarse) << FINE_BITS) | fine as u16
    }

    fn scale(&self, rank: u32) -> u16 {
        let coarse_rank = self.coarse.len() as u32;
        if coarse_rank == 0 {
            0
        } else {
            ((rank * self.max_coarse_rank + coarse_rank / 2) / coarse_rank) as u16
        }
    }

    fn encode_column(&self, entry: &BlockEntry) -> Vec<u16> {
        self.coarse
            .lower_bound_column(entry, u16::MAX as u32)
            .into_iter()
            .enumerate()
            .map(|(row, coarse)| {
                if coarse == u16::MAX as u32 {
                    u16::MAX
                } else {
                    // SAFETY: the rank vector has exactly one entry per input row.
                    self.encode_ranks(unsafe { entry.index_unchecked(row) }, coarse)
                }
            })
            .collect()
    }
}

pub(super) type HilbertSample = [Scalar; HILBERT_CLUSTER_DIMENSIONS];

struct LocalSketch {
    rows: usize,
    samples: Vec<HilbertSample>,
}

#[derive(Debug)]
struct HotKeyRange {
    first_owner: usize,
    last_owner: usize,
    start: f64,
    span: f64,
}

impl HotKeyRange {
    fn owner(&self, salt: u64) -> usize {
        let unit = (salt >> 11) as f64 * (1.0 / (1_u64 << 53) as f64);
        (self.first_owner + (self.start + self.span * unit).floor() as usize).min(self.last_owner)
    }
}

#[derive(Debug)]
struct HilbertRangePlan {
    dimensions: [DimensionBounds; HILBERT_CLUSTER_DIMENSIONS],
    exchange_bounds: Vec<u32>,
    hot_keys: HashMap<u32, HotKeyRange>,
}

struct HilbertRangeState {
    sketches: Vec<Option<LocalSketch>>,
    sample_targets: Vec<usize>,
    pending_resamples: usize,
    building_plan: bool,
    plan: Option<Arc<HilbertRangePlan>>,
    error: Option<ErrorCode>,
}

/// Task-local weighted samples and the immutable plan derived from them.
pub struct HilbertRangeExchange {
    collector_count: usize,
    target_sample_size: usize,
    local_sample_size: usize,
    dimension_offsets: [usize; HILBERT_CLUSTER_DIMENSIONS],
    inner: Mutex<HilbertRangeState>,
    sketches_ready: WatchNotify,
    plan_ready: WatchNotify,
}

impl HilbertRangeExchange {
    pub(super) fn local_sample_size(&self) -> usize {
        self.local_sample_size
    }

    pub(super) fn dimension_offsets(&self) -> [usize; HILBERT_CLUSTER_DIMENSIONS] {
        self.dimension_offsets
    }

    /// Publish a task-wide error so barrier waiters wake up instead of hanging.
    pub(super) fn fail(&self, error: ErrorCode) {
        {
            let mut inner = self.inner.lock();
            if inner.error.is_none() {
                inner.error = Some(error);
            }
        }
        self.sketches_ready.notify_waiters();
        self.plan_ready.notify_waiters();
    }

    pub fn create(
        dimension_offsets: [usize; HILBERT_CLUSTER_DIMENSIONS],
        task_rows: usize,
        worker_count: usize,
        collector_count: usize,
    ) -> Arc<Self> {
        let worker_count = worker_count.max(1);
        let collector_count = collector_count.max(1);
        let target_samples = task_rows.min(MAX_PLAN_SAMPLES);
        let local_sample_size = target_samples
            .saturating_mul(SAMPLE_OVERSUBSCRIPTION)
            .div_ceil(worker_count)
            .min(task_rows)
            .max(1);
        Arc::new(Self {
            collector_count,
            target_sample_size: target_samples,
            local_sample_size,
            dimension_offsets,
            inner: Mutex::new(HilbertRangeState {
                sketches: (0..worker_count).map(|_| None).collect(),
                sample_targets: vec![0; worker_count],
                pending_resamples: 0,
                building_plan: false,
                plan: None,
                error: None,
            }),
            sketches_ready: WatchNotify::new(),
            plan_ready: WatchNotify::new(),
        })
    }

    pub(super) fn submit_initial(
        &self,
        worker_id: usize,
        rows: usize,
        samples: Vec<HilbertSample>,
    ) {
        let sketch = LocalSketch { rows, samples };
        let sketches = {
            let mut inner = self.inner.lock();
            inner.sketches[worker_id] = Some(sketch);
            if inner.sketches.iter().any(Option::is_none) {
                return;
            }
            inner
                .sketches
                .iter()
                .map(|sketch| {
                    let sketch = sketch.as_ref().unwrap();
                    (sketch.rows, sketch.samples.len())
                })
                .collect::<Vec<_>>()
        };

        let total_rows = sketches.iter().map(|(rows, _)| *rows).sum::<usize>();
        // The final quota is a strict task-wide cap. Since every nonempty worker owns at least one
        // input row, target_sample_size is sufficient to preserve one sample for each of them.
        let target_samples = total_rows.min(self.target_sample_size);
        let mut sample_targets = vec![0; sketches.len()];
        let mut assigned = 0;
        let mut cumulative_rows = 0;
        // Cumulative rounding keeps the quotas proportional and makes their sum exact.
        for (worker_id, (rows, _)) in sketches.iter().copied().enumerate() {
            cumulative_rows += rows;
            let cumulative_target = ((target_samples as u128 * cumulative_rows as u128)
                / total_rows.max(1) as u128) as usize;
            sample_targets[worker_id] = cumulative_target - assigned;
            assigned = cumulative_target;
        }
        // Preserve representation for every nonempty worker without changing the exact budget.
        for worker_id in 0..sketches.len() {
            if sketches[worker_id].0 > 0 && sample_targets[worker_id] == 0 {
                let donor = sample_targets
                    .iter()
                    .enumerate()
                    .filter(|(_, target)| **target > 1)
                    .max_by_key(|(_, target)| **target)
                    .map(|(worker_id, _)| worker_id)
                    .expect("sample budget can represent every nonempty Hilbert worker");
                sample_targets[donor] -= 1;
                sample_targets[worker_id] = 1;
            }
        }

        let mut inner = self.inner.lock();
        for (worker_id, (_, sample_count)) in sketches.into_iter().enumerate() {
            let desired = sample_targets[worker_id];
            inner.sample_targets[worker_id] = desired;
            if desired > sample_count {
                inner.pending_resamples += 1;
            }
        }
        drop(inner);
        self.sketches_ready.notify_waiters();
    }

    pub(super) fn should_build_plan(&self) -> bool {
        let inner = self.inner.lock();
        inner.pending_resamples == 0
            && !inner.building_plan
            && inner.plan.is_none()
            && inner.error.is_none()
    }

    pub(super) fn resample_request(&self, worker_id: usize) -> Option<usize> {
        let inner = self.inner.lock();
        let target = inner.sample_targets[worker_id];
        (inner.sketches[worker_id]
            .as_ref()
            .is_some_and(|sketch| target > sketch.samples.len()))
        .then_some(target)
    }

    pub(super) fn complete_resample(
        &self,
        worker_id: usize,
        rows: usize,
        samples: Vec<HilbertSample>,
    ) {
        let mut inner = self.inner.lock();
        inner.sketches[worker_id] = Some(LocalSketch { rows, samples });
        inner.pending_resamples -= 1;
    }

    pub(super) fn check_error(&self) -> Result<()> {
        match &self.inner.lock().error {
            Some(error) => Err(error.clone()),
            None => Ok(()),
        }
    }

    pub(super) async fn wait_sketches(&self) -> Result<()> {
        if !self.sketches_ready.has_notified() {
            self.sketches_ready.notified().await;
        }
        self.check_error()
    }

    pub(super) async fn wait_plan(&self) -> Result<bool> {
        if !self.plan_ready.has_notified() {
            self.plan_ready.notified().await;
        }
        let inner = self.inner.lock();
        if let Some(error) = &inner.error {
            return Err(error.clone());
        }
        Ok(!inner
            .plan
            .as_ref()
            .ok_or_else(|| ErrorCode::Internal("Hilbert range plan was not published"))?
            .hot_keys
            .is_empty())
    }

    pub(super) fn publish_plan(&self) {
        // Claim ownership and move sketches out under the lock; sorting the bounded task-wide
        // sample must not block other processors from observing the task state.
        let sketches = {
            let mut inner = self.inner.lock();
            if inner.pending_resamples != 0
                || inner.building_plan
                || inner.plan.is_some()
                || inner.error.is_some()
            {
                return;
            }
            inner.building_plan = true;
            let worker_count = inner.sketches.len();
            (0..worker_count)
                .map(|worker_id| {
                    (
                        inner.sketches[worker_id]
                            .take()
                            .expect("every Hilbert worker submitted a sketch"),
                        inner.sample_targets[worker_id],
                        worker_id,
                    )
                })
                .collect::<Vec<_>>()
        };
        // Light workers are uniformly downsampled to their final quota. Heavy workers were
        // rescanned from the spill buffer with a reservoir sized exactly to their final quota.
        let sketches = sketches
            .into_iter()
            .map(|(mut sketch, target, worker_id)| {
                let mut rng =
                    SmallRng::seed_from_u64(mix64(worker_id as u64 ^ 0xd1b5_4a32_d192_ed03));
                sketch.samples.shuffle(&mut rng);
                sketch.samples.truncate(target);
                sketch
            })
            .collect::<Vec<_>>();
        let result = build_plan(sketches, self.collector_count);
        let mut inner = self.inner.lock();
        inner.building_plan = false;
        match result {
            Ok(plan) => inner.plan = Some(Arc::new(plan)),
            Err(error) => inner.error = Some(error),
        }
        drop(inner);
        self.plan_ready.notify_waiters();
    }
}

/// Adds a task-local Hilbert key and routes each row by weighted range bounds.
impl Exchange for HilbertRangeExchange {
    const NAME: &'static str = "HilbertRange";
    const SKIP_EMPTY_DATA_BLOCK: bool = true;

    fn partition(&self, mut data: DataBlock, n: usize) -> Result<Vec<DataBlock>> {
        debug_assert!(n > 0 && n <= u8::MAX as usize + 1);
        let plan = self
            .inner
            .lock()
            .plan
            .clone()
            .expect("Hilbert range plan must be ready before routing rows");
        let dimensions = self
            .dimension_offsets
            .map(|offset| data.get_by_offset(offset));
        let x = plan.dimensions[0].encode_column(dimensions[0]);
        let y = plan.dimensions[1].encode_column(dimensions[1]);
        let routing_salt =
            (!plan.hot_keys.is_empty()).then(|| data.get_by_offset(data.num_columns() - 1).clone());
        let mut values = Vec::with_capacity(data.num_rows());
        let mut owners = Vec::with_capacity(data.num_rows());
        for row in 0..data.num_rows() {
            let value = hilbert_value(x[row], y[row], u16::BITS);
            let owner = if let Some(hot) = plan.hot_keys.get(&value) {
                let salt = match routing_salt.as_ref().and_then(|entry| entry.index(row)) {
                    Some(ScalarRef::Number(NumberScalar::UInt64(salt))) => salt,
                    _ => return Err(ErrorCode::Internal("Hilbert routing salt must be UInt64")),
                };
                hot.owner(salt)
            } else {
                plan.exchange_bounds.partition_point(|bound| bound < &value)
            };
            values.push(value);
            owners.push(owner.min(n - 1) as u8);
        }
        if routing_salt.is_some() {
            data.pop_columns(1);
        }
        data.add_column(UInt32Type::from_data(values));
        data.scatter(&owners, n)
    }
}

fn build_plan(sketches: Vec<LocalSketch>, collector_count: usize) -> Result<HilbertRangePlan> {
    let mut samples = Vec::new();
    for sketch in sketches {
        let sample_count = sketch.samples.len();
        let weight = sketch.rows as f64 / sample_count.max(1) as f64;
        samples.extend(
            sketch
                .samples
                .into_iter()
                .map(|coordinates| (coordinates, weight)),
        );
    }
    if samples.is_empty() {
        return Err(ErrorCode::Internal("Hilbert recluster sampled no rows"));
    }

    let mut dimensions = from_fn(|dimension| {
        let mut values = samples
            .iter()
            .filter(|(coordinates, _)| !matches!(coordinates[dimension], Scalar::Null))
            .map(|(coordinates, weight)| (&coordinates[dimension], *weight))
            .collect::<Vec<_>>();
        values.sort_unstable_by(|(left, _), (right, _)| left.as_ref().cmp(&right.as_ref()));

        let coarse_range_limit = MAX_COARSE_RANGES.min(values.len());
        let coarse = weighted_quantile_bounds(&values, coarse_range_limit);
        // Fine bounds are globally sorted because each coarse slice is ordered and disjoint.
        // Offsets select the small per-range sub-slice without allocating one object per range.
        let mut fine = Vec::new();
        let mut fine_offsets = Vec::with_capacity(coarse.len() + 2);
        fine_offsets.push(0);
        let mut start = 0;
        for bound in coarse.iter().map(Some).chain(once(None)) {
            let end = bound.map_or(values.len(), |bound| {
                values.partition_point(|(value, _)| value.as_ref() <= bound.as_ref())
            });
            // Fine precision is enabled only when supported by independent weighted observations;
            // distinct values and the 4-bit coordinate capacity provide hard upper bounds.
            let bucket = &values[start..end];
            let fine_range_limit = fine_range_count(bucket);
            fine.extend(weighted_quantile_bounds(bucket, fine_range_limit));
            fine_offsets.push(fine.len() as u32);
            start = end;
        }

        DimensionBounds {
            coarse: TypedRangeBounds::from_scalars(coarse),
            fine: TypedRangeBounds::from_scalars(fine),
            fine_offsets,
            max_coarse_rank: 0,
        }
    });
    let max_coarse_rank = dimensions
        .iter()
        .map(|dimension| dimension.coarse.len())
        .max()
        .unwrap() as u32;
    // Stretch a low-cardinality dimension across the same coarse domain as the other dimension.
    // This changes scale only; it does not manufacture additional values or fine bounds.
    for dimension in &mut dimensions {
        dimension.max_coarse_rank = max_coarse_rank;
    }

    let mut weighted_keys = samples
        .into_iter()
        .map(|(sample, weight)| {
            let coordinates: [u16; HILBERT_CLUSTER_DIMENSIONS] =
                from_fn(|dimension| dimensions[dimension].coordinate(sample[dimension].as_ref()));
            (
                hilbert_value(coordinates[0], coordinates[1], u16::BITS),
                weight,
            )
        })
        .collect::<Vec<_>>();
    weighted_keys.sort_unstable_by_key(|(key, _)| *key);
    let (exchange_bounds, hot_keys) = weighted_exchange_bounds(&weighted_keys, collector_count);

    Ok(HilbertRangePlan {
        dimensions,
        exchange_bounds,
        hot_keys,
    })
}

fn fine_range_count(values: &[(&Scalar, f64)]) -> usize {
    if values.is_empty() {
        return 1;
    }

    let (weight_sum, squared_weight_sum) = values.iter().fold(
        (0.0, 0.0),
        |(weight_sum, squared_weight_sum), (_, weight)| {
            (weight_sum + weight, squared_weight_sum + weight * weight)
        },
    );
    // Kish ESS discounts unequal sample weights: a few heavy observations must not unlock the
    // same fine precision as many independent, equally weighted observations.
    let effective_samples = if squared_weight_sum == 0.0 {
        0
    } else {
        (weight_sum * weight_sum / squared_weight_sum).floor() as usize
    };
    let distinct_values = 1 + values
        .windows(2)
        .filter(|pair| pair[0].0.as_ref() < pair[1].0.as_ref())
        .count();

    (effective_samples / FINE_EFFECTIVE_SAMPLES_PER_RANGE)
        .clamp(1, 1 << FINE_BITS)
        .min(distinct_values)
}

fn weighted_quantile_bounds(values: &[(&Scalar, f64)], range_limit: usize) -> Vec<Scalar> {
    // Match Spark RangePartitioner::determineBounds: add each candidate's represented weight,
    // select the current value when the target is reached, and never emit duplicate boundaries.
    if values.is_empty() || range_limit <= 1 {
        return Vec::new();
    }
    let step = values.iter().map(|(_, weight)| *weight).sum::<f64>() / range_limit as f64;
    let mut target = step;
    let mut accumulated = 0.0;
    let mut bounds = Vec::with_capacity(range_limit - 1);
    for (value, weight) in values {
        accumulated += weight;
        if bounds.len() + 1 < range_limit
            && accumulated >= target
            && bounds
                .last()
                .is_none_or(|previous: &Scalar| previous.as_ref() < value.as_ref())
        {
            bounds.push((*value).clone());
            target += step;
        }
    }
    bounds
}

fn weighted_exchange_bounds(
    weighted_keys: &[(u32, f64)],
    collector_count: usize,
) -> (Vec<u32>, HashMap<u32, HotKeyRange>) {
    if collector_count <= 1 {
        return (Vec::new(), HashMap::new());
    }
    let total_weight = weighted_keys.iter().map(|(_, weight)| *weight).sum::<f64>();
    let target_weight = total_weight / collector_count as f64;
    let mut bounds = Vec::with_capacity(collector_count - 1);
    let mut hot_keys = HashMap::new();
    let mut accumulated = 0.0;
    let mut index = 0;
    while index < weighted_keys.len() {
        let key = weighted_keys[index].0;
        let mut key_weight = 0.0;
        while index < weighted_keys.len() && weighted_keys[index].0 == key {
            key_weight += weighted_keys[index].1;
            index += 1;
        }
        let scaled_start = accumulated / target_weight;
        let first_owner = scaled_start.floor().min((collector_count - 1) as f64) as usize;
        accumulated += key_weight;
        let last_owner = ((accumulated / target_weight).ceil() as usize)
            .saturating_sub(1)
            .min(collector_count - 1);
        if last_owner > first_owner {
            // Delta appends a secondary noise key before range repartitioning. Here stable salt is
            // needed only when one sampled Hilbert key actually spans multiple output ranges.
            hot_keys.insert(key, HotKeyRange {
                first_owner,
                last_owner,
                start: scaled_start - first_owner as f64,
                span: key_weight / target_weight,
            });
        }
        while bounds.len() < collector_count - 1
            && accumulated >= target_weight * (bounds.len() + 1) as f64
        {
            bounds.push(key);
        }
    }
    (bounds, hot_keys)
}

fn hilbert_value(x: u16, y: u16, coordinate_bits: u32) -> u32 {
    let states = LUT[0];
    let mut state = 0_usize;
    let mut key = 0_u32;
    for bit in (0..coordinate_bits).rev() {
        let point = ((((x >> bit) & 1) << 1) | ((y >> bit) & 1)) as usize;
        let transition = states[state * 4 + point];
        key = (key << 2) | (transition >> 8) as u32;
        state = (transition & 0xff) as usize;
    }
    key
}

pub(super) fn mix64(mut value: u64) -> u64 {
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use databend_common_expression::BlockEntry;
    use databend_common_expression::FromData;
    use databend_common_expression::types::Int32Type;
    use databend_common_expression::types::NumberScalar;
    use databend_common_expression::types::UInt64Type;

    use super::*;

    fn int(value: i32) -> Scalar {
        Scalar::Number(NumberScalar::Int32(value))
    }

    fn local_sketch(rows: usize, samples: Vec<HilbertSample>) -> LocalSketch {
        LocalSketch { rows, samples }
    }

    #[test]
    fn test_sample_budget_and_worker_quotas() {
        let sampling_exchange = HilbertRangeExchange::create([0, 1], 1_000_000, 4, 2);
        assert_eq!(sampling_exchange.target_sample_size, 100_000);
        assert_eq!(sampling_exchange.local_sample_size, 50_000);

        let exchange = HilbertRangeExchange::create([0, 1], 1_000_000, 2, 2);
        exchange.submit_initial(0, 900_000, vec![[int(0), int(0)]; 100]);
        exchange.submit_initial(1, 100_000, vec![[int(1), int(1)]; 100]);
        assert_eq!(exchange.resample_request(0), Some(90_000));
        assert_eq!(exchange.resample_request(1), Some(10_000));
        assert_eq!(
            exchange.inner.lock().sample_targets.iter().sum::<usize>(),
            100_000
        );

        let exchange = HilbertRangeExchange::create([0, 1], 100_002, 3, 1);
        for worker_id in 0..3 {
            exchange.submit_initial(worker_id, [1, 1, 100_000][worker_id], vec![[
                int(worker_id as i32),
                int(0),
            ]]);
        }
        assert_eq!(exchange.inner.lock().sample_targets, vec![1, 1, 99_998]);
    }

    #[test]
    fn test_plan_bounds_and_encoding() {
        let samples = (0..4096)
            .map(|value| [int(value % 32), int(value)])
            .collect::<Vec<_>>();
        let plan = build_plan(vec![local_sketch(samples.len(), samples)], 4).unwrap();
        let low_cardinality = &plan.dimensions[0];
        assert_eq!(low_cardinality.coarse.len(), 32);
        assert!(low_cardinality.coordinate(int(31).as_ref()) > 60_000);

        let weighted = build_plan(
            vec![
                local_sketch(900, vec![[int(0), int(0)]; 10]),
                local_sketch(100, vec![[int(100), int(100)]; 10]),
            ],
            2,
        )
        .unwrap();
        let dimension = &weighted.dimensions[0];
        assert!(dimension.coordinate(int(0).as_ref()) < dimension.coordinate(int(50).as_ref()));
        assert_eq!(
            dimension.coordinate(int(50).as_ref()),
            dimension.coordinate(int(100).as_ref())
        );

        let all_null =
            build_plan(vec![local_sketch(1, vec![[Scalar::Null, Scalar::Null]])], 1).unwrap();
        assert_eq!(all_null.dimensions[0].coordinate(ScalarRef::Null), u16::MAX);

        let dimension = DimensionBounds {
            coarse: TypedRangeBounds::from_scalars(vec![int(10), int(20), int(30)]),
            fine: TypedRangeBounds::from_scalars(vec![int(5), int(25)]),
            fine_offsets: vec![0, 1, 1, 2, 2],
            max_coarse_rank: 3,
        };
        let entry: BlockEntry =
            Int32Type::from_opt_data(vec![Some(5), Some(10), None, Some(25), Some(40)]).into();
        assert_eq!(dimension.encode_column(&entry), vec![
            0,
            15,
            u16::MAX,
            32,
            48
        ]);
    }

    #[test]
    fn test_fine_ranges_follow_effective_samples() {
        let values = (0..24).map(int).collect::<Vec<_>>();
        assert_eq!(
            fine_range_count(&values.iter().map(|value| (value, 1.0)).collect::<Vec<_>>()),
            2
        );

        let values = (0..192).map(int).collect::<Vec<_>>();
        assert_eq!(
            fine_range_count(&values.iter().map(|value| (value, 1.0)).collect::<Vec<_>>()),
            16
        );
        assert_eq!(
            fine_range_count(
                &values
                    .iter()
                    .enumerate()
                    .map(|(index, value)| (value, if index == 0 { 10_000.0 } else { 1.0 }))
                    .collect::<Vec<_>>()
            ),
            1
        );

        let duplicate = int(1);
        assert_eq!(
            fine_range_count(&(0..192).map(|_| (&duplicate, 1.0)).collect::<Vec<_>>()),
            1
        );
    }

    fn empty_plan(hot_keys: HashMap<u32, HotKeyRange>) -> Arc<HilbertRangePlan> {
        Arc::new(HilbertRangePlan {
            dimensions: from_fn(|_| DimensionBounds {
                coarse: TypedRangeBounds::from_scalars(Vec::new()),
                fine: TypedRangeBounds::from_scalars(Vec::new()),
                fine_offsets: vec![0, 0],
                max_coarse_rank: 0,
            }),
            exchange_bounds: vec![0],
            hot_keys,
        })
    }

    #[test]
    fn test_exchange_uses_salt_only_for_hot_keys() -> Result<()> {
        let exchange = HilbertRangeExchange::create([0, 1], 2, 1, 2);
        exchange.inner.lock().plan = Some(empty_plan(HashMap::new()));
        let output = exchange.partition(
            DataBlock::new_from_columns(vec![
                Int32Type::from_data(vec![1, 2]),
                Int32Type::from_data(vec![3, 4]),
            ]),
            2,
        )?;
        assert!(output.iter().all(|block| block.num_columns() == 3));

        let exchange = HilbertRangeExchange::create([0, 1], 4, 1, 2);
        exchange.inner.lock().plan = Some(empty_plan(HashMap::from([(0, HotKeyRange {
            first_owner: 0,
            last_owner: 1,
            start: 0.0,
            span: 2.0,
        })])));
        let output = exchange.partition(
            DataBlock::new_from_columns(vec![
                Int32Type::from_data(vec![1, 1, 1, 1]),
                Int32Type::from_data(vec![1, 1, 1, 1]),
                UInt64Type::from_data(vec![0, 1_u64 << 62, 1_u64 << 63, 3_u64 << 62]),
            ]),
            2,
        )?;
        assert_eq!(
            output.iter().map(DataBlock::num_rows).collect::<Vec<_>>(),
            vec![2, 2]
        );
        assert!(output.iter().all(|block| block.num_columns() == 3));
        Ok(())
    }

    #[test]
    fn test_weighted_exchange_bounds_and_hot_keys() {
        let (_, hot) = weighted_exchange_bounds(&vec![(7, 1.0); 100], 4);
        assert_eq!(hot[&7].last_owner - hot[&7].first_owner + 1, 4);

        let (bounds, hot) = weighted_exchange_bounds(&[(1, 1.0), (2, 1.0), (3, 1.0), (4, 1.0)], 2);
        assert_eq!(bounds, vec![2]);
        assert!(hot.is_empty());

        let mut keys = vec![(1, 1.0); 49];
        keys.extend(vec![(2, 1.0); 2]);
        keys.extend(vec![(3, 1.0); 49]);
        let (bounds, hot) = weighted_exchange_bounds(&keys, 2);
        assert_eq!(bounds, vec![2]);
        assert!(hot.contains_key(&2));
    }
}
