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

use std::any::Any;
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
use databend_common_expression::sampler::FixedSizeIndexSampler;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::UInt32Type;
use databend_common_expression::types::UInt64Type;
use databend_common_pipeline::basic::Exchange;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_sql::HILBERT_CLUSTER_DIMENSIONS;
use parking_lot::Mutex;
use rand::SeedableRng;
use rand::rngs::SmallRng;
use rand::seq::SliceRandom;

// Keep the original 12-bit coarse precision; the remaining four coordinate bits
// are filled by ordering-based quantiles only when a coarse range has enough samples.
const MAX_COARSE_RANGES: usize = 4095;
const FINE_BITS: u32 = 4;
const MIN_SAMPLES_PER_FINE_RANGE: usize = 8;
// Cap the single-threaded plan build while retaining far more than Spark's default
// 100 samples per output partition for Databend's at-most-256 collectors.
const MAX_PLAN_SAMPLES: usize = 100_000;
const MIN_HOT_KEY_TARGET_FRACTION: f64 = 0.1;
// Oversampling absorbs uneven worker input before sketches are normalized to MAX_PLAN_SAMPLES.
const LOCAL_SAMPLE_OVERSUBSCRIPTION: usize = 3;

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
        let coarse = self.coarse.upper_bound(value.clone());
        self.encode_ranks(value, coarse)
    }

    fn encode_ranks(&self, value: ScalarRef<'_>, coarse: u32) -> u16 {
        let range = self.fine_offsets[coarse as usize] as usize
            ..self.fine_offsets[coarse as usize + 1] as usize;
        let fine = self.fine.upper_bound_range(value, range.clone());
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
            .upper_bound_column(entry, u16::MAX as u32)
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

struct WeightedSample {
    coordinates: [Scalar; HILBERT_CLUSTER_DIMENSIONS],
    weight: f64,
}

struct LocalSketch {
    rows: usize,
    samples: Vec<[Scalar; HILBERT_CLUSTER_DIMENSIONS]>,
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

struct HilbertRangeStateInner {
    sketches: Vec<Option<LocalSketch>>,
    sample_targets: Vec<usize>,
    pending_resamples: usize,
    building_plan: bool,
    plan: Option<Arc<HilbertRangePlan>>,
    error: Option<ErrorCode>,
}

/// Task-local weighted samples and the immutable plan derived from them.
pub struct HilbertRangeState {
    worker_count: usize,
    collector_count: usize,
    local_sample_size: usize,
    dimension_offsets: [usize; HILBERT_CLUSTER_DIMENSIONS],
    inner: Mutex<HilbertRangeStateInner>,
    sketches_ready: WatchNotify,
    plan_ready: WatchNotify,
}

impl HilbertRangeState {
    pub fn create(
        dimension_offsets: [usize; HILBERT_CLUSTER_DIMENSIONS],
        task_rows: usize,
        worker_count: usize,
        collector_count: usize,
    ) -> Arc<Self> {
        let worker_count = worker_count.max(1);
        let target_samples = task_rows.min(MAX_PLAN_SAMPLES);
        let local_sample_size = target_samples
            .saturating_mul(LOCAL_SAMPLE_OVERSUBSCRIPTION)
            .div_ceil(worker_count)
            .min(target_samples)
            .max(1);
        Arc::new(Self {
            worker_count,
            collector_count: collector_count.max(1),
            local_sample_size,
            dimension_offsets,
            inner: Mutex::new(HilbertRangeStateInner {
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

    fn submit_initial(&self, worker_id: usize, sketch: LocalSketch) {
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
        let nonempty_workers = sketches.iter().filter(|(rows, _)| *rows > 0).count();
        // Every nonempty worker needs at least one sample so its row weight is represented.
        let target_samples = total_rows.min(MAX_PLAN_SAMPLES.max(nonempty_workers));
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

    fn should_build_plan(&self, worker_id: usize) -> bool {
        let inner = self.inner.lock();
        worker_id == 0
            && inner.pending_resamples == 0
            && !inner.building_plan
            && inner.plan.is_none()
            && inner.error.is_none()
    }

    fn resample_request(&self, worker_id: usize) -> Option<usize> {
        let inner = self.inner.lock();
        let target = inner.sample_targets[worker_id];
        (inner.sketches[worker_id]
            .as_ref()
            .is_some_and(|sketch| target > sketch.samples.len()))
        .then_some(target)
    }

    fn publish_plan(&self) {
        // Claim ownership and move sketches out under the lock; sorting up to 100K samples must
        // not block other processors from observing the task state.
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
            (0..self.worker_count)
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
        // Light workers already oversampled; randomly shrink their reservoirs to the final quota.
        // Heavy workers were rescanned to exactly their quota before reaching this point.
        let sketches = sketches
            .into_iter()
            .map(|(mut sketch, target, worker_id)| {
                if sketch.samples.len() > target {
                    sketch.samples.shuffle(&mut SmallRng::seed_from_u64(mix64(
                        worker_id as u64 ^ 0xd1b5_4a32_d192_ed03,
                    )));
                    sketch.samples.truncate(target);
                }
                sketch
            })
            .collect();
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

#[derive(Clone, Copy)]
enum HilbertWorkerPhase {
    Sampling,
    WaitSketches,
    Resample,
    WaitPlan,
    Replay { add_routing_salt: bool },
}

/// Samples one input stream, waits for all streams, then replays its buffered blocks.
pub struct TransformHilbertCluster {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    state: Arc<HilbertRangeState>,
    worker_id: usize,
    sampler: Option<FixedSizeIndexSampler<SmallRng>>,
    pending_blocks: Vec<DataBlock>,
    output_data: Option<DataBlock>,
    next_replay_row: u64,
    phase: HilbertWorkerPhase,
}

impl TransformHilbertCluster {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        state: Arc<HilbertRangeState>,
        worker_id: usize,
    ) -> Box<dyn Processor> {
        Box::new(Self {
            input,
            output,
            sampler: Some(FixedSizeIndexSampler::new(
                state.local_sample_size,
                SmallRng::seed_from_u64(mix64(worker_id as u64)),
            )),
            state,
            worker_id,
            pending_blocks: Vec::new(),
            output_data: None,
            next_replay_row: 0,
            phase: HilbertWorkerPhase::Sampling,
        })
    }

    fn sampled_coordinates(
        &self,
        sampler: &FixedSizeIndexSampler<SmallRng>,
    ) -> Vec<[Scalar; HILBERT_CLUSTER_DIMENSIONS]> {
        sampler
            .indices()
            .iter()
            .map(|&(block, row)| {
                let block = &self.pending_blocks[block as usize];
                self.state.dimension_offsets.map(|offset| {
                    // SAFETY: sampler positions originate from this block.
                    unsafe { block.get_by_offset(offset).index_unchecked(row as usize) }.to_owned()
                })
            })
            .collect()
    }
}

#[async_trait::async_trait]
impl Processor for TransformHilbertCluster {
    fn name(&self) -> String {
        "TransformHilbertCluster".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.output.is_finished() {
            self.input.finish();
            return Ok(Event::Finished);
        }
        if !self.output.can_push() {
            self.input.set_not_need_data();
            return Ok(Event::NeedConsume);
        }
        if let Some(block) = self.output_data.take() {
            self.output.push_data(Ok(block));
            return Ok(Event::NeedConsume);
        }
        match self.phase {
            HilbertWorkerPhase::Sampling => {
                if self.input.has_data() {
                    let block = self.input.pull_data().unwrap()?;
                    if !block.is_empty() {
                        let block_idx = self.pending_blocks.len() as u32;
                        self.sampler
                            .as_mut()
                            .unwrap()
                            .add_block(block.num_rows(), block_idx);
                        self.pending_blocks.push(block);
                    }
                    // `pull_data` clears the port's NEED_DATA flag. Re-arm it before returning
                    // `NeedData`, otherwise both this processor and its upstream become idle.
                    self.input.set_need_data();
                    return Ok(Event::NeedData);
                }
                if self.input.is_finished() {
                    let sampler = self
                        .sampler
                        .take()
                        .expect("Hilbert sampler is submitted exactly once");
                    let sketch = LocalSketch {
                        rows: sampler.rows_seen(),
                        samples: self.sampled_coordinates(&sampler),
                    };
                    self.state.submit_initial(self.worker_id, sketch);
                    self.phase = HilbertWorkerPhase::WaitSketches;
                    return Ok(Event::Async);
                }
                self.input.set_need_data();
                Ok(Event::NeedData)
            }
            HilbertWorkerPhase::WaitSketches => Ok(Event::Async),
            HilbertWorkerPhase::WaitPlan => {
                if self.state.should_build_plan(self.worker_id) {
                    Ok(Event::Sync)
                } else {
                    Ok(Event::Async)
                }
            }
            HilbertWorkerPhase::Resample => Ok(Event::Sync),
            HilbertWorkerPhase::Replay { .. } => {
                if self.pending_blocks.is_empty() {
                    self.output.finish();
                    Ok(Event::Finished)
                } else {
                    Ok(Event::Sync)
                }
            }
        }
    }

    fn process(&mut self) -> Result<()> {
        match self.phase {
            HilbertWorkerPhase::Resample => {
                let sample_size = self
                    .state
                    .resample_request(self.worker_id)
                    .expect("resample phase requires a larger worker quota");
                let mut sampler = FixedSizeIndexSampler::new(
                    sample_size,
                    SmallRng::seed_from_u64(mix64(self.worker_id as u64 ^ 0x9e37_79b9)),
                );
                for (block_idx, block) in self.pending_blocks.iter().enumerate() {
                    sampler.add_block(block.num_rows(), block_idx as u32);
                }
                let sketch = LocalSketch {
                    rows: sampler.rows_seen(),
                    samples: self.sampled_coordinates(&sampler),
                };
                let publish = {
                    let mut inner = self.state.inner.lock();
                    inner.sketches[self.worker_id] = Some(sketch);
                    inner.pending_resamples -= 1;
                    inner.pending_resamples == 0
                };
                self.phase = HilbertWorkerPhase::WaitPlan;
                if publish {
                    self.state.publish_plan();
                }
            }
            HilbertWorkerPhase::WaitPlan => {
                if self.state.should_build_plan(self.worker_id) {
                    self.state.publish_plan();
                }
            }
            HilbertWorkerPhase::Replay { add_routing_salt } => {
                if let Some(mut block) = self.pending_blocks.pop() {
                    if add_routing_salt {
                        let start = self.next_replay_row;
                        self.next_replay_row += block.num_rows() as u64;
                        let worker = (self.worker_id as u64) << 48;
                        block.add_column(UInt64Type::from_data(
                            (0..block.num_rows())
                                .map(|row| mix64(worker ^ (start + row as u64)))
                                .collect(),
                        ));
                    }
                    self.output_data = Some(block);
                }
            }
            HilbertWorkerPhase::Sampling | HilbertWorkerPhase::WaitSketches => {
                return Err(ErrorCode::Internal(
                    "Hilbert processor received a sync event in an asynchronous phase",
                ));
            }
        }
        Ok(())
    }

    async fn async_process(&mut self) -> Result<()> {
        match self.phase {
            HilbertWorkerPhase::WaitSketches => {
                if !self.state.sketches_ready.has_notified() {
                    self.state.sketches_ready.notified().await;
                }
                self.phase = if self.state.resample_request(self.worker_id).is_some() {
                    HilbertWorkerPhase::Resample
                } else {
                    HilbertWorkerPhase::WaitPlan
                };
            }
            HilbertWorkerPhase::WaitPlan => {
                if !self.state.plan_ready.has_notified() {
                    self.state.plan_ready.notified().await;
                }
                let inner = self.state.inner.lock();
                if let Some(error) = &inner.error {
                    return Err(error.clone());
                }
                let add_routing_salt = !inner
                    .plan
                    .as_ref()
                    .ok_or_else(|| ErrorCode::Internal("Hilbert range plan was not published"))?
                    .hot_keys
                    .is_empty();
                drop(inner);
                // `process` uses `pop` to release one block at a time; reverse once so replay and
                // its deterministic routing salt still follow this worker's original input order.
                self.pending_blocks.reverse();
                self.phase = HilbertWorkerPhase::Replay { add_routing_salt };
            }
            HilbertWorkerPhase::Sampling
            | HilbertWorkerPhase::Resample
            | HilbertWorkerPhase::Replay { .. } => {
                return Err(ErrorCode::Internal(
                    "Hilbert processor received an async event outside a barrier phase",
                ));
            }
        }
        Ok(())
    }
}

/// Adds a task-local Hilbert key and routes each row by weighted range bounds.
pub struct HilbertRangeExchange {
    state: Arc<HilbertRangeState>,
}

impl HilbertRangeExchange {
    pub fn create(state: Arc<HilbertRangeState>) -> Arc<Self> {
        Arc::new(Self { state })
    }
}

impl Exchange for HilbertRangeExchange {
    const NAME: &'static str = "HilbertRange";
    const SKIP_EMPTY_DATA_BLOCK: bool = true;

    fn partition(&self, mut data: DataBlock, n: usize) -> Result<Vec<DataBlock>> {
        debug_assert!(n > 0 && n <= u8::MAX as usize + 1);
        let plan = self
            .state
            .inner
            .lock()
            .plan
            .clone()
            .expect("Hilbert range plan must be ready before routing rows");
        let dimensions = self
            .state
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
    let samples = sketches
        .into_iter()
        .flat_map(|sketch| {
            let weight = sketch.rows as f64 / sketch.samples.len().max(1) as f64;
            sketch
                .samples
                .into_iter()
                .map(move |coordinates| WeightedSample {
                    coordinates,
                    weight,
                })
        })
        .collect::<Vec<_>>();
    if samples.is_empty() {
        return Err(ErrorCode::Internal("Hilbert recluster sampled no rows"));
    }

    let mut dimensions = from_fn(|dimension| {
        let mut values = samples
            .iter()
            .filter(|sample| !matches!(sample.coordinates[dimension], Scalar::Null))
            .map(|sample| (&sample.coordinates[dimension], sample.weight))
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
                values.partition_point(|(value, _)| value.as_ref() < bound.as_ref())
            });
            // Sparse buckets keep their coarse coordinate; denser buckets gain up to four bits.
            let fine_range_limit = (1 << FINE_BITS)
                .min((end - start) / MIN_SAMPLES_PER_FINE_RANGE)
                .max(1);
            fine.extend(weighted_quantile_bounds(
                &values[start..end],
                fine_range_limit,
            ));
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
        .map(|sample| {
            let coordinates: [u16; HILBERT_CLUSTER_DIMENSIONS] = from_fn(|dimension| {
                dimensions[dimension].coordinate(sample.coordinates[dimension].as_ref())
            });
            (
                hilbert_value(coordinates[0], coordinates[1], u16::BITS),
                sample.weight,
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

fn weighted_quantile_bounds(values: &[(&Scalar, f64)], range_limit: usize) -> Vec<Scalar> {
    // Select boundaries by represented row weight, not by the number of retained samples.
    // Duplicate values are consumed as one run so no artificial boundary splits equal keys.
    if values.is_empty() || range_limit <= 1 {
        return Vec::new();
    }
    let step = values.iter().map(|(_, weight)| *weight).sum::<f64>() / range_limit as f64;
    let mut target = step;
    let mut accumulated = 0.0;
    let mut bounds = Vec::with_capacity(range_limit - 1);
    let mut index = 0;
    while index < values.len() {
        let value = values[index].0;
        while bounds.len() + 1 < range_limit && accumulated >= target {
            if bounds
                .last()
                .is_none_or(|previous: &Scalar| previous.as_ref() < value.as_ref())
            {
                bounds.push(value.clone());
            }
            target += step;
        }
        while index < values.len() && values[index].0.as_ref() == value.as_ref() {
            accumulated += values[index].1;
            index += 1;
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
        if last_owner > first_owner && key_weight >= target_weight * MIN_HOT_KEY_TARGET_FRACTION {
            // A repeated key crosses one or more target collector boundaries. Record its exact
            // fractional span so stable salt fills partial edge collectors proportionally.
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

fn mix64(mut value: u64) -> u64 {
    value = (value ^ (value >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    value = (value ^ (value >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    value ^ (value >> 31)
}

#[cfg(test)]
mod tests {
    use databend_common_expression::BlockEntry;
    use databend_common_expression::Column;
    use databend_common_expression::FromData;
    use databend_common_expression::types::Int32Type;
    use databend_common_expression::types::NumberColumn;
    use databend_common_expression::types::NumberScalar;
    use databend_common_expression::types::StringType;
    use databend_common_expression::types::UInt64Type;
    use databend_common_pipeline::core::port::connect;

    use super::*;

    fn int(value: i32) -> Scalar {
        Scalar::Number(NumberScalar::Int32(value))
    }

    #[test]
    fn test_sampling_rearms_input_after_each_block() -> Result<()> {
        let input = InputPort::create();
        let upstream = OutputPort::create();
        let output = OutputPort::create();
        let downstream = InputPort::create();
        // SAFETY: each pair of ports is connected exactly once before use.
        unsafe {
            connect(&input, &upstream);
            connect(&downstream, &output);
        }
        downstream.set_need_data();

        let state = HilbertRangeState::create([0, 1], 4, 1, 1);
        let mut processor = TransformHilbertCluster::create(input, output, state, 0);

        assert!(matches!(processor.event()?, Event::NeedData));
        assert!(upstream.can_push());

        upstream.push_data(Ok(DataBlock::new_from_columns(vec![
            Int32Type::from_data(vec![1, 2]),
            Int32Type::from_data(vec![3, 4]),
        ])));
        assert!(matches!(processor.event()?, Event::NeedData));
        assert!(upstream.can_push());

        // Empty blocks must re-arm the input as well.
        upstream.push_data(Ok(DataBlock::empty()));
        assert!(matches!(processor.event()?, Event::NeedData));
        assert!(upstream.can_push());
        Ok(())
    }

    #[test]
    fn test_low_cardinality_dimension_is_stretched() {
        let samples = (0..4096)
            .map(|value| WeightedSample {
                coordinates: [int(value % 32), int(value)],
                weight: 1.0,
            })
            .collect::<Vec<_>>();
        let plan = build_plan(
            vec![LocalSketch {
                rows: samples.len(),
                samples: samples
                    .iter()
                    .map(|sample| sample.coordinates.clone())
                    .collect(),
            }],
            4,
        )
        .unwrap();
        let low = &plan.dimensions[0];
        assert_eq!(low.coarse.len(), 31);
        assert_eq!(low.coordinate(int(0).as_ref()), 0);
        assert!(low.coordinate(int(31).as_ref()) > 60_000);

        let single = build_plan(
            vec![LocalSketch {
                rows: 1,
                samples: vec![[int(7), int(9)]],
            }],
            1,
        )
        .unwrap();
        assert!(single.dimensions[0].coarse.is_empty());

        let all_null = build_plan(
            vec![LocalSketch {
                rows: 1,
                samples: vec![[Scalar::Null, Scalar::Null]],
            }],
            1,
        )
        .unwrap();
        assert_eq!(all_null.dimensions[0].coordinate(ScalarRef::Null), u16::MAX);
    }

    #[test]
    fn test_weighted_bounds_follow_rows_not_sample_count() {
        let sketches = vec![
            LocalSketch {
                rows: 900,
                samples: vec![[int(0), int(0)]; 10],
            },
            LocalSketch {
                rows: 100,
                samples: vec![[int(100), int(100)]; 10],
            },
        ];
        let plan = build_plan(sketches, 2).unwrap();
        assert_eq!(plan.dimensions[0].coordinate(int(50).as_ref()), 0);
    }

    #[test]
    fn test_resample_request_follows_worker_weight() {
        let state = HilbertRangeState::create([0, 1], 1_000_000, 2, 2);
        state.submit_initial(0, LocalSketch {
            rows: 900_000,
            samples: vec![[int(0), int(0)]; 100],
        });
        state.submit_initial(1, LocalSketch {
            rows: 100_000,
            samples: vec![[int(1), int(1)]; 100],
        });
        assert_eq!(state.resample_request(0), Some(90_000));
        assert_eq!(state.resample_request(1), Some(10_000));
        assert_eq!(
            state.inner.lock().sample_targets.iter().sum::<usize>(),
            100_000
        );

        let state = HilbertRangeState::create([0, 1], 100_002, 3, 1);
        for worker_id in 0..3 {
            state.submit_initial(worker_id, LocalSketch {
                rows: [1, 1, 100_000][worker_id],
                samples: vec![[int(worker_id as i32), int(0)]],
            });
        }
        assert_eq!(state.inner.lock().sample_targets, vec![1, 1, 99_998]);
    }

    #[test]
    fn test_plan_is_published_once() {
        let state = HilbertRangeState::create([0, 1], 1, 1, 1);
        state.submit_initial(0, LocalSketch {
            rows: 1,
            samples: vec![[int(1), int(1)]],
        });
        state.publish_plan();
        let first = state.inner.lock().plan.clone().unwrap();
        state.publish_plan();
        let second = state.inner.lock().plan.clone().unwrap();
        assert!(Arc::ptr_eq(&first, &second));
    }

    #[test]
    fn test_typed_encoder_matches_scalar_fallback_and_nulls() {
        let dimension = DimensionBounds {
            coarse: TypedRangeBounds::from_scalars(vec![int(10), int(20), int(30)]),
            fine: TypedRangeBounds::from_scalars(vec![int(5), int(25)]),
            fine_offsets: vec![0, 1, 1, 2, 2],
            max_coarse_rank: 3,
        };
        let entry: BlockEntry =
            Int32Type::from_opt_data(vec![Some(5), Some(10), None, Some(25), Some(40)]).into();
        let encoded = dimension.encode_column(&entry);
        let expected = (0..5)
            .map(|row| dimension.coordinate(entry.index(row).unwrap()))
            .collect::<Vec<_>>();
        assert_eq!(encoded, expected);
        assert_eq!(encoded, vec![15, 16, u16::MAX, 47, 48]);

        let string = |value: &str| Scalar::String(value.to_string());
        let dimension = DimensionBounds {
            coarse: TypedRangeBounds::from_scalars(vec![string("d")]),
            fine: TypedRangeBounds::from_scalars(vec![string("b"), string("f")]),
            fine_offsets: vec![0, 1, 2],
            max_coarse_rank: 1,
        };
        let entry: BlockEntry = StringType::from_data(vec!["a", "b", "c", "d", "f", "g"]).into();
        let encoded = dimension.encode_column(&entry);
        assert_eq!(encoded, vec![0, 15, 15, 16, 31, 31]);

        let generated = build_plan(
            vec![LocalSketch {
                rows: 65_536,
                samples: (0..65_536).map(|value| [int(value), int(value)]).collect(),
            }],
            1,
        )
        .unwrap();
        let dimension = &generated.dimensions[0];
        assert_eq!(dimension.fine_offsets.len(), dimension.coarse.len() + 2);
        assert!(!dimension.fine.is_empty());
        assert!(
            (0..65_535)
                .step_by(257)
                .all(|value| dimension.coordinate(int(value).as_ref())
                    <= dimension.coordinate(int(value + 1).as_ref()))
        );
    }

    #[test]
    fn test_exchange_uses_salt_only_for_hot_keys() -> Result<()> {
        let state = HilbertRangeState::create([0, 1], 2, 1, 2);
        state.inner.lock().plan = Some(Arc::new(HilbertRangePlan {
            dimensions: from_fn(|_| DimensionBounds {
                coarse: TypedRangeBounds::from_scalars(Vec::new()),
                fine: TypedRangeBounds::from_scalars(Vec::new()),
                fine_offsets: vec![0, 0],
                max_coarse_rank: 0,
            }),
            exchange_bounds: vec![0],
            hot_keys: HashMap::new(),
        }));
        let input = DataBlock::new_from_columns(vec![
            Int32Type::from_data(vec![1, 2]),
            Int32Type::from_data(vec![3, 4]),
        ]);
        let output = HilbertRangeExchange::create(state).partition(input, 2)?;
        assert!(output.iter().all(|block| block.num_columns() == 3));
        assert!(output.iter().all(|block| matches!(
            block.get_by_offset(1),
            BlockEntry::Column(Column::Number(NumberColumn::Int32(_)))
        )));
        assert!(output.iter().all(|block| matches!(
            block.get_by_offset(2),
            BlockEntry::Column(Column::Number(NumberColumn::UInt32(_)))
        )));

        let state = HilbertRangeState::create([0, 1], 4, 1, 2);
        state.inner.lock().plan = Some(Arc::new(HilbertRangePlan {
            dimensions: from_fn(|_| DimensionBounds {
                coarse: TypedRangeBounds::from_scalars(Vec::new()),
                fine: TypedRangeBounds::from_scalars(Vec::new()),
                fine_offsets: vec![0, 0],
                max_coarse_rank: 0,
            }),
            exchange_bounds: vec![0],
            hot_keys: HashMap::from([(0, HotKeyRange {
                first_owner: 0,
                last_owner: 1,
                start: 0.0,
                span: 2.0,
            })]),
        }));
        let input = DataBlock::new_from_columns(vec![
            Int32Type::from_data(vec![1, 1, 1, 1]),
            Int32Type::from_data(vec![1, 1, 1, 1]),
            UInt64Type::from_data(vec![0, 1_u64 << 62, 1_u64 << 63, 3_u64 << 62]),
        ]);
        let output = HilbertRangeExchange::create(state).partition(input, 2)?;
        assert_eq!(
            output.iter().map(DataBlock::num_rows).collect::<Vec<_>>(),
            vec![2, 2]
        );
        assert!(output.iter().all(|block| block.num_columns() == 3));
        assert!(output.iter().all(|block| matches!(
            block.get_by_offset(2),
            BlockEntry::Column(Column::Number(NumberColumn::UInt32(_)))
        )));
        Ok(())
    }

    #[test]
    fn test_weighted_exchange_bounds_and_hot_keys() {
        let keys = vec![(7, 1.0); 100];
        let (_, hot) = weighted_exchange_bounds(&keys, 4);
        assert_eq!(hot[&7].last_owner - hot[&7].first_owner + 1, 4);

        let keys = vec![(1, 1.0), (2, 1.0), (3, 1.0), (4, 1.0)];
        let (bounds, hot) = weighted_exchange_bounds(&keys, 2);
        assert_eq!(bounds, vec![2]);
        assert!(hot.is_empty());
        assert_eq!(bounds.partition_point(|bound| bound < &2), 0);
        assert_eq!(bounds.partition_point(|bound| bound < &3), 1);

        let mut keys = vec![(1, 1.0); 50];
        keys.extend(vec![(2, 1.0); 50]);
        let (_, hot) = weighted_exchange_bounds(&keys, 4);
        let range = &hot[&2];
        assert!(range.last_owner < 4);

        let mut keys = vec![(1, 1.0); 49];
        keys.extend(vec![(2, 1.0); 2]);
        keys.extend(vec![(3, 1.0); 49]);
        let (bounds, hot) = weighted_exchange_bounds(&keys, 2);
        assert_eq!(bounds, vec![2]);
        assert!(!hot.contains_key(&2));
    }
}
