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
use databend_common_expression::types::DataType;
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
    data_type: Option<DataType>,
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
        let coarse_ranges = self.coarse.len() as u32;
        let coarse = if coarse_ranges == 0 {
            0
        } else {
            ((coarse * self.max_coarse_rank + coarse_ranges / 2) / coarse_ranges) as u16
        };
        (coarse << FINE_BITS) | fine as u16
    }

    fn encode_column(&self, entry: &BlockEntry) -> Result<Vec<u16>> {
        if let Some(expected) = &self.data_type {
            let scalar_null = matches!(entry, BlockEntry::Const(Scalar::Null, _, _));
            let actual = entry.data_type().remove_nullable();
            if !scalar_null && actual != *expected {
                return Err(ErrorCode::Internal(format!(
                    "Hilbert dimension type changed from {expected} to {actual}"
                )));
            }
        }
        self.coarse
            .lower_bound_column(entry, u16::MAX as u32)
            .into_iter()
            .enumerate()
            .map(|(row, coarse)| {
                if coarse == u16::MAX as u32 {
                    Ok(u16::MAX)
                } else {
                    let value = entry.index(row).ok_or_else(|| {
                        ErrorCode::Internal(
                            "Hilbert dimension rank exceeded the input column length",
                        )
                    })?;
                    Ok(self.encode_ranks(value, coarse))
                }
            })
            .collect()
    }
}

pub(super) type HilbertSample = [Scalar; HILBERT_CLUSTER_DIMENSIONS];
type LocalSketch = (usize, Vec<HilbertSample>);

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

enum HilbertRangeState {
    Collecting(Vec<Option<LocalSketch>>),
    Resampling {
        sketches: Vec<LocalSketch>,
        sample_targets: Vec<usize>,
    },
    Building,
    Ready(Arc<HilbertRangePlan>),
    Failed(ErrorCode),
}

impl HilbertRangeState {
    fn can_build(&self) -> bool {
        let Self::Resampling {
            sketches,
            sample_targets,
        } = self
        else {
            return false;
        };
        sketches
            .iter()
            .zip(sample_targets)
            .all(|((_, samples), target)| samples.len() >= *target)
    }
}

fn validate_sketch(rows: usize, sample_count: usize) -> Result<()> {
    if rows == 0 && sample_count != 0 {
        return Err(ErrorCode::Internal(
            "Hilbert empty worker submitted nonempty samples",
        ));
    }
    if rows > 0 && sample_count == 0 {
        return Err(ErrorCode::Internal(
            "Hilbert nonempty worker submitted no samples",
        ));
    }
    if sample_count > rows {
        return Err(ErrorCode::Internal(
            "Hilbert worker submitted more samples than input rows",
        ));
    }
    Ok(())
}

fn allocate_sample_targets(rows: &[usize], target_sample_size: usize) -> Result<Vec<usize>> {
    let total_rows = rows.iter().try_fold(0usize, |total, rows| {
        total.checked_add(*rows).ok_or_else(|| {
            ErrorCode::Internal("Hilbert worker row counts overflowed the task row count")
        })
    })?;
    let target_samples = total_rows.min(target_sample_size);
    let nonempty_workers = rows.iter().filter(|rows| **rows > 0).count();
    if target_samples < nonempty_workers {
        return Err(ErrorCode::Internal(
            "Hilbert sample budget cannot represent every nonempty worker",
        ));
    }

    let mut sample_targets = vec![0; rows.len()];
    let mut assigned = 0;
    let mut cumulative_rows = 0;
    // Cumulative rounding keeps the quotas proportional and makes their sum exact.
    for (worker_id, rows) in rows.iter().copied().enumerate() {
        cumulative_rows += rows;
        let cumulative_target = ((target_samples as u128 * cumulative_rows as u128)
            / total_rows.max(1) as u128) as usize;
        sample_targets[worker_id] = cumulative_target - assigned;
        assigned = cumulative_target;
    }
    // Preserve representation for every nonempty worker without changing the exact budget.
    for worker_id in 0..rows.len() {
        if rows[worker_id] > 0 && sample_targets[worker_id] == 0 {
            let donor = sample_targets
                .iter()
                .enumerate()
                .filter(|(_, target)| **target > 1)
                .max_by_key(|(_, target)| **target)
                .map(|(worker_id, _)| worker_id)
                .ok_or_else(|| {
                    ErrorCode::Internal(
                        "Hilbert sample budget cannot represent every nonempty worker",
                    )
                })?;
            sample_targets[donor] -= 1;
            sample_targets[worker_id] = 1;
        }
    }
    Ok(sample_targets)
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
    pub(super) fn fail(&self, error: ErrorCode) -> ErrorCode {
        let error = {
            let mut inner = self.inner.lock();
            match &*inner {
                HilbertRangeState::Failed(error) => error.clone(),
                _ => {
                    *inner = HilbertRangeState::Failed(error.clone());
                    error
                }
            }
        };
        self.sketches_ready.notify_waiters();
        self.plan_ready.notify_waiters();
        error
    }

    /// A worker that disappears before the shared plan is ready must release its peers from the
    /// barrier. Once the plan is ready, downstream cancellation is ordinary replay shutdown.
    pub(super) fn cancel_before_plan(&self) {
        let cancelled = {
            let mut inner = self.inner.lock();
            if matches!(
                &*inner,
                HilbertRangeState::Collecting(_)
                    | HilbertRangeState::Resampling { .. }
                    | HilbertRangeState::Building
            ) {
                *inner = HilbertRangeState::Failed(ErrorCode::AbortedQuery(
                    "Hilbert recluster cancelled before its range plan was ready",
                ));
                true
            } else {
                false
            }
        };
        if cancelled {
            self.sketches_ready.notify_waiters();
            self.plan_ready.notify_waiters();
        }
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
            inner: Mutex::new(HilbertRangeState::Collecting(
                (0..worker_count).map(|_| None).collect(),
            )),
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
        let mut inner = self.inner.lock();
        let HilbertRangeState::Collecting(sketches) = &mut *inner else {
            return;
        };
        let Some(sketch) = sketches.get_mut(worker_id) else {
            drop(inner);
            self.fail(ErrorCode::Internal(format!(
                "Hilbert worker {worker_id} is outside the sampling exchange"
            )));
            return;
        };
        if sketch.is_some() {
            drop(inner);
            self.fail(ErrorCode::Internal(format!(
                "Hilbert worker {worker_id} submitted its initial sample twice"
            )));
            return;
        }
        if let Err(error) = validate_sketch(rows, samples.len()) {
            drop(inner);
            self.fail(error);
            return;
        }
        *sketch = Some((rows, samples));
        if sketches.iter().any(Option::is_none) {
            return;
        }

        let rows = sketches
            .iter()
            .flatten()
            .map(|(rows, _)| *rows)
            .collect::<Vec<_>>();
        // The final quota is a strict task-wide cap. Production task sizing provides at least one
        // sample for every nonempty worker; reject inconsistent metadata instead of panicking.
        let sample_targets = match allocate_sample_targets(&rows, self.target_sample_size) {
            Ok(sample_targets) => sample_targets,
            Err(error) => {
                drop(inner);
                self.fail(error);
                return;
            }
        };

        let Some(sketches) = std::mem::take(sketches)
            .into_iter()
            .collect::<Option<Vec<_>>>()
        else {
            drop(inner);
            self.fail(ErrorCode::Internal(
                "Hilbert initial samples became incomplete while allocating quotas",
            ));
            return;
        };
        *inner = HilbertRangeState::Resampling {
            sketches,
            sample_targets,
        };
        drop(inner);
        self.sketches_ready.notify_waiters();
    }

    pub(super) fn should_build_plan(&self) -> bool {
        self.inner.lock().can_build()
    }

    pub(super) fn resample_request(&self, worker_id: usize) -> Result<Option<usize>> {
        let inner = self.inner.lock();
        let HilbertRangeState::Resampling {
            sketches,
            sample_targets,
        } = &*inner
        else {
            return Ok(None);
        };
        let Some(((_, samples), target)) =
            sketches.get(worker_id).zip(sample_targets.get(worker_id))
        else {
            return Err(ErrorCode::Internal(format!(
                "Hilbert worker {worker_id} is outside the resampling exchange"
            )));
        };
        Ok((*target > samples.len()).then_some(*target))
    }

    pub(super) fn complete_resample(
        &self,
        worker_id: usize,
        rows: usize,
        samples: Vec<HilbertSample>,
    ) -> Result<()> {
        let mut inner = self.inner.lock();
        let HilbertRangeState::Resampling {
            sketches,
            sample_targets,
        } = &mut *inner
        else {
            return Ok(());
        };
        let Some((sketch, target)) = sketches
            .get_mut(worker_id)
            .zip(sample_targets.get(worker_id))
        else {
            return Err(ErrorCode::Internal(format!(
                "Hilbert worker {worker_id} is outside the resampling exchange"
            )));
        };
        if rows != sketch.0 {
            return Err(ErrorCode::Internal(
                "Hilbert worker row count changed during resampling",
            ));
        }
        validate_sketch(rows, samples.len())?;
        if samples.len() != *target {
            return Err(ErrorCode::Internal(
                "Hilbert worker did not fulfill its resample quota",
            ));
        }
        *sketch = (rows, samples);
        Ok(())
    }

    pub(super) fn check_error(&self) -> Result<()> {
        match &*self.inner.lock() {
            HilbertRangeState::Failed(error) => Err(error.clone()),
            _ => Ok(()),
        }
    }

    pub(super) async fn wait_sketches(&self) -> Result<()> {
        if !self.sketches_ready.has_notified() {
            self.sketches_ready.notified().await;
        }
        self.check_error()
    }

    fn ready_plan(&self) -> Result<Arc<HilbertRangePlan>> {
        match &*self.inner.lock() {
            HilbertRangeState::Ready(plan) => Ok(plan.clone()),
            HilbertRangeState::Failed(error) => Err(error.clone()),
            HilbertRangeState::Collecting(_)
            | HilbertRangeState::Resampling { .. }
            | HilbertRangeState::Building => {
                Err(ErrorCode::Internal("Hilbert range plan is not ready"))
            }
        }
    }

    pub(super) async fn wait_plan(&self) -> Result<bool> {
        if !self.plan_ready.has_notified() {
            self.plan_ready.notified().await;
        }
        Ok(!self.ready_plan()?.hot_keys.is_empty())
    }

    pub(super) fn publish_plan(&self) {
        // Claim ownership and move sketches out under the lock; sorting the bounded task-wide
        // sample must not block other processors from observing the task state.
        let sketches = {
            let mut inner = self.inner.lock();
            if !inner.can_build() {
                return;
            }
            let previous = std::mem::replace(&mut *inner, HilbertRangeState::Building);
            let HilbertRangeState::Resampling {
                sketches,
                sample_targets,
            } = previous
            else {
                *inner = previous;
                return;
            };
            sketches
                .into_iter()
                .zip(sample_targets)
                .enumerate()
                .map(|(worker_id, (sketch, target))| (sketch, target, worker_id))
                .collect::<Vec<_>>()
        };
        // Light workers are uniformly downsampled to their final quota. Heavy workers were
        // rescanned from the spill buffer with a reservoir sized exactly to their final quota.
        let sketches = sketches
            .into_iter()
            .map(|((rows, mut samples), target, worker_id)| {
                let mut rng =
                    SmallRng::seed_from_u64(mix64(worker_id as u64 ^ 0xd1b5_4a32_d192_ed03));
                samples.shuffle(&mut rng);
                samples.truncate(target);
                (rows, samples)
            })
            .collect::<Vec<_>>();
        let result = build_plan(sketches, self.collector_count);
        let mut inner = self.inner.lock();
        // Preserve a task-wide failure published while the sample was sorted outside the lock.
        if matches!(&*inner, HilbertRangeState::Building) {
            *inner = match result {
                Ok(plan) => HilbertRangeState::Ready(Arc::new(plan)),
                Err(error) => HilbertRangeState::Failed(error),
            };
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
        let result = (|| {
            if n == 0 || n > u8::MAX as usize + 1 {
                return Err(ErrorCode::Internal(
                    "Hilbert partition count must be between 1 and 256",
                ));
            }
            if n != self.collector_count {
                return Err(ErrorCode::Internal(format!(
                    "Hilbert partition count {n} does not match planned collector count {}",
                    self.collector_count
                )));
            }
            let plan = self.ready_plan()?;
            let dimensions = self.dimension_offsets.map(|offset| {
                data.columns().get(offset).ok_or_else(|| {
                    ErrorCode::Internal(format!(
                        "Hilbert dimension offset {offset} is outside the routed block"
                    ))
                })
            });
            let [x, y] = dimensions;
            let dimensions = [x?, y?];
            if dimensions
                .iter()
                .any(|dimension| dimension.len() != data.num_rows())
            {
                return Err(ErrorCode::Internal(
                    "Hilbert dimension column length does not match the routed block",
                ));
            }
            let x = plan.dimensions[0].encode_column(dimensions[0])?;
            let y = plan.dimensions[1].encode_column(dimensions[1])?;
            if x.len() != data.num_rows() || y.len() != data.num_rows() {
                return Err(ErrorCode::Internal(
                    "Hilbert encoded dimension length does not match the routed block",
                ));
            }
            let routing_salt = if plan.hot_keys.is_empty() {
                None
            } else {
                let salt =
                    data.columns().last().cloned().ok_or_else(|| {
                        ErrorCode::Internal("Hilbert routing salt column is missing")
                    })?;
                if salt.len() != data.num_rows() {
                    return Err(ErrorCode::Internal(
                        "Hilbert routing salt length does not match the routed block",
                    ));
                }
                Some(salt)
            };
            let mut values = Vec::with_capacity(data.num_rows());
            let mut owners = Vec::with_capacity(data.num_rows());
            for (row, (x, y)) in x.into_iter().zip(y).enumerate() {
                let value = hilbert_value(x, y, u16::BITS);
                let owner = if let Some(hot) = plan.hot_keys.get(&value) {
                    let salt = match routing_salt.as_ref().and_then(|entry| entry.index(row)) {
                        Some(ScalarRef::Number(NumberScalar::UInt64(salt))) => salt,
                        _ => {
                            return Err(ErrorCode::Internal("Hilbert routing salt must be UInt64"));
                        }
                    };
                    hot.owner(salt)
                } else {
                    plan.exchange_bounds.partition_point(|bound| bound < &value)
                };
                if owner >= n {
                    return Err(ErrorCode::Internal(format!(
                        "Hilbert range plan selected owner {owner} outside {n} partitions"
                    )));
                }
                values.push(value);
                owners.push(owner as u8);
            }
            if routing_salt.is_some() {
                data.pop_columns(1);
            }
            data.add_column(UInt32Type::from_data(values));
            data.scatter(&owners, n)
        })();
        if let Err(error) = &result {
            self.fail(error.clone());
        }
        result
    }
}

fn build_plan(sketches: Vec<LocalSketch>, collector_count: usize) -> Result<HilbertRangePlan> {
    let mut samples = Vec::new();
    for (rows, sketch) in sketches {
        validate_sketch(rows, sketch.len())?;
        if rows == 0 {
            continue;
        }
        let weight = rows as f64 / sketch.len() as f64;
        if !weight.is_finite() || weight <= 0.0 {
            return Err(ErrorCode::Internal(
                "Hilbert sample produced an invalid row weight",
            ));
        }
        samples.extend(sketch.into_iter().map(|coordinates| (coordinates, weight)));
    }
    if samples.is_empty() {
        return Err(ErrorCode::Internal("Hilbert recluster sampled no rows"));
    }

    let dimensions = from_fn(|dimension| {
        let mut values = samples
            .iter()
            .filter(|(coordinates, _)| !matches!(coordinates[dimension], Scalar::Null))
            .map(|(coordinates, weight)| (&coordinates[dimension], *weight))
            .collect::<Vec<_>>();
        let data_type = values
            .first()
            .map(|(value, _)| value.as_ref().infer_data_type());
        if let Some(expected) = &data_type
            && values
                .iter()
                .any(|(value, _)| value.as_ref().infer_data_type() != *expected)
        {
            return Err(ErrorCode::Internal(format!(
                "Hilbert dimension {dimension} samples have inconsistent data types"
            )));
        }
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

        Ok(DimensionBounds {
            data_type,
            coarse: TypedRangeBounds::from_scalars(coarse),
            fine: TypedRangeBounds::from_scalars(fine),
            fine_offsets,
            max_coarse_rank: 0,
        })
    });
    let [x, y] = dimensions;
    let mut dimensions = [x?, y?];
    let max_coarse_rank = dimensions
        .iter()
        .map(|dimension| dimension.coarse.len())
        .max()
        .unwrap_or(0) as u32;
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
    let (exchange_bounds, hot_keys) = weighted_exchange_bounds(&weighted_keys, collector_count)?;

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
) -> Result<(Vec<u32>, HashMap<u32, HotKeyRange>)> {
    if weighted_keys
        .iter()
        .any(|(_, weight)| !weight.is_finite() || *weight <= 0.0)
    {
        return Err(ErrorCode::Internal(
            "Hilbert exchange received an invalid sample weight",
        ));
    }
    let total_weight = weighted_keys.iter().map(|(_, weight)| *weight).sum::<f64>();
    if !total_weight.is_finite() || total_weight <= 0.0 {
        return Err(ErrorCode::Internal(
            "Hilbert exchange received an invalid total sample weight",
        ));
    }
    if collector_count <= 1 {
        return Ok((Vec::new(), HashMap::new()));
    }
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
    Ok((bounds, hot_keys))
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
    use rand::Rng;

    use super::*;

    fn int(value: i32) -> Scalar {
        Scalar::Number(NumberScalar::Int32(value))
    }

    fn block(x: Vec<i32>, y: Vec<i32>) -> DataBlock {
        DataBlock::new_from_columns(vec![Int32Type::from_data(x), Int32Type::from_data(y)])
    }

    #[test]
    fn test_sample_budget_and_worker_quotas() {
        let task_rows = MAX_PLAN_SAMPLES * 10;
        let sampling_exchange = HilbertRangeExchange::create([0, 1], task_rows, 4, 2);
        assert_eq!(sampling_exchange.target_sample_size, MAX_PLAN_SAMPLES);
        assert_eq!(sampling_exchange.local_sample_size, MAX_PLAN_SAMPLES / 2);

        let exchange = HilbertRangeExchange::create([0, 1], task_rows, 2, 2);
        exchange.submit_initial(0, task_rows * 9 / 10, vec![[int(0), int(0)]; 100]);
        assert!(!exchange.should_build_plan());
        exchange.submit_initial(1, task_rows / 10, vec![[int(1), int(1)]; 100]);
        assert_eq!(
            exchange.resample_request(0).unwrap(),
            Some(MAX_PLAN_SAMPLES * 9 / 10)
        );
        assert_eq!(
            exchange.resample_request(1).unwrap(),
            Some(MAX_PLAN_SAMPLES / 10)
        );

        let exchange = HilbertRangeExchange::create([0, 1], MAX_PLAN_SAMPLES + 2, 3, 1);
        for worker_id in 0..3 {
            exchange.submit_initial(worker_id, [1, 1, MAX_PLAN_SAMPLES][worker_id], vec![[
                int(worker_id as i32),
                int(0),
            ]]);
        }
        let inner = exchange.inner.lock();
        let HilbertRangeState::Resampling { sample_targets, .. } = &*inner else {
            panic!("all initial sketches must advance to resampling");
        };
        assert_eq!(sample_targets, &[1, 1, MAX_PLAN_SAMPLES - 2]);
    }

    #[tokio::test]
    async fn test_plan_publication_races_do_not_lose_terminal_notification() {
        use std::sync::Barrier;

        for iteration in 0..100 {
            let exchange = HilbertRangeExchange::create([0, 1], 1, 1, 1);
            exchange.submit_initial(0, 1, vec![[int(1), int(1)]]);
            let barrier = Arc::new(Barrier::new(3));
            std::thread::scope(|scope| {
                let publish_exchange = exchange.clone();
                let publish_barrier = barrier.clone();
                scope.spawn(move || {
                    publish_barrier.wait();
                    publish_exchange.publish_plan();
                });

                let terminal_exchange = exchange.clone();
                let terminal_barrier = barrier.clone();
                scope.spawn(move || {
                    terminal_barrier.wait();
                    if iteration % 2 == 0 {
                        terminal_exchange.fail(ErrorCode::Internal("concurrent terminal failure"));
                    } else {
                        terminal_exchange.cancel_before_plan();
                    }
                });
                barrier.wait();
            });

            let result =
                tokio::time::timeout(std::time::Duration::from_secs(1), exchange.wait_plan())
                    .await
                    .expect("plan waiter must not lose a concurrent terminal notification");
            if iteration % 2 == 0 {
                assert_eq!(result.unwrap_err().message(), "concurrent terminal failure");
            } else if let Err(error) = result {
                assert_eq!(error.name(), "AbortedQuery");
            }
        }
    }

    #[test]
    fn test_empty_worker_does_not_block_nonempty_plan() -> Result<()> {
        let exchange = HilbertRangeExchange::create([0, 1], 1, 2, 1);
        exchange.submit_initial(0, 0, Vec::new());
        assert!(!exchange.should_build_plan());
        exchange.submit_initial(1, 1, vec![[int(1), int(1)]]);
        assert!(exchange.should_build_plan());

        exchange.publish_plan();
        let output = exchange.partition(block(vec![1], vec![1]), 1)?;
        assert_eq!(output.len(), 1);
        assert_eq!(output[0].num_rows(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_dimension_type_mismatches_fail_safely() {
        assert_eq!(
            build_plan(
                vec![(2, vec![[int(1), int(1)], [
                    Scalar::String("wrong type".to_string()),
                    int(2)
                ],],)],
                1,
            )
            .unwrap_err()
            .message(),
            "Hilbert dimension 0 samples have inconsistent data types"
        );

        let exchange = HilbertRangeExchange::create([0, 1], 1, 1, 1);
        exchange.submit_initial(0, 1, vec![[int(1), int(1)]]);
        exchange.publish_plan();
        let input = DataBlock::new_from_columns(vec![
            UInt64Type::from_data(vec![1]),
            UInt64Type::from_data(vec![1]),
        ]);
        let error = exchange.partition(input, 1).unwrap_err();
        assert!(
            error
                .message()
                .starts_with("Hilbert dimension type changed")
        );
        assert_eq!(
            exchange.wait_plan().await.unwrap_err().message(),
            error.message()
        );
    }

    #[test]
    fn test_plan_bounds_and_encoding() {
        let samples = (0..MAX_COARSE_RANGES + 1)
            .map(|value| [int((value % 32) as i32), int(value as i32)])
            .collect::<Vec<_>>();
        let plan = build_plan(vec![(samples.len(), samples)], 4).unwrap();
        let low_cardinality = &plan.dimensions[0];
        assert_eq!(low_cardinality.coarse.len(), 32);
        assert!(low_cardinality.coordinate(int(31).as_ref()) > 60_000);

        let weighted = build_plan(
            vec![
                (900, vec![[int(0), int(0)]; 10]),
                (100, vec![[int(100), int(100)]; 10]),
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

        let all_null = build_plan(vec![(1, vec![[Scalar::Null, Scalar::Null]])], 1).unwrap();
        assert_eq!(all_null.dimensions[0].coordinate(ScalarRef::Null), u16::MAX);

        let dimension = DimensionBounds {
            data_type: Some(DataType::Number(
                databend_common_expression::types::NumberDataType::Int32,
            )),
            coarse: TypedRangeBounds::from_scalars(vec![int(10), int(20), int(30)]),
            fine: TypedRangeBounds::from_scalars(vec![int(5), int(25)]),
            fine_offsets: vec![0, 1, 1, 2, 2],
            max_coarse_rank: 3,
        };
        let entry: BlockEntry =
            Int32Type::from_opt_data(vec![Some(5), Some(10), None, Some(25), Some(40)]).into();
        assert_eq!(dimension.encode_column(&entry).unwrap(), vec![
            0,
            15,
            u16::MAX,
            32,
            48
        ]);
    }

    #[test]
    fn test_randomized_plan_and_routing_invariants() -> Result<()> {
        fn fingerprint(blocks: &[DataBlock]) -> Vec<Vec<Vec<Scalar>>> {
            blocks
                .iter()
                .map(|block| {
                    block
                        .columns()
                        .iter()
                        .map(|entry| {
                            (0..block.num_rows())
                                .map(|row| entry.index(row).unwrap().to_owned())
                                .collect()
                        })
                        .collect()
                })
                .collect()
        }

        let mut rng = SmallRng::seed_from_u64(0x6d5a_56da_b19c_4f2b);
        for _case in 0..256 {
            let worker_count = rng.gen_range(1..=6);
            let collector_count = rng.gen_range(1..=8);
            let mut sketches = Vec::with_capacity(worker_count);
            let mut total_rows = 0usize;
            for _worker in 0..worker_count {
                let sample_count = rng.gen_range(1..=64);
                let represented_rows = sample_count * rng.gen_range(1..=128);
                total_rows += represented_rows;
                let samples = (0..sample_count)
                    .map(|_| {
                        from_fn(|_| {
                            if rng.gen_ratio(1, 8) {
                                Scalar::Null
                            } else {
                                int(rng.gen_range(-32..=32))
                            }
                        })
                    })
                    .collect();
                sketches.push((represented_rows, samples));
            }

            let plan = build_plan(sketches, collector_count)?;
            for dimension in &plan.dimensions {
                let coordinates = (-40..=40)
                    .map(|value| dimension.coordinate(int(value).as_ref()))
                    .collect::<Vec<_>>();
                assert!(coordinates.windows(2).all(|pair| pair[0] <= pair[1]));
                assert_eq!(dimension.coordinate(ScalarRef::Null), u16::MAX);
            }
            assert!(
                plan.exchange_bounds
                    .windows(2)
                    .all(|pair| pair[0] <= pair[1])
            );
            assert!(plan.exchange_bounds.len() < collector_count);
            for hot in plan.hot_keys.values() {
                assert!(hot.first_owner <= hot.last_owner);
                assert!(hot.last_owner < collector_count);
                for salt in [0, 1, u64::MAX / 2, u64::MAX] {
                    let owner = hot.owner(salt);
                    assert!(owner >= hot.first_owner && owner <= hot.last_owner);
                }
            }

            let hot_keys = !plan.hot_keys.is_empty();
            let exchange =
                HilbertRangeExchange::create([0, 1], total_rows, worker_count, collector_count);
            *exchange.inner.lock() = HilbertRangeState::Ready(Arc::new(plan));
            let x = (0..97).map(|_| rng.gen_range(-40..=40)).collect::<Vec<_>>();
            let y = (0..97).map(|_| rng.gen_range(-40..=40)).collect::<Vec<_>>();
            let input = if hot_keys {
                DataBlock::new_from_columns(vec![
                    Int32Type::from_data(x),
                    Int32Type::from_data(y),
                    UInt64Type::from_data(
                        (0..97)
                            .map(|_| rng.gen_range(u64::MIN..=u64::MAX))
                            .collect(),
                    ),
                ])
            } else {
                block(x, y)
            };
            let first = exchange.partition(input.clone(), collector_count)?;
            let second = exchange.partition(input, collector_count)?;
            assert_eq!(fingerprint(&first), fingerprint(&second));
            assert_eq!(first.iter().map(DataBlock::num_rows).sum::<usize>(), 97);
        }
        Ok(())
    }

    fn fine_range_count_for(values: &[Scalar], weight: impl Fn(usize) -> f64) -> usize {
        fine_range_count(
            &values
                .iter()
                .enumerate()
                .map(|(index, value)| (value, weight(index)))
                .collect::<Vec<_>>(),
        )
    }

    #[test]
    fn test_fine_ranges_follow_effective_samples() {
        let values = (0..24).map(int).collect::<Vec<_>>();
        assert_eq!(fine_range_count_for(&values, |_| 1.0), 2);

        let values = (0..192).map(int).collect::<Vec<_>>();
        assert_eq!(fine_range_count_for(&values, |_| 1.0), 16);
        assert_eq!(
            fine_range_count_for(&values, |index| if index == 0 { 10_000.0 } else { 1.0 }),
            1
        );

        let duplicates = vec![int(1); 192];
        assert_eq!(fine_range_count_for(&duplicates, |_| 1.0), 1);
    }

    fn empty_plan(hot_keys: HashMap<u32, HotKeyRange>) -> Arc<HilbertRangePlan> {
        Arc::new(HilbertRangePlan {
            dimensions: from_fn(|_| DimensionBounds {
                data_type: None,
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
    fn test_partition_rejects_unready_and_failed_plan() {
        let input = || block(vec![1], vec![1]);
        let exchange = HilbertRangeExchange::create([0, 1], 1, 1, 1);
        assert_eq!(
            exchange.partition(input(), 1).unwrap_err().message(),
            "Hilbert range plan is not ready"
        );

        let exchange = HilbertRangeExchange::create([0, 1], 1, 1, 1);
        exchange.fail(ErrorCode::Internal("routing failed"));
        assert_eq!(
            exchange.partition(input(), 1).unwrap_err().message(),
            "routing failed"
        );
    }

    #[test]
    fn test_exchange_uses_salt_only_for_hot_keys() -> Result<()> {
        let exchange = HilbertRangeExchange::create([0, 1], 2, 1, 2);
        *exchange.inner.lock() = HilbertRangeState::Ready(empty_plan(HashMap::new()));
        let output = exchange.partition(block(vec![1, 2], vec![3, 4]), 2)?;
        assert!(output.iter().all(|block| block.num_columns() == 3));

        let exchange = HilbertRangeExchange::create([0, 1], 4, 1, 2);
        *exchange.inner.lock() =
            HilbertRangeState::Ready(empty_plan(HashMap::from([(0, HotKeyRange {
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
        let (_, hot) = weighted_exchange_bounds(&[(7, 1.0); 100], 4).unwrap();
        assert_eq!(hot[&7].last_owner - hot[&7].first_owner + 1, 4);

        let (bounds, hot) =
            weighted_exchange_bounds(&[(1, 1.0), (2, 1.0), (3, 1.0), (4, 1.0)], 2).unwrap();
        assert_eq!(bounds, vec![2]);
        assert!(hot.is_empty());

        let mut keys = vec![(1, 1.0); 49];
        keys.extend([(2, 1.0); 2]);
        keys.extend([(3, 1.0); 49]);
        let (bounds, hot) = weighted_exchange_bounds(&keys, 2).unwrap();
        assert_eq!(bounds, vec![2]);
        assert!(hot.contains_key(&2));

        for weight in [0.0, f64::NAN, f64::INFINITY] {
            assert_eq!(
                weighted_exchange_bounds(&[(1, weight)], 2)
                    .unwrap_err()
                    .message(),
                "Hilbert exchange received an invalid sample weight"
            );
        }
        assert_eq!(
            weighted_exchange_bounds(&[(1, f64::MAX), (2, f64::MAX)], 2)
                .unwrap_err()
                .message(),
            "Hilbert exchange received an invalid total sample weight"
        );
    }
}
