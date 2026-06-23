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

use std::alloc::Layout;
use std::f64::consts::PI;
use std::fmt::Display;
use std::fmt::Formatter;
use std::marker::PhantomData;
use std::sync::Arc;

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggrStateRegistry;
use databend_common_expression::AggrStateType;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::StateSerdeItem;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::compute_view::NumberConvertView;
use databend_common_expression::types::number::*;
use databend_common_expression::types::*;
use databend_common_expression::with_decimal_mapped_type;
use databend_common_expression::with_number_mapped_type;

use super::AggrState;
use super::AggrStateLoc;
use super::AggregateFunction;
use super::AggregateFunctionDescription;
use super::AggregateFunctionRef;
use super::AggregateFunctionSortDesc;
use super::StateAddr;
use super::assert_params;
use super::assert_unary_arguments;
use super::borsh_partial_deserialize;
use super::get_levels;

pub(crate) const MEDIAN: u8 = 0;
pub(crate) const QUANTILE: u8 = 1;

#[derive(BorshSerialize, BorshDeserialize)]
pub(crate) enum QuantileTDigestState {
    Normal(TDigestData),
    Nan,
}

#[derive(BorshSerialize, BorshDeserialize)]
pub(crate) struct TDigestData {
    total_weight: f64,
    centroids: Vec<Centroid>,
    unmerged_total_weight: f64,
    unmerged: Vec<Centroid>,

    min: f64,
    max: f64,
}

#[derive(Clone, Copy, BorshSerialize, BorshDeserialize)]
struct Centroid {
    mean: f64,
    weight: f64,
}

impl QuantileTDigestState {
    pub(crate) fn new() -> Self {
        Self::Normal(TDigestData {
            total_weight: 0.0,
            centroids: vec![],
            unmerged_total_weight: 0.0,
            unmerged: vec![],
            min: f64::INFINITY,
            max: f64::NEG_INFINITY,
        })
    }

    pub(crate) fn add(&mut self, other: f64, weight: Option<u64>) {
        let weight = weight.unwrap_or(1) as f64;
        if weight == 0.0 {
            return;
        }
        if other.is_nan() {
            *self = Self::Nan;
            return;
        }

        let Self::Normal(state) = self else {
            return;
        };
        state.add_finite(other, weight);
    }

    pub(crate) fn merge(&mut self, rhs: &mut Self) -> Result<()> {
        match (&mut *self, rhs) {
            (Self::Nan, _) | (_, Self::Nan) => {
                *self = Self::Nan;
            }
            (Self::Normal(state), Self::Normal(rhs)) => state.merge(rhs)?,
        }
        Ok(())
    }

    pub(crate) fn merge_result(
        &mut self,
        builder: &mut ColumnBuilder,
        levels: Vec<f64>,
    ) -> Result<()> {
        if levels.len() > 1 {
            let builder = match builder {
                ColumnBuilder::Array(box b) => b,
                _ => unreachable!(),
            };
            levels.iter().for_each(|level| {
                let q = self.quantile(*level);
                builder.put_item(ScalarRef::Number(NumberScalar::Float64(q.into())))
            });
            builder.commit_row();
        } else {
            let mut builder = NumberType::<F64>::downcast_builder(builder);
            let q = self.quantile(levels[0]);
            builder.push(q.into());
        }
        Ok(())
    }

    pub(crate) fn quantile(&mut self, level: f64) -> f64 {
        match self {
            Self::Normal(state) => state.quantile(level),
            Self::Nan => f64::NAN,
        }
    }
}

impl TDigestData {
    const EPSILON: f64 = 100.0;
    const MAX_CENTROIDS: usize = 2048;

    fn add_finite(&mut self, other: f64, weight: f64) {
        if self.unmerged.len() + self.centroids.len() >= Self::MAX_CENTROIDS - 1 {
            self.compress();
        }

        self.unmerged.push(Centroid {
            mean: other,
            weight,
        });
        self.unmerged_total_weight += weight;
    }

    fn merge(&mut self, rhs: &mut Self) -> Result<()> {
        if rhs.len() == 0 {
            return Ok(());
        }

        rhs.compress();

        self.unmerged.extend_from_slice(&rhs.centroids);
        self.unmerged_total_weight += rhs
            .centroids
            .iter()
            .map(|centroid| centroid.weight)
            .sum::<f64>();
        self.compress();

        Ok(())
    }

    fn quantile(&mut self, level: f64) -> f64 {
        self.compress();
        let (first, last) = match self.centroids.as_slice() {
            [] => return 0.0,
            [Centroid { mean, .. }] => return *mean,
            [first, .., last] => (*first, *last),
        };

        let index = level * self.total_weight;
        if index < 1.0 {
            return self.min;
        }
        if first.weight > 1.0 && index < first.weight / 2.0 {
            return self.min + (index - 1.0) / (first.weight / 2.0 - 1.0) * (first.mean - self.min);
        }
        if index > self.total_weight - 1.0 {
            return self.max;
        }
        if last.weight > 1.0 && self.total_weight - index <= last.weight / 2.0 {
            if last.weight / 2.0 <= 1.0 {
                return self.max;
            }
            return self.max
                - (self.total_weight - index - 1.0) / (last.weight / 2.0 - 1.0)
                    * (self.max - last.mean);
        }

        let mut weight_so_far = first.weight / 2.0;
        for (left, right) in self
            .centroids
            .windows(2)
            .map(|centroids| (centroids[0], centroids[1]))
        {
            let dw = (left.weight + right.weight) / 2.0;
            if weight_so_far + dw > index {
                let mut left_unit = 0.0;
                if left.weight == 1.0 {
                    if index - weight_so_far < 0.5 {
                        return left.mean;
                    }
                    left_unit = 0.5;
                }

                let mut right_unit = 0.0;
                if right.weight == 1.0 {
                    if weight_so_far + dw - index <= 0.5 {
                        return right.mean;
                    }
                    right_unit = 0.5;
                }

                let z1 = index - weight_so_far - left_unit;
                let z2 = weight_so_far + dw - index - right_unit;
                return Self::weighted_average(left.mean, z2, right.mean, z1);
            }
            weight_so_far += dw;
        }

        debug_assert!(index <= self.total_weight);
        debug_assert!(index >= self.total_weight - last.weight / 2.0);

        let z1 = index - self.total_weight - last.weight / 2.0;
        let z2 = last.weight / 2.0 - z1;

        Self::weighted_average(last.mean, z1, self.max, z2)
    }

    fn len(&self) -> usize {
        (self.total_weight + self.unmerged_total_weight) as usize
    }

    fn weighted_average(m1: f64, w1: f64, m2: f64, w2: f64) -> f64 {
        let a = f64::min(m1, m2);
        let b = f64::max(m1, m2);
        let x = (m1 * w1 + m2 * w2) / (w1 + w2);

        f64::max(a, f64::min(b, x))
    }

    fn compress(&mut self) {
        if self.unmerged_total_weight > 0.0 {
            self.merge_centroid(self.unmerged.clone());
            self.unmerged.clear();
            self.unmerged_total_weight = 0.0;
        }
    }

    fn merge_centroid(&mut self, mut incoming: Vec<Centroid>) {
        incoming.extend_from_slice(&self.centroids);
        incoming.sort_by(|a, b| a.mean.total_cmp(&b.mean));

        self.total_weight += self.unmerged_total_weight;

        let normalizer = Self::EPSILON / (PI * self.total_weight);

        let mut incoming = incoming.into_iter();
        let mut current = incoming.next().unwrap();
        let first_mean = current.mean;
        let mut centroids = vec![];
        let mut weight_so_far = 0.0;

        for centroid in incoming {
            let proposed_weight = current.weight + centroid.weight;
            let z = normalizer * proposed_weight;
            let q0 = weight_so_far / self.total_weight;
            let q2 = (weight_so_far + proposed_weight) / self.total_weight;
            if z * z <= q0 * (1.0 - q0) && z * z <= q2 * (1.0 - q2) {
                current.weight = proposed_weight;
                current.mean += (centroid.mean - current.mean) * centroid.weight / current.weight;
            } else {
                weight_so_far += current.weight;
                centroids.push(current);
                current = centroid;
            }
        }

        if self.total_weight > 0.0 {
            self.min = f64::min(self.min, first_mean);
            self.max = f64::max(self.max, current.mean);
        }

        centroids.push(current);
        self.centroids = centroids;
    }
}

#[derive(Clone)]
pub struct AggregateQuantileTDigestFunction<T> {
    display_name: String,
    return_type: DataType,
    levels: Vec<f64>,
    _arguments: Vec<DataType>,
    _t: PhantomData<fn(T)>,
}

impl<T> Display for AggregateQuantileTDigestFunction<T>
where for<'a> T: AccessType<Scalar = F64, ScalarRef<'a> = F64>
{
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

impl<T> AggregateFunction for AggregateQuantileTDigestFunction<T>
where for<'a> T: AccessType<Scalar = F64, ScalarRef<'a> = F64>
{
    fn name(&self) -> &str {
        "AggregateQuantileDiscFunction"
    }
    fn return_type(&self) -> Result<DataType> {
        Ok(self.return_type.clone())
    }
    fn init_state(&self, place: AggrState) {
        place.write(QuantileTDigestState::new)
    }
    fn register_state(&self, registry: &mut AggrStateRegistry) {
        registry.register(AggrStateType::Custom(Layout::new::<QuantileTDigestState>()));
    }
    fn accumulate(
        &self,
        place: AggrState,
        columns: ProjectedBlock,
        validity: Option<&Bitmap>,
        _input_rows: usize,
    ) -> Result<()> {
        let column = columns[0].downcast::<T>().unwrap();

        let state = place.get::<QuantileTDigestState>();
        match validity {
            Some(bitmap) => {
                for (value, is_valid) in column.iter().zip(bitmap.iter()) {
                    if is_valid {
                        state.add(value.into(), None);
                    }
                }
            }
            None => {
                for value in column.iter() {
                    state.add(value.into(), None);
                }
            }
        }

        Ok(())
    }
    fn accumulate_row(&self, place: AggrState, columns: ProjectedBlock, row: usize) -> Result<()> {
        let column = columns[0].downcast::<T>().unwrap();

        let v = column.index(row);

        if let Some(v) = v {
            let state = place.get::<QuantileTDigestState>();
            state.add(v.into(), None)
        }
        Ok(())
    }
    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        columns: ProjectedBlock,
        _input_rows: usize,
    ) -> Result<()> {
        let column = columns[0].downcast::<T>().unwrap();
        column.iter().zip(places.iter()).for_each(|(v, place)| {
            let state = AggrState::new(*place, loc).get::<QuantileTDigestState>();
            state.add(v.into(), None)
        });
        Ok(())
    }

    fn serialize_type(&self) -> Vec<StateSerdeItem> {
        vec![StateSerdeItem::Binary(None)]
    }

    fn batch_serialize(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        let binary_builder = builders[0].as_binary_mut().unwrap();
        for place in places {
            let state = AggrState::new(*place, loc).get::<QuantileTDigestState>();
            state.serialize(&mut binary_builder.data)?;
            binary_builder.commit_row();
        }
        Ok(())
    }

    fn batch_merge(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        state: &BlockEntry,
        filter: Option<&Bitmap>,
    ) -> Result<()> {
        let view = state.downcast::<UnaryType<BinaryType>>().unwrap();
        let iter = places.iter().zip(view.iter());

        if let Some(filter) = filter {
            for (place, mut data) in iter.zip(filter.iter()).filter_map(|(v, b)| b.then_some(v)) {
                let state = AggrState::new(*place, loc).get::<QuantileTDigestState>();
                let mut rhs: QuantileTDigestState = borsh_partial_deserialize(&mut data)?;
                state.merge(&mut rhs)?;
            }
        } else {
            for (place, mut data) in iter {
                let state = AggrState::new(*place, loc).get::<QuantileTDigestState>();
                let mut rhs: QuantileTDigestState = borsh_partial_deserialize(&mut data)?;
                state.merge(&mut rhs)?;
            }
        }
        Ok(())
    }

    fn merge_states(&self, place: AggrState, rhs: AggrState) -> Result<()> {
        let state = place.get::<QuantileTDigestState>();
        let other = rhs.get::<QuantileTDigestState>();
        state.merge(other)
    }

    fn merge_result(
        &self,
        place: AggrState,
        _read_only: bool,
        builder: &mut ColumnBuilder,
    ) -> Result<()> {
        let state = place.get::<QuantileTDigestState>();
        state.merge_result(builder, self.levels.clone())
    }

    fn need_manual_drop_state(&self) -> bool {
        true
    }

    unsafe fn drop_state(&self, place: AggrState) {
        let state = place.get::<QuantileTDigestState>();
        unsafe { std::ptr::drop_in_place(state) };
    }
}

impl<T> AggregateQuantileTDigestFunction<T>
where for<'a> T: AccessType<Scalar = F64, ScalarRef<'a> = F64>
{
    fn try_create(
        display_name: &str,
        return_type: DataType,
        params: Vec<Scalar>,
        arguments: Vec<DataType>,
    ) -> Result<Arc<dyn AggregateFunction>> {
        let levels = get_levels(&params)?;
        let func = AggregateQuantileTDigestFunction::<T> {
            display_name: display_name.to_string(),
            return_type,
            levels,
            _arguments: arguments,
            _t: PhantomData,
        };
        Ok(Arc::new(func))
    }
}

pub fn try_create_aggregate_quantile_tdigest_function<const TYPE: u8>(
    display_name: &str,
    params: Vec<Scalar>,
    arguments: Vec<DataType>,
    _sort_descs: Vec<AggregateFunctionSortDesc>,
) -> Result<AggregateFunctionRef> {
    if TYPE == MEDIAN {
        assert_params(display_name, params.len(), 0)?;
    }

    let return_type = if params.len() > 1 {
        DataType::Array(Box::new(DataType::Number(NumberDataType::Float64)))
    } else {
        DataType::Number(NumberDataType::Float64)
    };

    assert_unary_arguments(display_name, arguments.len())?;
    with_number_mapped_type!(|NUM_TYPE| match &arguments[0] {
        DataType::Number(NumberDataType::NUM_TYPE) => {
            AggregateQuantileTDigestFunction::<NumberConvertView<NUM_TYPE, F64>>::try_create(
                display_name,
                return_type,
                params,
                arguments,
            )
        }

        DataType::Decimal(s) => {
            with_decimal_mapped_type!(|DECIMAL| match s.data_kind() {
                DecimalDataKind::DECIMAL => {
                    AggregateQuantileTDigestFunction::<DecimalF64View<DECIMAL>>::try_create(
                        display_name,
                        return_type,
                        params,
                        arguments,
                    )
                }
            })
        }

        _ => Err(ErrorCode::BadDataValueType(format!(
            "{} just support numeric type, but got '{:?}'",
            display_name, arguments[0]
        ))),
    })
}

pub fn aggregate_quantile_tdigest_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(
        try_create_aggregate_quantile_tdigest_function::<QUANTILE>,
    ))
}

pub fn aggregate_median_tdigest_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(
        try_create_aggregate_quantile_tdigest_function::<MEDIAN>,
    ))
}
