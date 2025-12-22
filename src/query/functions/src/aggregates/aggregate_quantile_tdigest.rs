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
use itertools::Itertools;

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
pub(crate) struct QuantileTDigestState {
    epsilon: u32,
    max_centroids: usize,

    total_weight: f64,
    weights: Vec<f64>,
    means: Vec<f64>,
    unmerged_total_weight: f64,
    unmerged_weights: Vec<f64>,
    unmerged_means: Vec<f64>,

    min: f64,
    max: f64,
}

impl QuantileTDigestState {
    pub(crate) fn new() -> Self {
        Self {
            epsilon: 100u32,
            max_centroids: 2048,
            total_weight: 0f64,
            weights: vec![],
            means: vec![],
            unmerged_total_weight: 0f64,
            unmerged_weights: vec![],
            unmerged_means: vec![],
            min: 0f64,
            max: 0f64,
        }
    }

    pub(crate) fn add(&mut self, other: f64, weight: Option<u64>) {
        if self.unmerged_weights.len() + self.weights.len() >= self.max_centroids - 1 {
            self.compress();
        }

        self.unmerged_weights.push(weight.unwrap_or(1) as f64);
        self.unmerged_means.push(other);
        self.unmerged_total_weight += 1f64;
    }

    pub(crate) fn merge(&mut self, rhs: &mut Self) -> Result<()> {
        if rhs.len() == 0 {
            return Ok(());
        }

        rhs.compress();

        self.unmerged_weights.extend_from_slice(&rhs.weights);
        self.unmerged_means.extend_from_slice(&rhs.means);
        self.unmerged_total_weight = rhs.weights.iter().sum();
        self.compress();

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
        self.compress();
        if self.weights.is_empty() {
            return 0f64;
        } else if self.weights.len() == 1 {
            return self.means[0];
        }

        let mean_last = self.means.len() - 1;
        let weight_last = self.weights.len() - 1;

        let index = level * self.total_weight;
        if index < 1f64 {
            return self.min;
        }
        if self.weights[0] > 1f64 && index < self.weights[0] / 2f64 {
            return self.min
                + (index - 1f64) / (self.weights[0] / 2f64 - 1f64) * (self.means[0] - self.min);
        }
        if index > self.total_weight - 1f64 {
            return self.max;
        }
        if self.weights[weight_last] > 1f64
            && self.total_weight - index <= self.weights[weight_last] / 2f64
        {
            return self.max
                - (self.total_weight - index - 1f64) / (self.weights[weight_last] / 2f64 - 1f64)
                    * (self.max - self.means[mean_last]);
        }

        let mut weight_so_far = self.weights[0] / 2f64;
        for i in 0..(self.weights.len() - 1) {
            let dw = (self.weights[i] + self.weights[i + 1]) / 2f64;
            if weight_so_far + dw > index {
                let mut left_unit = 0f64;
                if self.weights[i] == 1f64 {
                    if index - weight_so_far < 0.5 {
                        return self.means[i];
                    }
                    left_unit = 0.5;
                }

                let mut right_unit = 0f64;
                if self.weights[i + 1] == 1f64 {
                    if weight_so_far + dw - index <= 0.5 {
                        return self.means[i + 1];
                    }
                    right_unit = 0.5;
                }

                let z1 = index - weight_so_far - left_unit;
                let z2 = weight_so_far + dw - index - right_unit;
                return QuantileTDigestState::weighted_average(
                    self.means[i],
                    z2,
                    self.means[i + 1],
                    z1,
                );
            }
            weight_so_far += dw;
        }

        debug_assert!(index <= self.total_weight);
        debug_assert!(index >= self.total_weight - self.weights[weight_last] / 2f64);

        let z1 = index - self.total_weight - self.weights[weight_last] / 2f64;
        let z2 = self.weights[weight_last] / 2f64 - z1;

        QuantileTDigestState::weighted_average(self.means[mean_last], z1, self.max, z2)
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
        if self.unmerged_total_weight > 0f64 {
            self.merge_centroid(self.unmerged_weights.clone(), self.unmerged_means.clone());
            self.unmerged_weights.clear();
            self.unmerged_means.clear();
            self.unmerged_total_weight = 0f64;
        }
    }

    fn merge_centroid(&mut self, incoming_weights: Vec<f64>, incoming_means: Vec<f64>) {
        let mut incoming_weights = incoming_weights;
        incoming_weights.extend_from_slice(&self.weights);
        let mut incoming_means = incoming_means;
        incoming_means.extend_from_slice(&self.means);

        // sort (0..incoming_means.len()) by values in incoming_means.
        // e.g. incoming_means[5.0, 2.0, 9.1, 1.3] => [3, 1, 0, 2]
        let incoming_order = (0..incoming_means.len())
            .sorted_by(|&i, &j| incoming_means[i].partial_cmp(&incoming_means[j]).unwrap())
            .collect::<Vec<_>>();

        self.total_weight += self.unmerged_total_weight;

        let normalizer = self.epsilon as f64 / (PI * self.total_weight);

        let mut weights = vec![];
        let mut means = vec![];

        weights.push(incoming_weights[incoming_order[0]]);
        means.push(incoming_means[incoming_order[0]]);

        let mut weight_so_far = 0f64;

        for idx in &incoming_order[1..] {
            let idx = *idx;
            let proposed_weight = weights[weights.len() - 1] + incoming_weights[idx];
            let z = normalizer * proposed_weight;
            let q0 = weight_so_far / self.total_weight;
            let q2 = (weight_so_far + proposed_weight) / self.total_weight;
            let weight_last = weights.len() - 1;
            let mean_last = means.len() - 1;
            if z * z <= q0 * (1f64 - q0) && z * z <= q2 * (1f64 - q2) {
                weights[weight_last] += incoming_weights[idx];
                means[mean_last] = means[mean_last]
                    + (incoming_means[idx] - means[mean_last]) * incoming_weights[idx]
                        / weights[weight_last];
            } else {
                weight_so_far += weights[weight_last];
                weights.push(incoming_weights[idx]);
                means.push(incoming_means[idx]);
            }
        }

        if self.total_weight > 0f64 {
            self.min = f64::min(self.min, means[0]);
            self.max = f64::max(self.max, means[means.len() - 1]);
        }

        self.weights = weights;
        self.means = means;
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
