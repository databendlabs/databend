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

use common_arrow::arrow::bitmap::Bitmap;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::type_check::check_number;
use common_expression::types::number::*;
use common_expression::types::*;
use common_expression::with_number_mapped_type;
use common_expression::Column;
use common_expression::ColumnBuilder;
use common_expression::Expr;
use common_expression::FunctionContext;
use common_expression::Scalar;
use common_expression::ScalarRef;
use itertools::Itertools;
use num_traits::AsPrimitive;
use serde::Deserialize;
use serde::Serialize;

use super::deserialize_state;
use super::serialize_state;
use crate::aggregates::aggregate_function_factory::AggregateFunctionDescription;
use crate::aggregates::assert_params;
use crate::aggregates::assert_unary_arguments;
use crate::aggregates::AggregateFunction;
use crate::aggregates::AggregateFunctionRef;
use crate::aggregates::StateAddr;
use crate::BUILTIN_FUNCTIONS;

const MEDIAN: u8 = 0;
const QUANTILE: u8 = 1;

#[derive(Serialize, Deserialize)]
struct QuantileTDigestState {
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
    fn new() -> Self {
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

    fn add(&mut self, other: f64) {
        if self.unmerged_weights.len() + self.weights.len() >= self.max_centroids - 1 {
            self.compress();
        }

        self.unmerged_weights.push(1f64);
        self.unmerged_means.push(other);
        self.unmerged_total_weight += 1f64;
    }

    fn merge(&mut self, rhs: &mut Self) -> Result<()> {
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

    fn merge_result(&mut self, builder: &mut ColumnBuilder, levels: Vec<f64>) -> Result<()> {
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
            let builder = NumberType::<F64>::try_downcast_builder(builder).unwrap();
            let q = self.quantile(levels[0]);
            builder.push(q.into());
        }
        Ok(())
    }

    fn quantile(&mut self, level: f64) -> f64 {
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
    _t: PhantomData<T>,
}

impl<T> Display for AggregateQuantileTDigestFunction<T>
where T: Number + AsPrimitive<f64>
{
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

impl<T> AggregateFunction for AggregateQuantileTDigestFunction<T>
where T: Number + AsPrimitive<f64>
{
    fn name(&self) -> &str {
        "AggregateQuantileDiscFunction"
    }
    fn return_type(&self) -> Result<DataType> {
        Ok(self.return_type.clone())
    }
    fn init_state(&self, place: StateAddr) {
        place.write(QuantileTDigestState::new)
    }
    fn state_layout(&self) -> Layout {
        Layout::new::<QuantileTDigestState>()
    }
    fn accumulate(
        &self,
        place: StateAddr,
        columns: &[Column],
        validity: Option<&Bitmap>,
        _input_rows: usize,
    ) -> Result<()> {
        let column = NumberType::<T>::try_downcast_column(&columns[0]).unwrap();
        let state = place.get::<QuantileTDigestState>();
        match validity {
            Some(bitmap) => {
                for (value, is_valid) in column.iter().zip(bitmap.iter()) {
                    if is_valid {
                        state.add(value.as_());
                    }
                }
            }
            None => {
                for value in column.iter() {
                    state.add(value.as_());
                }
            }
        }

        Ok(())
    }
    fn accumulate_row(&self, place: StateAddr, columns: &[Column], row: usize) -> Result<()> {
        let column = NumberType::<T>::try_downcast_column(&columns[0]).unwrap();
        let v = NumberType::<T>::index_column(&column, row);
        if let Some(v) = v {
            let state = place.get::<QuantileTDigestState>();
            state.add(v.as_())
        }
        Ok(())
    }
    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        offset: usize,
        columns: &[Column],
        _input_rows: usize,
    ) -> Result<()> {
        let column = NumberType::<T>::try_downcast_column(&columns[0]).unwrap();
        column.iter().zip(places.iter()).for_each(|(v, place)| {
            let addr = place.next(offset);
            let state = addr.get::<QuantileTDigestState>();
            let v = v.as_();
            state.add(v)
        });
        Ok(())
    }
    fn serialize(&self, place: StateAddr, writer: &mut Vec<u8>) -> Result<()> {
        let state = place.get::<QuantileTDigestState>();
        serialize_state(writer, state)
    }

    fn merge(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        let state = place.get::<QuantileTDigestState>();
        let mut rhs: QuantileTDigestState = deserialize_state(reader)?;
        state.merge(&mut rhs)
    }

    fn merge_states(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        let state = place.get::<QuantileTDigestState>();
        let other = rhs.get::<QuantileTDigestState>();
        state.merge(other)
    }

    fn merge_result(&self, place: StateAddr, builder: &mut ColumnBuilder) -> Result<()> {
        let state = place.get::<QuantileTDigestState>();
        state.merge_result(builder, self.levels.clone())
    }

    fn need_manual_drop_state(&self) -> bool {
        true
    }

    unsafe fn drop_state(&self, place: StateAddr) {
        let state = place.get::<QuantileTDigestState>();
        std::ptr::drop_in_place(state);
    }
}

impl<T> AggregateQuantileTDigestFunction<T>
where T: Number + AsPrimitive<f64>
{
    fn try_create(
        display_name: &str,
        return_type: DataType,
        params: Vec<Scalar>,
        arguments: Vec<DataType>,
    ) -> Result<Arc<dyn AggregateFunction>> {
        let levels = if params.len() == 1 {
            let level: F64 = check_number(
                None,
                &FunctionContext::default(),
                &Expr::<usize>::Cast {
                    span: None,
                    is_try: false,
                    expr: Box::new(Expr::Constant {
                        span: None,
                        scalar: params[0].clone(),
                        data_type: params[0].as_ref().infer_data_type(),
                    }),
                    dest_type: DataType::Number(NumberDataType::Float64),
                },
                &BUILTIN_FUNCTIONS,
            )?;
            let level = level.0;
            if !(0.0..=1.0).contains(&level) {
                return Err(ErrorCode::BadDataValueType(format!(
                    "level range between [0, 1], got: {:?}",
                    level
                )));
            }
            vec![level]
        } else if params.is_empty() {
            vec![0.5f64]
        } else {
            let mut levels = Vec::with_capacity(params.len());
            for param in params {
                let level: F64 = check_number(
                    None,
                    &FunctionContext::default(),
                    &Expr::<usize>::Cast {
                        span: None,
                        is_try: false,
                        expr: Box::new(Expr::Constant {
                            span: None,
                            scalar: param.clone(),
                            data_type: param.as_ref().infer_data_type(),
                        }),
                        dest_type: DataType::Number(NumberDataType::Float64),
                    },
                    &BUILTIN_FUNCTIONS,
                )?;
                let level = level.0;
                if !(0.0..=1.0).contains(&level) {
                    return Err(ErrorCode::BadDataValueType(format!(
                        "level range between [0, 1], got: {:?} in levels",
                        level
                    )));
                }
                levels.push(level);
            }
            levels
        };
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
) -> Result<AggregateFunctionRef> {
    if TYPE == MEDIAN {
        assert_params(display_name, params.len(), 0)?;
    }

    assert_unary_arguments(display_name, arguments.len())?;
    with_number_mapped_type!(|NUM_TYPE| match &arguments[0] {
        DataType::Number(NumberDataType::NUM_TYPE) => {
            let return_type = if params.len() > 1 {
                DataType::Array(Box::new(DataType::Number(NumberDataType::Float64)))
            } else {
                DataType::Number(NumberDataType::Float64)
            };

            AggregateQuantileTDigestFunction::<NUM_TYPE>::try_create(
                display_name,
                return_type,
                params,
                arguments,
            )
        }

        _ => Err(ErrorCode::BadDataValueType(format!(
            "{} does not support type '{:?}'",
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
