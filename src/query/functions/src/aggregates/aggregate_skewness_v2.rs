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

use common_arrow::arrow::bitmap::Bitmap;
use common_exception::Result;
use common_expression::types::number::*;
use common_expression::types::*;
use common_expression::Column;
use common_expression::ColumnBuilder;
use num_traits::AsPrimitive;
use serde::Deserialize;
use serde::Serialize;

use super::deserialize_state;
use super::serialize_state;
use crate::aggregates::aggregate_unary::UnaryState;
use crate::aggregates::StateAddr;

#[derive(Default, Serialize, Deserialize, Clone)]
pub struct SkewnessStateV2 {
    pub n: u64,
    pub sum: f64,
    pub sum_sqr: f64,
    pub sum_cub: f64,
}

impl SkewnessStateV2 {
    fn new() -> Self {
        Self::default()
    }

    #[inline(always)]
    fn add(&mut self, other: f64) {
        self.n += 1;
        self.sum += other;
        self.sum_sqr += other.powi(2);
        self.sum_cub += other.powi(3);
    }
}

impl UnaryState<Float64Type, Float64Type> for SkewnessStateV2 {
    fn merge(&mut self, rhs: &Self) {
        if rhs.n == 0 {
            return;
        }
        self.n += rhs.n;
        self.sum += rhs.sum;
        self.sum_sqr += rhs.sum_sqr;
        self.sum_cub += rhs.sum_cub;
    }

    fn merge_result(&mut self, builder: &mut ColumnBuilder) -> Result<()> {
        let builder = match builder {
            ColumnBuilder::Nullable(box b) => b,
            _ => unreachable!(),
        };
        if self.n <= 2 {
            builder.push_null();
            return Ok(());
        }
        let n = self.n as f64;
        let temp = 1.0 / n;
        let div = (temp * (self.sum_sqr - self.sum * self.sum * temp))
            .powi(3)
            .sqrt();
        if div == 0.0 {
            builder.push_null();
            return Ok(());
        }
        let temp1 = (n * (n - 1.0)).sqrt() / (n - 2.0);
        let value = temp1
            * temp
            * (self.sum_cub - 3.0 * self.sum_sqr * self.sum * temp
                + 2.0 * self.sum.powi(3) * temp * temp)
            / div;
        if value.is_infinite() || value.is_nan() {
            builder.push_null();
        } else {
            builder.push(Float64Type::upcast_scalar(value.into()).as_ref());
        }
        Ok(())
    }

    fn accumulate(
        &mut self,
        place: StateAddr,
        columns: &[Column],
        validity: Option<&Bitmap>,
        _input_rows: usize,
    ) -> Result<()> {
        let column = Float64Type::try_downcast_column(&columns[0]).unwrap();
        let state = place.get::<Self>();
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

    fn accumulate_row(&mut self, place: StateAddr, columns: &[Column], row: usize) -> Result<()> {
        let column = Float64Type::try_downcast_column(&columns[0]).unwrap();

        let state = place.get::<Self>();
        let v: f64 = column[row].as_();
        state.add(v);
        Ok(())
    }

    fn serialize(&self, place: StateAddr, writer: &mut Vec<u8>) -> Result<()> {
        let state = place.get::<Self>();
        serialize_state(writer, state)
    }

    fn deserialize(&self, reader: &mut &[u8]) -> Result<Self> {
        deserialize_state::<Self>(reader)
    }
}
