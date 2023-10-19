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
use std::fmt;
use std::sync::Arc;

use common_arrow::arrow::bitmap::Bitmap;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::Column;
use common_expression::ColumnBuilder;
use common_expression::Scalar;
use common_io::prelude::BinaryWrite;

use crate::aggregates::aggregate_function_factory::AggregateFunctionFeatures;
use crate::aggregates::AggregateFunction;
use crate::aggregates::AggregateFunctionRef;
use crate::aggregates::StateAddr;

/// OrNullAdaptor will use OrNull for aggregate functions.
/// If there are no input values, return NULL or a default value, accordingly.
/// Use a single additional byte of data after the nested function data:
/// 0 means there was no input, 1 means there was some.
pub struct AggregateFunctionOrNullAdaptor {
    inner: AggregateFunctionRef,
    size_of_data: usize,
    inner_nullable: bool,
}

impl AggregateFunctionOrNullAdaptor {
    pub fn create(
        inner: AggregateFunctionRef,
        features: AggregateFunctionFeatures,
    ) -> Result<AggregateFunctionRef> {
        // count/count distinct should not be nullable for empty set, just return zero
        let inner_return_type = inner.return_type()?;
        if features.returns_default_when_only_null || inner_return_type == DataType::Null {
            return Ok(inner);
        }

        let inner_layout = inner.state_layout();
        Ok(Arc::new(AggregateFunctionOrNullAdaptor {
            inner,
            size_of_data: inner_layout.size(),
            inner_nullable: matches!(inner_return_type, DataType::Nullable(_)),
        }))
    }

    #[inline]
    pub fn set_flag(&self, place: StateAddr, flag: u8) {
        let c = place.next(self.size_of_data).get::<u8>();
        *c = flag;
    }

    #[inline]
    pub fn get_flag(&self, place: StateAddr) -> u8 {
        let c = place.next(self.size_of_data).get::<u8>();
        *c
    }
}

impl AggregateFunction for AggregateFunctionOrNullAdaptor {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(self.inner.return_type()?.wrap_nullable())
    }

    #[inline]
    fn init_state(&self, place: StateAddr) {
        let c = place.next(self.size_of_data).get::<u8>();
        *c = 0;
        self.inner.init_state(place)
    }

    fn serialize_size_per_row(&self) -> Option<usize> {
        self.inner.serialize_size_per_row().map(|row| row + 1)
    }

    #[inline]
    fn state_layout(&self) -> std::alloc::Layout {
        let layout = self.inner.state_layout();
        Layout::from_size_align(layout.size() + layout.align(), layout.align()).unwrap()
    }

    #[inline]
    fn accumulate(
        &self,
        place: StateAddr,
        columns: &[Column],
        validity: Option<&Bitmap>,
        input_rows: usize,
    ) -> Result<()> {
        if input_rows == 0 {
            return Ok(());
        }

        let if_cond = self.inner.get_if_condition(columns);

        let validity = match (if_cond, validity) {
            (None, None) => None,
            (None, Some(b)) => Some(b.clone()),
            (Some(a), None) => Some(a),
            (Some(a), Some(b)) => Some(&a & b),
        };

        if validity
            .as_ref()
            .map(|c| c.unset_bits() != input_rows)
            .unwrap_or(true)
        {
            self.set_flag(place, 1);
            self.inner
                .accumulate(place, columns, validity.as_ref(), input_rows)?;
        }
        Ok(())
    }

    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        offset: usize,
        columns: &[Column],
        input_rows: usize,
    ) -> Result<()> {
        self.inner
            .accumulate_keys(places, offset, columns, input_rows)?;
        let if_cond = self.inner.get_if_condition(columns);
        match if_cond {
            Some(v) if v.unset_bits() > 0 => {
                // all nulls
                if v.unset_bits() == v.len() {
                    return Ok(());
                }

                for (place, valid) in places.iter().zip(v.iter()) {
                    if valid {
                        self.set_flag(place.next(offset), 1);
                    }
                }
            }
            _ => {
                for place in places {
                    self.set_flag(place.next(offset), 1);
                }
            }
        }

        Ok(())
    }

    #[inline]
    fn accumulate_row(&self, place: StateAddr, columns: &[Column], row: usize) -> Result<()> {
        self.inner.accumulate_row(place, columns, row)?;
        self.set_flag(place, 1);
        Ok(())
    }

    #[inline]
    fn serialize(&self, place: StateAddr, writer: &mut Vec<u8>) -> Result<()> {
        self.inner.serialize(place, writer)?;
        writer.write_scalar(&self.get_flag(place))
    }

    #[inline]
    fn merge(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        let flag = self.get_flag(place) > 0 || reader[reader.len() - 1] > 0;

        self.inner.merge(place, &mut &reader[..reader.len() - 1])?;
        self.set_flag(place, flag as u8);
        Ok(())
    }

    fn merge_states(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        self.inner.merge_states(place, rhs)?;
        let flag = self.get_flag(place) > 0 || self.get_flag(rhs) > 0;
        self.set_flag(place, u8::from(flag));
        Ok(())
    }

    fn merge_result(&self, place: StateAddr, builder: &mut ColumnBuilder) -> Result<()> {
        match builder {
            ColumnBuilder::Nullable(inner_mut) => {
                if self.get_flag(place) == 0 {
                    inner_mut.push_null();
                } else if self.inner_nullable {
                    self.inner.merge_result(place, builder)?;
                } else {
                    self.inner.merge_result(place, &mut inner_mut.builder)?;
                    inner_mut.validity.push(true);
                }
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    fn get_own_null_adaptor(
        &self,
        nested_function: AggregateFunctionRef,
        params: Vec<Scalar>,
        arguments: Vec<DataType>,
    ) -> Result<Option<AggregateFunctionRef>> {
        self.inner
            .get_own_null_adaptor(nested_function, params, arguments)
    }

    fn need_manual_drop_state(&self) -> bool {
        self.inner.need_manual_drop_state()
    }

    unsafe fn drop_state(&self, place: StateAddr) {
        self.inner.drop_state(place)
    }

    fn convert_const_to_full(&self) -> bool {
        self.inner.convert_const_to_full()
    }
}
impl fmt::Display for AggregateFunctionOrNullAdaptor {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.inner)
    }
}
