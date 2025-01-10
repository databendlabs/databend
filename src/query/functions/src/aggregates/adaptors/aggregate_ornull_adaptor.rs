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

use std::fmt;
use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::DataType;
use databend_common_expression::AggrStateRegistry;
use databend_common_expression::AggrStateType;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::InputColumns;
use databend_common_expression::Scalar;
use databend_common_io::prelude::BinaryWrite;

use super::AggrState;
use super::AggrStateLoc;
use super::AggregateFunction;
use super::AggregateFunctionFeatures;
use super::AggregateFunctionRef;
use super::StateAddr;

/// OrNullAdaptor will use OrNull for aggregate functions.
/// If there are no input values, return NULL or a default value, accordingly.
/// Use a single additional byte of data after the nested function data:
/// 0 means there was no input, 1 means there was some.
pub struct AggregateFunctionOrNullAdaptor {
    inner: AggregateFunctionRef,
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

        Ok(Arc::new(AggregateFunctionOrNullAdaptor {
            inner,
            inner_nullable: inner_return_type.is_nullable(),
        }))
    }
}

pub fn set_flag(place: AggrState, flag: bool) {
    let c = place.addr.next(flag_offset(place)).get::<u8>();
    *c = flag as u8;
}

pub fn get_flag(place: AggrState) -> bool {
    let c = place.addr.next(flag_offset(place)).get::<u8>();
    *c != 0
}

fn flag_offset(place: AggrState) -> usize {
    *place.loc.last().unwrap().as_bool().unwrap().1
}

impl AggregateFunction for AggregateFunctionOrNullAdaptor {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(self.inner.return_type()?.wrap_nullable())
    }

    #[inline]
    fn init_state(&self, place: AggrState) {
        let c = place.addr.next(flag_offset(place)).get::<u8>();
        *c = 0;
        self.inner.init_state(place.remove_last_loc())
    }

    fn serialize_size_per_row(&self) -> Option<usize> {
        self.inner.serialize_size_per_row().map(|row| row + 1)
    }

    fn register_state(&self, registry: &mut AggrStateRegistry) {
        self.inner.register_state(registry);
        registry.register(AggrStateType::Bool);
    }

    #[inline]
    fn accumulate(
        &self,
        place: AggrState,
        columns: InputColumns,
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
            .map(|c| c.null_count() != input_rows)
            .unwrap_or(true)
        {
            set_flag(place, true);
            self.inner.accumulate(
                place.remove_last_loc(),
                columns,
                validity.as_ref(),
                input_rows,
            )?;
        }
        Ok(())
    }

    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        columns: InputColumns,
        input_rows: usize,
    ) -> Result<()> {
        self.inner
            .accumulate_keys(places, &loc[..loc.len() - 1], columns, input_rows)?;
        let if_cond = self.inner.get_if_condition(columns);

        match if_cond {
            Some(v) if v.null_count() > 0 => {
                // all nulls
                if v.null_count() == v.len() {
                    return Ok(());
                }

                for (&addr, valid) in places.iter().zip(v.iter()) {
                    if valid {
                        set_flag(AggrState::new(addr, loc), true);
                    }
                }
            }
            _ => {
                for &addr in places {
                    set_flag(AggrState::new(addr, loc), true);
                }
            }
        }

        Ok(())
    }

    #[inline]
    fn accumulate_row(&self, place: AggrState, columns: InputColumns, row: usize) -> Result<()> {
        self.inner
            .accumulate_row(place.remove_last_loc(), columns, row)?;
        set_flag(place, true);
        Ok(())
    }

    #[inline]
    fn serialize(&self, place: AggrState, writer: &mut Vec<u8>) -> Result<()> {
        self.inner.serialize(place.remove_last_loc(), writer)?;
        let flag = get_flag(place) as u8;
        writer.write_scalar(&flag)
    }

    #[inline]
    fn merge(&self, place: AggrState, reader: &mut &[u8]) -> Result<()> {
        let flag = get_flag(place) || reader[reader.len() - 1] > 0;
        self.inner
            .merge(place.remove_last_loc(), &mut &reader[..reader.len() - 1])?;
        set_flag(place, flag);
        Ok(())
    }

    fn merge_states(&self, place: AggrState, rhs: AggrState) -> Result<()> {
        self.inner
            .merge_states(place.remove_last_loc(), rhs.remove_last_loc())?;
        let flag = get_flag(place) || get_flag(rhs);
        set_flag(place, flag);
        Ok(())
    }

    fn merge_result(&self, place: AggrState, builder: &mut ColumnBuilder) -> Result<()> {
        match builder {
            ColumnBuilder::Nullable(inner_mut) => {
                if !get_flag(place) {
                    inner_mut.push_null();
                } else if self.inner_nullable {
                    self.inner.merge_result(place.remove_last_loc(), builder)?;
                } else {
                    self.inner
                        .merge_result(place.remove_last_loc(), &mut inner_mut.builder)?;
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

    unsafe fn drop_state(&self, place: AggrState) {
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
