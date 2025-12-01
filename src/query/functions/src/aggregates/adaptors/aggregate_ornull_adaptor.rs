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
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::Scalar;
use databend_common_expression::StateSerdeItem;

use super::AggrState;
use super::AggrStateLoc;
use super::AggregateFunction;
use super::AggregateFunctionRef;
use super::StateAddr;

/// OrNullAdaptor will use OrNull for aggregate functions.
/// If there are no input values, return NULL or a default value, accordingly.
/// Use a single additional byte of data after the nested function data:
/// 0 means there was no input, 1 means there was some.
pub struct AggregateFunctionOrNullAdaptor {
    nested: AggregateFunctionRef,
    inner_nullable: bool,
}

impl AggregateFunctionOrNullAdaptor {
    pub fn create(nested: AggregateFunctionRef) -> Result<AggregateFunctionRef> {
        // count/count distinct should not be nullable for empty set, just return zero
        let inner_return_type = nested.return_type()?;
        if inner_return_type == DataType::Null {
            return Ok(nested);
        }

        Ok(Arc::new(AggregateFunctionOrNullAdaptor {
            nested,
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

fn merge_flag(place: AggrState, other: bool) {
    let flag = other || get_flag(place);
    set_flag(place, flag);
}

fn flag_offset(place: AggrState) -> usize {
    *place.loc.last().unwrap().as_bool().unwrap().1
}

impl AggregateFunction for AggregateFunctionOrNullAdaptor {
    fn name(&self) -> &str {
        self.nested.name()
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(self.nested.return_type()?.wrap_nullable())
    }

    #[inline]
    fn init_state(&self, place: AggrState) {
        let c = place.addr.next(flag_offset(place)).get::<u8>();
        *c = 0;
        self.nested.init_state(place.remove_last_loc())
    }

    fn register_state(&self, registry: &mut AggrStateRegistry) {
        self.nested.register_state(registry);
        registry.register(AggrStateType::Bool);
    }

    #[inline]
    fn accumulate(
        &self,
        place: AggrState,
        columns: ProjectedBlock,
        validity: Option<&Bitmap>,
        input_rows: usize,
    ) -> Result<()> {
        if input_rows == 0 {
            return Ok(());
        }

        let if_cond = self.nested.get_if_condition(columns);

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
            self.nested.accumulate(
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
        columns: ProjectedBlock,
        input_rows: usize,
    ) -> Result<()> {
        self.nested
            .accumulate_keys(places, &loc[..loc.len() - 1], columns, input_rows)?;
        let if_cond = self.nested.get_if_condition(columns);

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
    fn accumulate_row(&self, place: AggrState, columns: ProjectedBlock, row: usize) -> Result<()> {
        self.nested
            .accumulate_row(place.remove_last_loc(), columns, row)?;
        set_flag(place, true);
        Ok(())
    }

    fn serialize_type(&self) -> Vec<StateSerdeItem> {
        self.nested
            .serialize_type()
            .into_iter()
            .chain(Some(StateSerdeItem::DataType(DataType::Boolean)))
            .collect()
    }

    fn batch_serialize(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        let n = builders.len();
        debug_assert_eq!(self.nested.serialize_type().len() + 1, n);
        let flag_builder = builders
            .last_mut()
            .and_then(ColumnBuilder::as_boolean_mut)
            .unwrap();
        for place in places {
            let place = AggrState::new(*place, loc);
            flag_builder.push(get_flag(place));
        }
        self.nested
            .batch_serialize(places, &loc[..loc.len() - 1], &mut builders[..(n - 1)])
    }

    fn batch_merge(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        state: &BlockEntry,
        filter: Option<&Bitmap>,
    ) -> Result<()> {
        match state {
            BlockEntry::Column(Column::Tuple(tuple)) => {
                let flag = tuple.last().unwrap().as_boolean().unwrap();
                let iter = places.iter().zip(flag.iter());
                if let Some(filter) = filter {
                    for (place, flag) in iter.zip(filter.iter()).filter_map(|(v, b)| b.then_some(v))
                    {
                        merge_flag(AggrState::new(*place, loc), flag);
                    }
                } else {
                    for (place, flag) in iter {
                        merge_flag(AggrState::new(*place, loc), flag);
                    }
                }
                let inner_state = Column::Tuple(tuple[0..tuple.len() - 1].to_vec()).into();
                self.nested
                    .batch_merge(places, &loc[0..loc.len() - 1], &inner_state, filter)?;
            }
            BlockEntry::Const(Scalar::Tuple(tuple), DataType::Tuple(data_type), num_rows) => {
                let flag = *tuple.last().unwrap().as_boolean().unwrap();
                if let Some(filter) = filter {
                    for place in places
                        .iter()
                        .zip(filter.iter())
                        .filter_map(|(v, b)| b.then_some(v))
                    {
                        merge_flag(AggrState::new(*place, loc), flag);
                    }
                } else {
                    for place in places {
                        merge_flag(AggrState::new(*place, loc), flag);
                    }
                }
                let inner_state = BlockEntry::new_const_column(
                    DataType::Tuple(data_type[0..data_type.len() - 1].to_vec()),
                    Scalar::Tuple(tuple[0..tuple.len() - 1].to_vec()),
                    *num_rows,
                );
                self.nested
                    .batch_merge(places, &loc[0..loc.len() - 1], &inner_state, filter)?;
            }
            _ => unreachable!(),
        }

        Ok(())
    }

    fn merge_states(&self, place: AggrState, rhs: AggrState) -> Result<()> {
        self.nested
            .merge_states(place.remove_last_loc(), rhs.remove_last_loc())?;
        let flag = get_flag(place) || get_flag(rhs);
        set_flag(place, flag);
        Ok(())
    }

    fn merge_result(
        &self,
        place: AggrState,
        read_only: bool,
        builder: &mut ColumnBuilder,
    ) -> Result<()> {
        match builder {
            ColumnBuilder::Nullable(inner_mut) => {
                if !get_flag(place) {
                    inner_mut.push_null();
                } else if self.inner_nullable {
                    self.nested
                        .merge_result(place.remove_last_loc(), read_only, builder)?;
                } else {
                    self.nested.merge_result(
                        place.remove_last_loc(),
                        read_only,
                        &mut inner_mut.builder,
                    )?;
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
        self.nested
            .get_own_null_adaptor(nested_function, params, arguments)
    }

    fn need_manual_drop_state(&self) -> bool {
        self.nested.need_manual_drop_state()
    }

    unsafe fn drop_state(&self, place: AggrState) {
        self.nested.drop_state(place.remove_last_loc())
    }
}

impl fmt::Display for AggregateFunctionOrNullAdaptor {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.nested)
    }
}
