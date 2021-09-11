// Copyright 2020 Datafuse Labs.
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

use bytes::BytesMut;
use common_arrow::arrow;
use common_arrow::arrow::array::*;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;

use super::StateAddr;
use crate::aggregates::aggregate_function_factory::FactoryFunc;
use crate::aggregates::AggregateFunction;
use crate::aggregates::AggregateFunctionRef;
use crate::aggregates::StateAddrs;

#[derive(Clone)]
pub struct AggregateIfCombinator {
    name: String,
    argument_len: usize,
    nested_name: String,
    nested: AggregateFunctionRef,
}

impl AggregateIfCombinator {
    pub fn try_create(
        nested_name: &str,
        params: Vec<DataValue>,
        arguments: Vec<DataField>,
        nested_creator: FactoryFunc,
    ) -> Result<AggregateFunctionRef> {
        let name = format!("IfCombinator({})", nested_name);
        let argument_len = arguments.len();

        if argument_len == 0 {
            return Err(ErrorCode::NumberArgumentsNotMatch(format!(
                "{} expect to have more than one argument",
                name
            )));
        }

        match arguments[argument_len - 1].data_type() {
            DataType::Boolean => {}
            other => {
                return Err(ErrorCode::BadArguments(format!(
                    "The type of the last argument for {} must be boolean type, but got {:?}",
                    name, other
                )));
            }
        }

        let nested_arguments = &arguments[0..argument_len - 1];
        let nested = nested_creator(nested_name, params, nested_arguments.to_vec())?;

        Ok(Arc::new(AggregateIfCombinator {
            name,
            argument_len,
            nested_name: nested_name.to_owned(),
            nested,
        }))
    }
}

impl AggregateFunction for AggregateIfCombinator {
    fn name(&self) -> &str {
        &self.name
    }

    fn return_type(&self) -> Result<DataType> {
        self.nested.return_type()
    }

    fn nullable(&self, input_schema: &DataSchema) -> Result<bool> {
        self.nested.nullable(input_schema)
    }

    fn init_state(&self, place: StateAddr) {
        self.nested.init_state(place);
    }

    fn state_layout(&self) -> Layout {
        self.nested.state_layout()
    }

    fn accumulate(&self, place: StateAddr, arrays: &[Series], _input_rows: usize) -> Result<()> {
        if arrays.is_empty() {
            return Ok(());
        };

        let predicate_array = arrays[self.argument_len - 1].cast_with_type(&DataType::Boolean)?;
        let predicate = predicate_array.bool()?;

        let (column_array, rows_size) = self.filter_array(arrays, predicate)?;
        self.nested
            .accumulate(place, column_array.as_slice(), rows_size)
    }

    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        offset: usize,
        arrays: &[Series],
        _input_rows: usize,
    ) -> Result<()> {
        if arrays.is_empty() {
            // TODO: maybe is error?
            return Ok(());
        };

        let predicate_array = arrays[self.argument_len - 1].cast_with_type(&DataType::Boolean)?;
        let predicate = predicate_array.bool()?;

        let (column_array, row_size) = self.filter_array(arrays, predicate)?;
        let new_places = Self::filter_place(places, predicate, row_size);

        let new_places_slice = new_places.as_slice();
        let column_array_slice = column_array.as_slice();
        self.nested
            .accumulate_keys(new_places_slice, offset, column_array_slice, row_size)
    }

    fn serialize(&self, place: StateAddr, writer: &mut BytesMut) -> Result<()> {
        self.nested.serialize(place, writer)
    }

    fn deserialize(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        self.nested.deserialize(place, reader)
    }

    fn merge(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        self.nested.merge(place, rhs)
    }

    fn merge_result(&self, place: StateAddr) -> Result<DataValue> {
        self.nested.merge_result(place)
    }
}

impl fmt::Display for AggregateIfCombinator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.nested_name)
    }
}

impl AggregateIfCombinator {
    fn filter_place(places: &[StateAddr], predicate: &DFBooleanArray, rows: usize) -> StateAddrs {
        let mut new_places = Vec::with_capacity(rows);
        let arrow_filter_array = predicate.inner();

        for (index, place) in places.iter().enumerate() {
            if !arrow_filter_array.is_null(index) && arrow_filter_array.value(index) {
                new_places.push(*place);
            }
        }

        new_places
    }

    #[inline]
    fn filter_array(
        &self,
        array: &[Series],
        predicate: &DFBooleanArray,
    ) -> Result<(Vec<Series>, usize)> {
        let arrow_filter_array = predicate.inner();
        let bitmap = arrow_filter_array.values();

        let mut column_array = Vec::with_capacity(self.argument_len - 1);
        let row_size = match array.len() - 1 {
            0 => {
                // if it has no args, only return the row_count
                if predicate.null_count() > 0 {
                    // this greatly simplifies subsequent filtering code
                    // now we only have a boolean mask to deal with
                    let boolean_bm = arrow_filter_array.validity();
                    let res = combine_validities(&Some(bitmap.clone()), boolean_bm);
                    match res {
                        Some(v) => v.len() - v.null_count(),
                        None => 0,
                    }
                } else {
                    bitmap.len() - bitmap.null_count()
                }
            }
            1 => {
                // single array handle
                let data = arrow::compute::filter::filter(
                    array[0].get_array_ref().as_ref(),
                    arrow_filter_array,
                )?;
                let data: ArrayRef = Arc::from(data);
                column_array.push(data.into_series());
                column_array[0].len()
            }
            _ => {
                // multi array handle
                let mut args_array = Vec::with_capacity(self.argument_len - 1);
                for column in array.iter().take(self.argument_len - 1) {
                    args_array.push(column.clone());
                }
                let data = DataArrayFilter::filter_batch_array(args_array, predicate)?;
                data.into_iter()
                    .for_each(|column| column_array.push(column));
                column_array[0].len()
            }
        };

        Ok((column_array, row_size))
    }
}
