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

use databend_common_arrow::arrow::bitmap::Bitmap;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::ValueType;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::InputColumns;
use databend_common_expression::Scalar;

use super::StateAddr;
use crate::aggregates::aggregate_function_factory::AggregateFunctionCreator;
use crate::aggregates::aggregate_function_factory::CombinatorDescription;
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
        params: Vec<Scalar>,
        arguments: Vec<DataType>,
        nested_creator: &AggregateFunctionCreator,
    ) -> Result<AggregateFunctionRef> {
        let name = format!("IfCombinator({})", nested_name);
        let argument_len = arguments.len();

        if argument_len == 0 {
            return Err(ErrorCode::NumberArgumentsNotMatch(format!(
                "{} expect to have more than one argument",
                name
            )));
        }

        if !matches!(&arguments[argument_len - 1], DataType::Boolean) {
            return Err(ErrorCode::BadArguments(format!(
                "The type of the last argument for {} must be boolean type, but got {:?}",
                name,
                &arguments[argument_len - 1]
            )));
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

    pub fn combinator_desc() -> CombinatorDescription {
        CombinatorDescription::creator(Box::new(Self::try_create))
    }
}

impl AggregateFunction for AggregateIfCombinator {
    fn name(&self) -> &str {
        &self.name
    }

    fn return_type(&self) -> Result<DataType> {
        self.nested.return_type()
    }

    fn init_state(&self, place: StateAddr) {
        self.nested.init_state(place);
    }

    fn state_layout(&self) -> Layout {
        self.nested.state_layout()
    }

    fn accumulate(
        &self,
        place: StateAddr,
        columns: InputColumns,
        validity: Option<&Bitmap>,
        input_rows: usize,
    ) -> Result<()> {
        let predicate: Bitmap =
            BooleanType::try_downcast_column(&columns[self.argument_len - 1]).unwrap();

        let bitmap = match validity {
            Some(validity) => validity & (&predicate),
            None => predicate,
        };
        self.nested.accumulate(
            place,
            columns.slice(0..self.argument_len - 1),
            Some(&bitmap),
            input_rows,
        )
    }

    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        offset: usize,
        columns: InputColumns,
        _input_rows: usize,
    ) -> Result<()> {
        let predicate: Bitmap =
            BooleanType::try_downcast_column(&columns[self.argument_len - 1]).unwrap();
        let (columns, row_size) =
            self.filter_column(columns.slice(0..self.argument_len - 1), &predicate);
        let new_places = Self::filter_place(places, &predicate);

        let new_places_slice = new_places.as_slice();
        self.nested
            .accumulate_keys(new_places_slice, offset, (&columns).into(), row_size)
    }

    fn accumulate_row(&self, place: StateAddr, columns: InputColumns, row: usize) -> Result<()> {
        let predicate: Bitmap =
            BooleanType::try_downcast_column(&columns[self.argument_len - 1]).unwrap();
        if predicate.get_bit(row) {
            self.nested
                .accumulate_row(place, columns.slice(0..self.argument_len - 1), row)?;
        }
        Ok(())
    }

    fn serialize(&self, place: StateAddr, writer: &mut Vec<u8>) -> Result<()> {
        self.nested.serialize(place, writer)
    }

    fn merge(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        self.nested.merge(place, reader)
    }

    fn merge_states(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        self.nested.merge_states(place, rhs)
    }

    fn merge_result(&self, place: StateAddr, builder: &mut ColumnBuilder) -> Result<()> {
        self.nested.merge_result(place, builder)
    }

    fn need_manual_drop_state(&self) -> bool {
        self.nested.need_manual_drop_state()
    }

    unsafe fn drop_state(&self, place: StateAddr) {
        self.nested.drop_state(place);
    }

    fn get_if_condition(&self, columns: InputColumns) -> Option<Bitmap> {
        let condition_col = &columns[self.argument_len - 1];
        let predicate: Bitmap =
            BooleanType::try_downcast_column(&condition_col.remove_nullable()).unwrap();
        Some(predicate)
    }
}

impl fmt::Display for AggregateIfCombinator {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}_if", self.nested_name)
    }
}

impl AggregateIfCombinator {
    #[inline]
    fn filter_column(&self, columns: InputColumns, predicate: &Bitmap) -> (Vec<Column>, usize) {
        let columns = columns
            .iter()
            .map(|c| c.filter(predicate))
            .collect::<Vec<_>>();

        let rows = predicate.len() - predicate.unset_bits();

        (columns, rows)
    }

    fn filter_place(places: &[StateAddr], predicate: &Bitmap) -> StateAddrs {
        if predicate.unset_bits() == 0 {
            return places.to_vec();
        }
        let it = predicate
            .iter()
            .zip(places.iter())
            .filter(|(v, _)| *v)
            .map(|(_, c)| *c);

        Vec::from_iter(it)
    }
}
