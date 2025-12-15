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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::DataType;
use databend_common_expression::AggrStateRegistry;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ColumnView;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::Scalar;
use databend_common_expression::StateSerdeItem;

use super::AggrState;
use super::AggrStateLoc;
use super::AggregateFunction;
use super::AggregateFunctionCreator;
use super::AggregateFunctionRef;
use super::AggregateFunctionSortDesc;
use super::CombinatorDescription;
use super::StateAddr;
use super::StateAddrs;

#[derive(Clone)]
pub struct AggregateIfCombinator {
    name: String,
    argument_len: usize,
    nested_name: String,
    nested: AggregateFunctionRef,
    always_false: bool,
}

impl AggregateIfCombinator {
    pub fn try_create(
        nested_name: &str,
        params: Vec<Scalar>,
        arguments: Vec<DataType>,
        sort_descs: Vec<AggregateFunctionSortDesc>,
        nested_creator: &AggregateFunctionCreator,
    ) -> Result<AggregateFunctionRef> {
        let name = format!("IfCombinator({nested_name})");
        let argument_len = arguments.len();

        if argument_len == 0 {
            return Err(ErrorCode::NumberArgumentsNotMatch(format!(
                "{name} expect to have more than one argument",
            )));
        }

        let mut always_false = false;
        if arguments[argument_len - 1].is_null() {
            always_false = true;
        } else if !matches!(&arguments[argument_len - 1], DataType::Boolean) {
            return Err(ErrorCode::BadArguments(format!(
                "The type of the last argument for {name} must be boolean type, but got {:?}",
                &arguments[argument_len - 1]
            )));
        }

        let nested_arguments = &arguments[0..argument_len - 1];
        let nested = nested_creator(nested_name, params, nested_arguments.to_vec(), sort_descs)?;

        Ok(Arc::new(AggregateIfCombinator {
            name,
            argument_len,
            nested_name: nested_name.to_owned(),
            nested,
            always_false,
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

    fn init_state(&self, place: AggrState) {
        self.nested.init_state(place);
    }

    fn register_state(&self, registry: &mut AggrStateRegistry) {
        self.nested.register_state(registry);
    }

    fn accumulate(
        &self,
        place: AggrState,
        block: ProjectedBlock,
        validity: Option<&Bitmap>,
        input_rows: usize,
    ) -> Result<()> {
        if self.always_false {
            return Ok(());
        }

        let view = block[self.argument_len - 1]
            .downcast::<BooleanType>()
            .unwrap();
        let predicate = match view.and_bitmap(validity) {
            ColumnView::Const(true, _) => None,
            ColumnView::Const(false, _) => {
                return Ok(());
            }
            ColumnView::Column(predicate) => Some(predicate),
        };

        self.nested.accumulate(
            place,
            block.slice(0..self.argument_len - 1),
            predicate.as_ref(),
            input_rows,
        )
    }

    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        block: ProjectedBlock,
        input_rows: usize,
    ) -> Result<()> {
        if self.always_false {
            return Ok(());
        }

        match &block[self.argument_len - 1] {
            BlockEntry::Column(Column::Boolean(predicate)) => {
                let (entries, num_rows) =
                    self.filter_column(block.slice(0..self.argument_len - 1), predicate);
                let new_places = Self::filter_place(places, predicate);

                let new_places_slice = new_places.as_slice();
                self.nested
                    .accumulate_keys(new_places_slice, loc, (&entries).into(), num_rows)
            }
            BlockEntry::Const(Scalar::Boolean(v), _, _) => {
                if !*v {
                    return Ok(());
                }
                self.nested.accumulate_keys(
                    places,
                    loc,
                    block.slice(0..self.argument_len - 1),
                    input_rows,
                )
            }
            _ => unreachable!(),
        }
    }

    fn accumulate_row(&self, place: AggrState, block: ProjectedBlock, row: usize) -> Result<()> {
        if self.always_false {
            return Ok(());
        }

        let predicate = block[self.argument_len - 1]
            .downcast::<BooleanType>()
            .unwrap();

        if predicate.index(row).unwrap() {
            self.nested
                .accumulate_row(place, block.slice(0..self.argument_len - 1), row)?;
        }
        Ok(())
    }

    fn serialize_type(&self) -> Vec<StateSerdeItem> {
        self.nested.serialize_type()
    }

    fn batch_serialize(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        self.nested.batch_serialize(places, loc, builders)
    }

    fn batch_merge(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        state: &BlockEntry,
        filter: Option<&Bitmap>,
    ) -> Result<()> {
        self.nested.batch_merge(places, loc, state, filter)
    }

    fn merge_states(&self, place: AggrState, rhs: AggrState) -> Result<()> {
        self.nested.merge_states(place, rhs)
    }

    fn merge_result(
        &self,
        place: AggrState,
        read_only: bool,
        builder: &mut ColumnBuilder,
    ) -> Result<()> {
        self.nested.merge_result(place, read_only, builder)
    }

    fn need_manual_drop_state(&self) -> bool {
        self.nested.need_manual_drop_state()
    }

    unsafe fn drop_state(&self, place: AggrState) {
        self.nested.drop_state(place);
    }

    fn get_if_condition(&self, entries: ProjectedBlock) -> Option<Bitmap> {
        if self.always_false {
            return Some(Bitmap::new_constant(false, entries.len()));
        }
        let condition_col = entries[self.argument_len - 1].clone().remove_nullable();
        let predicate = BooleanType::try_downcast_column(&condition_col.to_column()).unwrap();
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
    fn filter_column(&self, block: ProjectedBlock, predicate: &Bitmap) -> (Vec<BlockEntry>, usize) {
        let entries = block
            .iter()
            .map(|entry| match entry {
                BlockEntry::Const(scalar, data_type, _) => {
                    BlockEntry::Const(scalar.clone(), data_type.clone(), predicate.true_count())
                }
                BlockEntry::Column(column) => column.filter(predicate).into(),
            })
            .collect();

        (entries, predicate.true_count())
    }

    fn filter_place(places: &[StateAddr], predicate: &Bitmap) -> StateAddrs {
        if predicate.null_count() == 0 {
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
