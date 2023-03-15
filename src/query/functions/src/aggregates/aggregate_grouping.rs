// Copyright 2023 Datafuse Labs.
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
use std::fmt::Display;
use std::intrinsics::unlikely;
use std::sync::Arc;

use common_arrow::arrow::bitmap::Bitmap;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::types::NumberColumnBuilder;
use common_expression::types::NumberDataType;
use common_expression::types::NumberScalar;
use common_expression::Column;
use common_expression::ColumnBuilder;
use common_expression::Scalar;
use common_io::prelude::deserialize_from_slice;
use common_io::prelude::serialize_into_buf;
use itertools::Itertools;

use super::aggregate_function_factory::AggregateFunctionDescription;
use super::assert_unary_arguments;
use super::AggregateFunction;
use super::StateAddr;

/// The argument of [`AggregateGroupingFunction`] is the virtual column `_grouping_id` and it can't be `NULL`.
#[derive(Clone)]
pub struct AggregateGroupingFunction {
    display_name: String,
    /// The index of the column represented in `_grouping_id`.
    /// The order will influence the result of `grouping`.
    ///
    /// If `cols` is empty, returns `_grouping_id`.
    cols: Vec<u32>,
}

impl AggregateGroupingFunction {
    pub fn try_create(
        display_name: &str,
        params: Vec<Scalar>,
        arguments: Vec<DataType>,
    ) -> Result<Arc<dyn AggregateFunction>> {
        assert_unary_arguments(display_name, arguments.len())?;
        if params.is_empty() {
            return Err(ErrorCode::NumberArgumentsNotMatch(format!(
                "{} expect to have at least 1 parameter",
                display_name
            )));
        }

        let cols = params
            .into_iter()
            .map(|s| {
                if let Scalar::Number(NumberScalar::UInt32(n)) = s {
                    Ok(n)
                } else {
                    Err(ErrorCode::BadArguments(format!(
                        "{} needs UInt32 parameters, but got {:?}",
                        display_name, s
                    )))
                }
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(Arc::new(Self {
            display_name: display_name.to_string(),
            cols,
        }))
    }

    #[inline(always)]
    pub fn compute_grouping(&self, grouping_id: u32) -> u32 {
        let mut grouping = 0;
        for (i, &j) in self.cols.iter().rev().enumerate() {
            grouping |= ((grouping_id & (1 << j)) >> j) << i;
        }
        grouping
    }

    pub fn desc() -> AggregateFunctionDescription {
        let features = super::aggregate_function_factory::AggregateFunctionFeatures {
            // grouping column will never be NULL.
            returns_default_when_only_null: true,
            ..Default::default()
        };
        AggregateFunctionDescription::creator_with_features(Box::new(Self::try_create), features)
    }
}

impl Display for AggregateGroupingFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

struct AggregateGroupingState {
    /// If it is [None], we don't need to update it any more.
    grouping: Option<u32>,
}

impl AggregateFunction for AggregateGroupingFunction {
    fn name(&self) -> &str {
        "AggregateGroupingFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(DataType::Number(NumberDataType::UInt32))
    }

    fn init_state(&self, place: StateAddr) {
        place.write(|| AggregateGroupingState { grouping: None });
    }

    fn state_layout(&self) -> Layout {
        Layout::new::<AggregateGroupingState>()
    }

    fn accumulate(
        &self,
        _place: StateAddr,
        _columns: &[Column],
        _validity: Option<&Bitmap>,
        _input_rows: usize,
    ) -> Result<()> {
        unreachable!("{} can only be used in group by clause", self.display_name)
    }

    fn accumulate_row(&self, place: StateAddr, columns: &[Column], row: usize) -> Result<()> {
        let state = place.get::<AggregateGroupingState>();
        if state.grouping.is_some() {
            return Ok(());
        }
        let grouping_id = unsafe {
            let col = columns
                .get_unchecked(0)
                .as_number()
                .unwrap()
                .as_u_int32()
                .unwrap();
            *col.get_unchecked(row)
        };

        state.grouping = Some(self.compute_grouping(grouping_id));

        Ok(())
    }

    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        offset: usize,
        columns: &[Column],
        _input_rows: usize,
    ) -> Result<()> {
        // The argument is `_grouping_id` and it is a non-null group-by column, so the value for the same place is the same.
        // Therefore, we only need one place to accumulate the result.
        let places_with_rows = places
            .iter()
            .enumerate()
            .unique_by(|(_, place)| *place)
            .collect::<Vec<_>>();
        for (row, place) in places_with_rows {
            let state = place.next(offset).get::<AggregateGroupingState>();
            if unlikely(state.grouping.is_none()) {
                let grouping_id = unsafe {
                    let col = columns
                        .get_unchecked(0)
                        .as_number()
                        .unwrap()
                        .as_u_int32()
                        .unwrap();
                    *col.get_unchecked(row)
                };
                state.grouping = Some(self.compute_grouping(grouping_id));
            }
        }

        Ok(())
    }

    fn serialize(&self, place: StateAddr, writer: &mut Vec<u8>) -> Result<()> {
        let state = place.get::<AggregateGroupingState>();
        serialize_into_buf(writer, &state.grouping)
    }

    fn deserialize(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        let state = place.get::<AggregateGroupingState>();
        state.grouping = deserialize_from_slice(reader)?;
        Ok(())
    }

    fn merge(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        let state = place.get::<AggregateGroupingState>();
        let rhs = rhs.get::<AggregateGroupingState>();
        if state.grouping.is_none() {
            state.grouping = rhs.grouping;
        }
        Ok(())
    }

    fn merge_result(&self, place: StateAddr, builder: &mut ColumnBuilder) -> Result<()> {
        match builder {
            ColumnBuilder::Number(NumberColumnBuilder::UInt32(builder)) => {
                let state = place.get::<AggregateGroupingState>();
                builder.push(state.grouping.unwrap_or_default());
            }
            _ => unreachable!(),
        }
        Ok(())
    }

    fn batch_merge_result(&self, places: &[StateAddr], builder: &mut ColumnBuilder) -> Result<()> {
        match builder {
            ColumnBuilder::Number(NumberColumnBuilder::UInt32(builder)) => {
                for place in places {
                    let state = place.get::<AggregateGroupingState>();
                    builder.push(state.grouping.unwrap_or_default());
                }
            }
            _ => unreachable!(),
        }
        Ok(())
    }
}
