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
use std::marker::PhantomData;
use std::ops::BitAndAssign;
use std::ops::BitOrAssign;
use std::ops::BitXorAssign;
use std::ops::SubAssign;
use std::sync::Arc;

use common_arrow::arrow::bitmap::Bitmap;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::*;
use common_expression::Column;
use common_expression::ColumnBuilder;
use common_expression::Scalar;
use common_io::prelude::BinaryWrite;
use croaring::treemap::NativeSerializer;
use croaring::Treemap;

use super::aggregate_function_factory::AggregateFunctionDescription;
use super::StateAddr;
use crate::aggregates::assert_unary_arguments;
use crate::aggregates::AggregateFunction;

#[derive(Clone)]
struct AggregateBitmapFunction<OP, AGG> {
    display_name: String,
    _op: PhantomData<OP>,
    _agg: PhantomData<AGG>,
}

impl<OP, AGG> AggregateBitmapFunction<OP, AGG>
where
    OP: BitmapOperate,
    AGG: BitmapAggResult,
{
    fn try_create(display_name: &str) -> Result<Arc<dyn AggregateFunction>> {
        let func = AggregateBitmapFunction::<OP, AGG> {
            display_name: display_name.to_string(),
            _op: PhantomData,
            _agg: PhantomData,
        };
        Ok(Arc::new(func))
    }
}

const BITMAP_AGG_RAW: u8 = 0;
const BITMAP_AGG_COUNT: u8 = 1;

macro_rules! with_bitmap_agg_mapped_type {
    (| $t:tt | $($tail:tt)*) => {
        match_template::match_template! {
            $t = [
                BITMAP_AGG_RAW   => BitmapRawResult,
                BITMAP_AGG_COUNT => BitmapCountResult,
            ],
            $($tail)*
        }
    }
}

trait BitmapAggResult: Send + Sync + 'static {
    fn merge_result(place: StateAddr, builder: &mut ColumnBuilder) -> Result<()>;

    fn return_type() -> Result<DataType>;
}

struct BitmapCountResult;

struct BitmapRawResult;

impl BitmapAggResult for BitmapCountResult {
    fn merge_result(place: StateAddr, builder: &mut ColumnBuilder) -> Result<()> {
        let builder = UInt64Type::try_downcast_builder(builder).unwrap();
        let state = place.get::<BitmapAggState>();
        builder.push(state.rb.as_ref().map(|rb| rb.cardinality()).unwrap_or(0));
        Ok(())
    }

    fn return_type() -> Result<DataType> {
        Ok(UInt64Type::data_type())
    }
}

impl BitmapAggResult for BitmapRawResult {
    fn merge_result(place: StateAddr, builder: &mut ColumnBuilder) -> Result<()> {
        let builder = BitmapType::try_downcast_builder(builder).unwrap();
        let state = place.get::<BitmapAggState>();
        if let Some(rb) = state.rb.as_ref() {
            builder.put(&rb.serialize()?);
        };
        builder.commit_row();
        Ok(())
    }

    fn return_type() -> Result<DataType> {
        Ok(BitmapType::data_type())
    }
}

const BITMAP_AND: u8 = 0;
const BITMAP_OR: u8 = 1;
const BITMAP_XOR: u8 = 2;
const BITMAP_NOT: u8 = 3;

macro_rules! with_bitmap_op_mapped_type {
    (| $t:tt | $($tail:tt)*) => {
        match_template::match_template! {
            $t = [
                BITMAP_AND => BitmapAndOp,
                BITMAP_OR  => BitmapOrOp,
                BITMAP_XOR => BitmapXorOp,
                BITMAP_NOT => BitmapNotOp,
            ],
            $($tail)*
        }
    }
}

trait BitmapOperate: Send + Sync + 'static {
    fn operate(lhs: &mut Treemap, rhs: Treemap);
}

struct BitmapAndOp;

struct BitmapOrOp;

struct BitmapXorOp;

struct BitmapNotOp;

impl BitmapOperate for BitmapAndOp {
    fn operate(lhs: &mut Treemap, rhs: Treemap) {
        lhs.bitand_assign(rhs);
    }
}

impl BitmapOperate for BitmapOrOp {
    fn operate(lhs: &mut Treemap, rhs: Treemap) {
        lhs.bitor_assign(rhs);
    }
}

impl BitmapOperate for BitmapXorOp {
    fn operate(lhs: &mut Treemap, rhs: Treemap) {
        lhs.bitxor_assign(rhs);
    }
}

impl BitmapOperate for BitmapNotOp {
    fn operate(lhs: &mut Treemap, rhs: Treemap) {
        lhs.sub_assign(rhs);
    }
}

struct BitmapAggState {
    rb: Option<Treemap>,
}

impl BitmapAggState {
    fn new() -> Self {
        Self { rb: None }
    }

    fn add<OP: BitmapOperate>(&mut self, other: Treemap) {
        match &mut self.rb {
            Some(v) => {
                OP::operate(v, other);
            }
            None => {
                self.rb = Some(other);
            }
        }
    }
}

impl<OP, AGG> AggregateFunction for AggregateBitmapFunction<OP, AGG>
where
    OP: BitmapOperate,
    AGG: BitmapAggResult,
{
    fn name(&self) -> &str {
        "AggregateBitmapFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        AGG::return_type()
    }

    fn init_state(&self, place: super::StateAddr) {
        place.write(BitmapAggState::new);
    }

    fn state_layout(&self) -> Layout {
        Layout::new::<BitmapAggState>()
    }

    fn accumulate(
        &self,
        place: StateAddr,
        columns: &[Column],
        validity: Option<&Bitmap>,
        _input_rows: usize,
    ) -> Result<()> {
        let column = BitmapType::try_downcast_column(&columns[0]).unwrap();
        if column.len() == 0 {
            return Ok(());
        }

        let column_iter = column.iter();
        let state = place.get::<BitmapAggState>();

        if let Some(validity) = validity {
            if validity.unset_bits() == column.len() {
                return Ok(());
            }

            for (data, valid) in column_iter.zip(validity.iter()) {
                if !valid {
                    continue;
                }
                let rb = Treemap::deserialize(data)?;
                state.add::<OP>(rb);
            }
        } else {
            for data in column_iter {
                let rb = Treemap::deserialize(data)?;
                state.add::<OP>(rb);
            }
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
        let column = BitmapType::try_downcast_column(&columns[0]).unwrap();

        for (data, place) in column.iter().zip(places.iter()) {
            let addr = place.next(offset);
            let state = addr.get::<BitmapAggState>();
            let rb = Treemap::deserialize(data)?;
            state.add::<OP>(rb);
        }
        Ok(())
    }

    fn accumulate_row(&self, place: StateAddr, columns: &[Column], row: usize) -> Result<()> {
        let column = BitmapType::try_downcast_column(&columns[0]).unwrap();
        let state = place.get::<BitmapAggState>();
        if let Some(data) = BitmapType::index_column(&column, row) {
            let rb = Treemap::deserialize(data)?;
            state.add::<OP>(rb);
        }
        Ok(())
    }

    fn serialize(&self, place: StateAddr, writer: &mut Vec<u8>) -> Result<()> {
        let state = place.get::<BitmapAggState>();
        // flag indicate where bitmap is none
        let flag: u8 = if state.rb.is_some() { 1 } else { 0 };
        writer.write_scalar(&flag)?;
        if let Some(rb) = &state.rb {
            writer.extend_from_slice(rb.serialize()?.as_slice());
        }
        Ok(())
    }

    fn deserialize(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        let state = place.get::<BitmapAggState>();
        let flag = reader[0];
        state.rb = if flag == 1 {
            Some(Treemap::deserialize(&reader[1..])?)
        } else {
            None
        };
        Ok(())
    }

    fn merge(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        let state = place.get::<BitmapAggState>();
        let rhs = rhs.get::<BitmapAggState>();
        if let Some(rb) = &rhs.rb {
            state.add::<OP>(rb.clone());
        }
        Ok(())
    }

    fn merge_result(&self, place: StateAddr, builder: &mut ColumnBuilder) -> Result<()> {
        AGG::merge_result(place, builder)
    }

    fn need_manual_drop_state(&self) -> bool {
        true
    }

    unsafe fn drop_state(&self, place: StateAddr) {
        let state = place.get::<BitmapAggState>();
        std::ptr::drop_in_place(state);
    }
}

impl<OP, AGG> fmt::Display for AggregateBitmapFunction<OP, AGG> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

pub fn try_create_aggregate_bitmap_function<const OP_TYPE: u8, const AGG_TYPE: u8>(
    display_name: &str,
    _params: Vec<Scalar>,
    argument_types: Vec<DataType>,
) -> Result<Arc<dyn AggregateFunction>> {
    assert_unary_arguments(display_name, argument_types.len())?;
    let data_type = argument_types[0].clone();
    with_bitmap_op_mapped_type!(|OP| match OP_TYPE {
        OP => {
            with_bitmap_agg_mapped_type!(|AGG| match AGG_TYPE {
                AGG => {
                    match data_type {
                        DataType::Bitmap => {
                            AggregateBitmapFunction::<OP, AGG>::try_create(display_name)
                        }
                        _ => Err(ErrorCode::BadDataValueType(format!(
                            "{} does not support type '{:?}'",
                            display_name, data_type
                        ))),
                    }
                }
                _ => {
                    Err(ErrorCode::BadDataValueType(format!(
                        "Unsupported bitmap agg type for aggregate function {} (type number: {})",
                        display_name, AGG_TYPE
                    )))
                }
            })
        }
        _ => Err(ErrorCode::BadDataValueType(format!(
            "Unsupported bitmap operate type for aggregate function {} (type number: {})",
            display_name, OP_TYPE
        ))),
    })
}

pub fn aggregate_bitmap_and_count_function_desc() -> AggregateFunctionDescription {
    let features = super::aggregate_function_factory::AggregateFunctionFeatures {
        is_decomposable: true,
        ..Default::default()
    };
    AggregateFunctionDescription::creator_with_features(
        Box::new(try_create_aggregate_bitmap_function::<BITMAP_AND, BITMAP_AGG_COUNT>),
        features,
    )
}

pub fn aggregate_bitmap_not_count_function_desc() -> AggregateFunctionDescription {
    let features = super::aggregate_function_factory::AggregateFunctionFeatures {
        is_decomposable: true,
        ..Default::default()
    };
    AggregateFunctionDescription::creator_with_features(
        Box::new(try_create_aggregate_bitmap_function::<BITMAP_NOT, BITMAP_AGG_COUNT>),
        features,
    )
}

pub fn aggregate_bitmap_or_count_function_desc() -> AggregateFunctionDescription {
    let features = super::aggregate_function_factory::AggregateFunctionFeatures {
        is_decomposable: true,
        ..Default::default()
    };
    AggregateFunctionDescription::creator_with_features(
        Box::new(try_create_aggregate_bitmap_function::<BITMAP_OR, BITMAP_AGG_COUNT>),
        features,
    )
}

pub fn aggregate_bitmap_xor_count_function_desc() -> AggregateFunctionDescription {
    let features = super::aggregate_function_factory::AggregateFunctionFeatures {
        is_decomposable: true,
        ..Default::default()
    };
    AggregateFunctionDescription::creator_with_features(
        Box::new(try_create_aggregate_bitmap_function::<BITMAP_XOR, BITMAP_AGG_COUNT>),
        features,
    )
}

pub fn aggregate_bitmap_union_function_desc() -> AggregateFunctionDescription {
    let features = super::aggregate_function_factory::AggregateFunctionFeatures {
        is_decomposable: true,
        ..Default::default()
    };
    AggregateFunctionDescription::creator_with_features(
        Box::new(try_create_aggregate_bitmap_function::<BITMAP_OR, BITMAP_AGG_RAW>),
        features,
    )
}

pub fn aggregate_bitmap_intersect_function_desc() -> AggregateFunctionDescription {
    let features = super::aggregate_function_factory::AggregateFunctionFeatures {
        is_decomposable: true,
        ..Default::default()
    };
    AggregateFunctionDescription::creator_with_features(
        Box::new(try_create_aggregate_bitmap_function::<BITMAP_AND, BITMAP_AGG_RAW>),
        features,
    )
}
