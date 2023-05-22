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
use std::sync::Arc;

use common_arrow::arrow::bitmap::Bitmap;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::*;
use common_expression::Column;
use common_expression::ColumnBuilder;
use common_expression::Scalar;
use common_io::prelude::BinaryWrite;
use roaring::RoaringTreemap;

use super::aggregate_function_factory::AggregateFunctionDescription;
use super::StateAddr;
use crate::aggregates::assert_unary_arguments;
use crate::aggregates::AggregateFunction;

#[derive(Clone)]
struct AggregateBitmapCountFunction<OP> {
    display_name: String,
    _op: PhantomData<OP>,
}

impl<OP> AggregateBitmapCountFunction<OP>
where OP: BitmapOperate
{
    fn try_create(display_name: &str) -> Result<Arc<dyn AggregateFunction>> {
        let func = AggregateBitmapCountFunction::<OP> {
            display_name: display_name.to_string(),
            _op: PhantomData,
        };
        Ok(Arc::new(func))
    }
}

const BITMAP_AND: u8 = 0;
const BITMAP_OR: u8 = 1;
const BITMAP_XOR: u8 = 2;

macro_rules! with_bitmap_mapped_type {
    (| $t:tt | $($tail:tt)*) => {
        match_template::match_template! {
            $t = [
                BITMAP_AND => BitmapAndOp,
                BITMAP_OR  => BitmapOrOp,
                BITMAP_XOR => BitmapXorOp,
            ],
            $($tail)*
        }
    }
}

trait BitmapOperate: Send + Sync + 'static {
    fn operate(lhs: &mut RoaringTreemap, rhs: RoaringTreemap);
}

struct BitmapAndOp;
struct BitmapOrOp;
struct BitmapXorOp;

impl BitmapOperate for BitmapAndOp {
    fn operate(lhs: &mut RoaringTreemap, rhs: RoaringTreemap) {
        lhs.bitand_assign(rhs);
    }
}

impl BitmapOperate for BitmapOrOp {
    fn operate(lhs: &mut RoaringTreemap, rhs: RoaringTreemap) {
        lhs.bitor_assign(rhs);
    }
}

impl BitmapOperate for BitmapXorOp {
    fn operate(lhs: &mut RoaringTreemap, rhs: RoaringTreemap) {
        lhs.bitxor_assign(rhs);
    }
}

struct BitmapCountState {
    rb: Option<RoaringTreemap>,
}

impl BitmapCountState {
    fn new() -> Self {
        Self { rb: None }
    }

    fn add<OP: BitmapOperate>(&mut self, other: RoaringTreemap) {
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

impl<OP> AggregateFunction for AggregateBitmapCountFunction<OP>
where OP: BitmapOperate
{
    fn name(&self) -> &str {
        "AggregateBitmapCountFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(UInt64Type::data_type())
    }

    fn init_state(&self, place: super::StateAddr) {
        place.write(BitmapCountState::new);
    }

    fn state_layout(&self) -> Layout {
        Layout::new::<BitmapCountState>()
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
        let state = place.get::<BitmapCountState>();

        if let Some(validity) = validity {
            if validity.unset_bits() == column.len() {
                return Ok(());
            }

            for (data, valid) in column_iter.zip(validity.iter()) {
                if !valid {
                    continue;
                }
                let rb = RoaringTreemap::deserialize_from(data)?;
                state.add::<OP>(rb);
            }
        } else {
            for data in column_iter {
                let rb = RoaringTreemap::deserialize_from(data)?;
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
            let state = addr.get::<BitmapCountState>();
            let rb = RoaringTreemap::deserialize_from(data)?;
            state.add::<OP>(rb);
        }
        Ok(())
    }

    fn accumulate_row(&self, place: StateAddr, columns: &[Column], row: usize) -> Result<()> {
        let column = BitmapType::try_downcast_column(&columns[0]).unwrap();
        let state = place.get::<BitmapCountState>();
        if let Some(data) = BitmapType::index_column(&column, row) {
            let rb = RoaringTreemap::deserialize_from(data)?;
            state.add::<OP>(rb);
        }
        Ok(())
    }

    fn serialize(&self, place: StateAddr, writer: &mut Vec<u8>) -> Result<()> {
        let state = place.get::<BitmapCountState>();
        // flag indicate where bitmap is none
        let flag: u8 = if state.rb.is_some() { 1 } else { 0 };
        writer.write_scalar(&flag)?;
        if let Some(rb) = &state.rb {
            rb.serialize_into(writer)?;
        }
        Ok(())
    }

    fn deserialize(&self, place: StateAddr, reader: &mut &[u8]) -> Result<()> {
        let state = place.get::<BitmapCountState>();
        let flag = reader[0];
        state.rb = if flag == 1 {
            Some(RoaringTreemap::deserialize_from(&reader[1..])?)
        } else {
            None
        };
        Ok(())
    }

    fn merge(&self, place: StateAddr, rhs: StateAddr) -> Result<()> {
        let state = place.get::<BitmapCountState>();
        let rhs = rhs.get::<BitmapCountState>();
        if let Some(rb) = &rhs.rb {
            state.add::<OP>(rb.clone());
        }
        Ok(())
    }

    fn merge_result(&self, place: StateAddr, builder: &mut ColumnBuilder) -> Result<()> {
        let builder = UInt64Type::try_downcast_builder(builder).unwrap();
        let state = place.get::<BitmapCountState>();
        builder.push(state.rb.as_ref().map(|rb| rb.len()).unwrap_or(0));
        Ok(())
    }

    fn need_manual_drop_state(&self) -> bool {
        true
    }

    unsafe fn drop_state(&self, place: StateAddr) {
        let state = place.get::<BitmapCountState>();
        std::ptr::drop_in_place(state);
    }
}

impl<OP> fmt::Display for AggregateBitmapCountFunction<OP> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

pub fn try_create_aggregate_bitmap_count_function<const OP_TYPE: u8>(
    display_name: &str,
    _params: Vec<Scalar>,
    argument_types: Vec<DataType>,
) -> Result<Arc<dyn AggregateFunction>> {
    assert_unary_arguments(display_name, argument_types.len())?;
    let data_type = argument_types[0].clone();
    with_bitmap_mapped_type!(|OP| match OP_TYPE {
        OP => {
            match data_type {
                DataType::Bitmap => AggregateBitmapCountFunction::<OP>::try_create(display_name),
                _ => Err(ErrorCode::BadDataValueType(format!(
                    "{} does not support type '{:?}'",
                    display_name, data_type
                ))),
            }
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
        Box::new(try_create_aggregate_bitmap_count_function::<BITMAP_AND>),
        features,
    )
}

pub fn aggregate_bitmap_or_count_function_desc() -> AggregateFunctionDescription {
    let features = super::aggregate_function_factory::AggregateFunctionFeatures {
        is_decomposable: true,
        ..Default::default()
    };
    AggregateFunctionDescription::creator_with_features(
        Box::new(try_create_aggregate_bitmap_count_function::<BITMAP_OR>),
        features,
    )
}

pub fn aggregate_bitmap_xor_count_function_desc() -> AggregateFunctionDescription {
    let features = super::aggregate_function_factory::AggregateFunctionFeatures {
        is_decomposable: true,
        ..Default::default()
    };
    AggregateFunctionDescription::creator_with_features(
        Box::new(try_create_aggregate_bitmap_count_function::<BITMAP_XOR>),
        features,
    )
}
