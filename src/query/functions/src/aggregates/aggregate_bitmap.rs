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
use std::io::BufRead;
use std::marker::PhantomData;
use std::ops::BitAndAssign;
use std::ops::BitOrAssign;
use std::ops::BitXorAssign;
use std::ops::SubAssign;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::decimal::DecimalType;
use databend_common_expression::types::i256;
use databend_common_expression::types::number::Number;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::MutableBitmap;
use databend_common_expression::types::*;
use databend_common_expression::with_decimal_mapped_type;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::with_unsigned_integer_mapped_type;
use databend_common_expression::AggrStateRegistry;
use databend_common_expression::AggrStateType;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::Scalar;
use databend_common_expression::StateSerdeItem;
use databend_common_io::prelude::BinaryWrite;
use databend_common_io::HybridBitmap;
use num_traits::AsPrimitive;

use super::assert_arguments;
use super::assert_params;
use super::assert_unary_arguments;
use super::assert_variadic_params;
use super::extract_number_param;
use super::AggrState;
use super::AggrStateLoc;
use super::AggregateFunction;
use super::AggregateFunctionDescription;
use super::AggregateFunctionSortDesc;
use super::StateAddr;
use super::StateAddrs;
use crate::with_simple_no_number_mapped_type;

#[derive(Clone)]
struct AggregateBitmapFunction<OP, AGG> {
    display_name: String,
    _p: PhantomData<(OP, AGG)>,
}

impl<OP, AGG> AggregateBitmapFunction<OP, AGG>
where
    OP: BitmapOperate,
    AGG: BitmapAggResult,
{
    fn try_create(display_name: &str) -> Result<Arc<dyn AggregateFunction>> {
        let func = AggregateBitmapFunction::<OP, AGG> {
            display_name: display_name.to_string(),
            _p: PhantomData,
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
    fn merge_result(place: AggrState, builder: &mut ColumnBuilder) -> Result<()>;

    fn return_type() -> Result<DataType>;
}

struct BitmapCountResult;

struct BitmapRawResult;

impl BitmapAggResult for BitmapCountResult {
    fn merge_result(place: AggrState, builder: &mut ColumnBuilder) -> Result<()> {
        let mut builder = UInt64Type::downcast_builder(builder);
        let state = place.get::<BitmapAggState>();
        builder.push(state.rb.as_ref().map(|rb| rb.len()).unwrap_or(0));
        Ok(())
    }

    fn return_type() -> Result<DataType> {
        Ok(UInt64Type::data_type())
    }
}

impl BitmapAggResult for BitmapRawResult {
    fn merge_result(place: AggrState, builder: &mut ColumnBuilder) -> Result<()> {
        let mut builder = BitmapType::downcast_builder(builder);
        let state = place.get::<BitmapAggState>();
        match state.rb.as_ref() {
            Some(rb) => builder.push(rb),
            None => builder.push(&HybridBitmap::default()),
        }
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
    fn operate(lhs: &mut HybridBitmap, rhs: HybridBitmap);
}

struct BitmapAndOp;

struct BitmapOrOp;

struct BitmapXorOp;

struct BitmapNotOp;

impl BitmapOperate for BitmapAndOp {
    fn operate(lhs: &mut HybridBitmap, rhs: HybridBitmap) {
        lhs.bitand_assign(rhs);
    }
}

impl BitmapOperate for BitmapOrOp {
    fn operate(lhs: &mut HybridBitmap, rhs: HybridBitmap) {
        lhs.bitor_assign(rhs);
    }
}

impl BitmapOperate for BitmapXorOp {
    fn operate(lhs: &mut HybridBitmap, rhs: HybridBitmap) {
        lhs.bitxor_assign(rhs);
    }
}

impl BitmapOperate for BitmapNotOp {
    fn operate(lhs: &mut HybridBitmap, rhs: HybridBitmap) {
        lhs.sub_assign(rhs);
    }
}

struct BitmapAggState {
    rb: Option<HybridBitmap>,
}

impl BitmapAggState {
    fn new() -> Self {
        Self { rb: None }
    }

    fn insert(&mut self, value: u64) {
        match &mut self.rb {
            Some(rb) => {
                rb.insert(value);
            }
            None => {
                let mut rb = HybridBitmap::new();
                rb.insert(value);
                self.rb = Some(rb);
            }
        }
    }

    fn add<OP: BitmapOperate>(&mut self, other: HybridBitmap) {
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

    fn init_state(&self, place: AggrState) {
        place.write(BitmapAggState::new);
    }

    fn register_state(&self, registry: &mut AggrStateRegistry) {
        registry.register(AggrStateType::Custom(Layout::new::<BitmapAggState>()));
    }

    fn accumulate(
        &self,
        place: AggrState,
        entries: ProjectedBlock,
        validity: Option<&Bitmap>,
        _input_rows: usize,
    ) -> Result<()> {
        let view = entries[0].downcast::<BitmapType>().unwrap();
        if view.len() == 0 {
            return Ok(());
        }

        let state = place.get::<BitmapAggState>();

        if let Some(validity) = validity {
            if validity.null_count() == view.len() {
                return Ok(());
            }

            for (data, valid) in view.iter().zip(validity.iter()) {
                if !valid {
                    continue;
                }
                state.add::<OP>(data.clone());
            }
        } else {
            for data in view.iter() {
                state.add::<OP>(data.clone());
            }
        }
        Ok(())
    }

    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        entries: ProjectedBlock,
        _input_rows: usize,
    ) -> Result<()> {
        let view = entries[0].downcast::<BitmapType>().unwrap();

        for (data, addr) in view.iter().zip(places.iter().cloned()) {
            let state = AggrState::new(addr, loc).get::<BitmapAggState>();
            state.add::<OP>(data.clone());
        }
        Ok(())
    }

    fn accumulate_row(&self, place: AggrState, entries: ProjectedBlock, row: usize) -> Result<()> {
        let view = entries[0].downcast::<BitmapType>().unwrap();
        let state = place.get::<BitmapAggState>();
        if let Some(data) = view.index(row) {
            state.add::<OP>(data.clone());
        }
        Ok(())
    }

    fn serialize_type(&self) -> Vec<StateSerdeItem> {
        vec![StateSerdeItem::Binary(None)]
    }

    fn batch_serialize(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        let binary_builder = builders[0].as_binary_mut().unwrap();
        for place in places {
            let state = AggrState::new(*place, loc).get::<BitmapAggState>();
            // flag indicate where bitmap is none
            let flag: u8 = if state.rb.is_some() { 1 } else { 0 };
            binary_builder.data.write_scalar(&flag)?;
            if let Some(rb) = &state.rb {
                rb.serialize_into(&mut binary_builder.data)?;
            }
            binary_builder.commit_row();
        }
        Ok(())
    }

    fn batch_merge(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        state: &BlockEntry,
        filter: Option<&Bitmap>,
    ) -> Result<()> {
        let view = state.downcast::<UnaryType<BinaryType>>().unwrap();
        let iter = places.iter().zip(view.iter());

        if let Some(filter) = filter {
            for (place, mut data) in iter.zip(filter.iter()).filter_map(|(v, b)| b.then_some(v)) {
                let state = AggrState::new(*place, loc).get::<BitmapAggState>();
                let flag = data[0];
                data.consume(1);
                if flag == 1 {
                    let rb = deserialize_bitmap(data)?;
                    state.add::<OP>(rb);
                }
            }
        } else {
            for (place, mut data) in iter {
                let state = AggrState::new(*place, loc).get::<BitmapAggState>();

                let flag = data[0];
                data.consume(1);
                if flag == 1 {
                    let rb = deserialize_bitmap(data)?;
                    state.add::<OP>(rb);
                }
            }
        }
        Ok(())
    }

    fn merge_states(&self, place: AggrState, rhs: AggrState) -> Result<()> {
        let state = place.get::<BitmapAggState>();
        let other = rhs.get::<BitmapAggState>();

        if let Some(rb) = other.rb.take() {
            state.add::<OP>(rb);
        }
        Ok(())
    }

    fn merge_result(
        &self,
        place: AggrState,
        _read_only: bool,
        builder: &mut ColumnBuilder,
    ) -> Result<()> {
        AGG::merge_result(place, builder)
    }

    fn need_manual_drop_state(&self) -> bool {
        true
    }

    unsafe fn drop_state(&self, place: AggrState) {
        let state = place.get::<BitmapAggState>();
        std::ptr::drop_in_place(state);
    }
}

impl<OP, AGG> fmt::Display for AggregateBitmapFunction<OP, AGG> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

#[derive(Clone)]
struct AggregateGroupBitmapFunction<NUM, AGG> {
    display_name: String,
    _p: PhantomData<(NUM, AGG)>,
}

impl<NUM, AGG> AggregateGroupBitmapFunction<NUM, AGG>
where
    NUM: Number + AsPrimitive<u64>,
    AGG: BitmapAggResult,
{
    fn try_create(display_name: &str) -> Result<Arc<dyn AggregateFunction>> {
        let func = AggregateGroupBitmapFunction::<NUM, AGG> {
            display_name: display_name.to_string(),
            _p: PhantomData,
        };
        Ok(Arc::new(func))
    }
}

impl<NUM, AGG> AggregateFunction for AggregateGroupBitmapFunction<NUM, AGG>
where
    NUM: Number + AsPrimitive<u64>,
    AGG: BitmapAggResult,
{
    fn name(&self) -> &str {
        "AggregateGroupBitmapFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        AGG::return_type()
    }

    fn init_state(&self, place: AggrState) {
        place.write(BitmapAggState::new);
    }

    fn register_state(&self, registry: &mut AggrStateRegistry) {
        registry.register(AggrStateType::Custom(Layout::new::<BitmapAggState>()));
    }

    fn accumulate(
        &self,
        place: AggrState,
        entries: ProjectedBlock,
        validity: Option<&Bitmap>,
        _input_rows: usize,
    ) -> Result<()> {
        let view = entries[0].downcast::<NumberType<NUM>>().unwrap();
        if view.len() == 0 {
            return Ok(());
        }
        let state = place.get::<BitmapAggState>();

        if let Some(validity) = validity {
            if validity.null_count() == view.len() {
                return Ok(());
            }
            for (value, valid) in view.iter().zip(validity.iter()) {
                if valid {
                    state.insert(value.as_());
                }
            }
        } else {
            for value in view.iter() {
                state.insert(value.as_());
            }
        }
        Ok(())
    }

    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        entries: ProjectedBlock,
        _input_rows: usize,
    ) -> Result<()> {
        let view = entries[0].downcast::<NumberType<NUM>>().unwrap();
        for (value, addr) in view.iter().zip(places.iter().cloned()) {
            let state = AggrState::new(addr, loc).get::<BitmapAggState>();
            state.insert(value.as_());
        }
        Ok(())
    }

    fn accumulate_row(&self, place: AggrState, entries: ProjectedBlock, row: usize) -> Result<()> {
        let view = entries[0].downcast::<NumberType<NUM>>().unwrap();
        if let Some(value) = view.index(row) {
            let state = place.get::<BitmapAggState>();
            state.insert(value.as_());
        }
        Ok(())
    }

    fn serialize_type(&self) -> Vec<StateSerdeItem> {
        vec![StateSerdeItem::Binary(None)]
    }

    fn batch_serialize(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        let binary_builder = builders[0].as_binary_mut().unwrap();
        for place in places {
            let state = AggrState::new(*place, loc).get::<BitmapAggState>();
            let flag: u8 = if state.rb.is_some() { 1 } else { 0 };
            binary_builder.data.write_scalar(&flag)?;
            if let Some(rb) = &state.rb {
                rb.serialize_into(&mut binary_builder.data)?;
            }
            binary_builder.commit_row();
        }
        Ok(())
    }

    fn batch_merge(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        state: &BlockEntry,
        filter: Option<&Bitmap>,
    ) -> Result<()> {
        let view = state.downcast::<UnaryType<BinaryType>>().unwrap();
        let iter = places.iter().zip(view.iter());

        if let Some(filter) = filter {
            for (place, mut data) in iter.zip(filter.iter()).filter_map(|(v, b)| b.then_some(v)) {
                let state = AggrState::new(*place, loc).get::<BitmapAggState>();
                let flag = data[0];
                data.consume(1);
                if flag == 1 {
                    let rb = deserialize_bitmap(data)?;
                    state.add::<BitmapOrOp>(rb);
                }
            }
        } else {
            for (place, mut data) in iter {
                let state = AggrState::new(*place, loc).get::<BitmapAggState>();
                let flag = data[0];
                data.consume(1);
                if flag == 1 {
                    let rb = deserialize_bitmap(data)?;
                    state.add::<BitmapOrOp>(rb);
                }
            }
        }
        Ok(())
    }

    fn merge_states(&self, place: AggrState, rhs: AggrState) -> Result<()> {
        let state = place.get::<BitmapAggState>();
        let other = rhs.get::<BitmapAggState>();

        if let Some(rb) = other.rb.take() {
            state.add::<BitmapOrOp>(rb);
        }
        Ok(())
    }

    fn merge_result(
        &self,
        place: AggrState,
        _read_only: bool,
        builder: &mut ColumnBuilder,
    ) -> Result<()> {
        AGG::merge_result(place, builder)
    }

    fn need_manual_drop_state(&self) -> bool {
        true
    }

    unsafe fn drop_state(&self, place: AggrState) {
        let state = place.get::<BitmapAggState>();
        std::ptr::drop_in_place(state);
    }
}

impl<NUM, AGG> fmt::Display for AggregateGroupBitmapFunction<NUM, AGG>
where
    NUM: Number + AsPrimitive<u64>,
    AGG: BitmapAggResult,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

struct AggregateBitmapIntersectCountFunction<T>
where T: ValueType
{
    display_name: String,
    inner: AggregateBitmapFunction<BitmapAndOp, BitmapCountResult>,
    filter_values: Vec<T::Scalar>,
    _t: PhantomData<fn(T)>,
}

impl<T> AggregateBitmapIntersectCountFunction<T>
where
    T: ValueType,
    T::Scalar: Sync,
{
    fn try_create(
        display_name: &str,
        filter_values: Vec<T::Scalar>,
    ) -> Result<Arc<dyn AggregateFunction>> {
        let func = AggregateBitmapIntersectCountFunction::<T> {
            display_name: display_name.to_string(),
            inner: AggregateBitmapFunction {
                display_name: "".to_string(),
                _p: PhantomData,
            },
            filter_values,
            _t: PhantomData,
        };
        Ok(Arc::new(func))
    }

    fn get_filter_bitmap(&self, columns: ProjectedBlock) -> Bitmap {
        let filter_col = columns[1].downcast::<T>().unwrap();

        let mut result = MutableBitmap::from_len_zeroed(columns[0].len());

        for filter_val in &self.filter_values {
            let filter_ref = T::to_scalar_ref(filter_val);
            let mut col_bitmap = MutableBitmap::with_capacity(filter_col.len());

            filter_col.iter().for_each(|val| {
                col_bitmap.push(val == filter_ref);
            });
            (&mut result).bitor_assign(&Bitmap::from(col_bitmap));
        }

        Bitmap::from(result)
    }

    fn filter_row(&self, columns: ProjectedBlock, row: usize) -> Result<bool> {
        let check_col = columns[1].downcast::<T>().unwrap();
        let check_val_opt = check_col.index(row);

        if let Some(check_val) = check_val_opt {
            for filter_val in &self.filter_values {
                if T::to_scalar_ref(filter_val) == check_val {
                    return Ok(true);
                }
            }
        }
        Ok(false)
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

impl<T> AggregateFunction for AggregateBitmapIntersectCountFunction<T>
where
    T: ValueType,
    T::Scalar: Sync,
{
    fn name(&self) -> &str {
        "AggregateBitmapIntersectCountFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        self.inner.return_type()
    }

    fn init_state(&self, place: AggrState) {
        self.inner.init_state(place);
    }

    fn register_state(&self, registry: &mut AggrStateRegistry) {
        self.inner.register_state(registry);
    }

    fn accumulate(
        &self,
        place: AggrState,
        columns: ProjectedBlock,
        validity: Option<&Bitmap>,
        input_rows: usize,
    ) -> Result<()> {
        let predicate = self.get_filter_bitmap(columns);
        let bitmap = match validity {
            Some(validity) => validity & (&predicate),
            None => predicate,
        };
        self.inner
            .accumulate(place, columns, Some(&bitmap), input_rows)
    }

    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        columns: ProjectedBlock,
        _input_rows: usize,
    ) -> Result<()> {
        let predicate = self.get_filter_bitmap(columns);
        let entry = columns[0].to_column().filter(&predicate).into();

        let new_places = Self::filter_place(places, &predicate);
        let new_places_slice = new_places.as_slice();
        let row_size = predicate.len() - predicate.null_count();

        let input = [entry];
        self.inner
            .accumulate_keys(new_places_slice, loc, input.as_slice().into(), row_size)
    }

    fn accumulate_row(&self, place: AggrState, columns: ProjectedBlock, row: usize) -> Result<()> {
        if self.filter_row(columns, row)? {
            return self.inner.accumulate_row(place, columns, row);
        }
        Ok(())
    }

    fn serialize_type(&self) -> Vec<StateSerdeItem> {
        vec![StateSerdeItem::Binary(None)]
    }

    fn batch_serialize(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        self.inner.batch_serialize(places, loc, builders)
    }

    fn batch_merge(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        state: &BlockEntry,
        filter: Option<&Bitmap>,
    ) -> Result<()> {
        self.inner.batch_merge(places, loc, state, filter)
    }

    fn merge_states(&self, place: AggrState, rhs: AggrState) -> Result<()> {
        self.inner.merge_states(place, rhs)
    }

    fn merge_result(
        &self,
        place: AggrState,
        read_only: bool,
        builder: &mut ColumnBuilder,
    ) -> Result<()> {
        self.inner.merge_result(place, read_only, builder)
    }
}

impl<T> fmt::Display for AggregateBitmapIntersectCountFunction<T>
where T: ValueType
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

pub fn try_create_aggregate_bitmap_function<const OP_TYPE: u8, const AGG_TYPE: u8>(
    display_name: &str,
    params: Vec<Scalar>,
    argument_types: Vec<DataType>,
    _sort_descs: Vec<AggregateFunctionSortDesc>,
) -> Result<Arc<dyn AggregateFunction>> {
    assert_params(display_name, params.len(), 0)?;
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

pub fn try_create_aggregate_bitmap_intersect_count_function(
    display_name: &str,
    params: Vec<Scalar>,
    argument_types: Vec<DataType>,
    _sort_descs: Vec<AggregateFunctionSortDesc>,
) -> Result<Arc<dyn AggregateFunction>> {
    assert_arguments(display_name, argument_types.len(), 2)?;
    assert_variadic_params(display_name, params.len(), (1, 32))?;

    let first_argument = argument_types[0].remove_nullable();
    if !first_argument.is_bitmap() {
        return Err(ErrorCode::BadDataValueType(format!(
            "{} the first argument type mismatch, expect: '{:?}', but got: '{:?}'",
            display_name,
            DataType::Bitmap,
            first_argument,
        )));
    }

    let filter_column_type = argument_types[1].remove_nullable();

    with_simple_no_number_mapped_type!(|T| match filter_column_type {
        DataType::T => {
            AggregateBitmapIntersectCountFunction::<T>::try_create(
                display_name,
                extract_params::<T>(display_name, filter_column_type, params)?,
            )
        }
        DataType::Number(num_type) => {
            with_number_mapped_type!(|NUM| match num_type {
                NumberDataType::NUM => {
                    AggregateBitmapIntersectCountFunction::<NumberType<NUM>>::try_create(
                        display_name,
                        extract_number_params::<NUM>(params)?,
                    )
                }
            })
        }
        DataType::Decimal(size) => {
            with_decimal_mapped_type!(|DECIMAL| match size.data_kind() {
                DecimalDataKind::DECIMAL => {
                    AggregateBitmapIntersectCountFunction::<DecimalType<DECIMAL>>::try_create(
                        display_name,
                        extract_params::<DecimalType<DECIMAL>>(
                            display_name,
                            filter_column_type,
                            params,
                        )?,
                    )
                }
            })
        }
        _ => {
            AggregateBitmapIntersectCountFunction::<AnyType>::try_create(
                display_name,
                extract_params::<AnyType>(display_name, filter_column_type, params)?,
            )
        }
    })
}

fn try_create_bitmap_construct_function_impl<AGG: BitmapAggResult>(
    display_name: &str,
    params: Vec<Scalar>,
    argument_types: Vec<DataType>,
) -> Result<Arc<dyn AggregateFunction>> {
    assert_params(display_name, params.len(), 0)?;
    assert_unary_arguments(display_name, argument_types.len())?;
    let arg_type = argument_types[0].remove_nullable();
    match &arg_type {
        DataType::Number(num_type) => {
            let num_type = *num_type;
            with_unsigned_integer_mapped_type!(|NUM_TYPE| match num_type {
                NumberDataType::NUM_TYPE => {
                    AggregateGroupBitmapFunction::<NUM_TYPE, AGG>::try_create(display_name)
                }
                _ => Err(ErrorCode::BadDataValueType(format!(
                    "{} does not support type '{:?}', expect unsigned integer",
                    display_name, arg_type
                ))),
            })
        }
        _ => Err(ErrorCode::BadDataValueType(format!(
            "{} does not support type '{:?}', expect unsigned integer",
            display_name, arg_type
        ))),
    }
}

pub fn try_create_bitmap_construct_agg_function(
    display_name: &str,
    params: Vec<Scalar>,
    argument_types: Vec<DataType>,
    _sort_descs: Vec<AggregateFunctionSortDesc>,
) -> Result<Arc<dyn AggregateFunction>> {
    try_create_bitmap_construct_function_impl::<BitmapRawResult>(
        display_name,
        params,
        argument_types,
    )
}

fn extract_params<T: AccessType>(
    display_name: &str,
    val_type: DataType,
    params: Vec<Scalar>,
) -> Result<Vec<T::Scalar>> {
    let mut filter_values = Vec::with_capacity(params.len());
    for (i, param) in params.iter().enumerate() {
        match T::try_downcast_scalar(&param.as_ref()) {
            Ok(scalar) => filter_values.push(T::to_owned_scalar(scalar)),
            Err(_) => {
                return Err(ErrorCode::BadDataValueType(format!(
                    "{} param({}) type mismatch, expect: '{:?}', but got: '{:?}'",
                    display_name,
                    i,
                    val_type,
                    param.as_ref().infer_data_type()
                )));
            }
        };
    }
    Ok(filter_values)
}

fn extract_number_params<N: Number>(params: Vec<Scalar>) -> Result<Vec<N>> {
    let mut result = Vec::with_capacity(params.len());
    for param in &params {
        let val = extract_number_param::<N>(param.clone())?;
        result.push(val);
    }
    Ok(result)
}

pub fn aggregate_bitmap_and_count_function_desc() -> AggregateFunctionDescription {
    let features = super::AggregateFunctionFeatures {
        is_decomposable: true,
        ..Default::default()
    };
    AggregateFunctionDescription::creator_with_features(
        Box::new(try_create_aggregate_bitmap_function::<BITMAP_AND, BITMAP_AGG_COUNT>),
        features,
    )
}

pub fn aggregate_bitmap_not_count_function_desc() -> AggregateFunctionDescription {
    let features = super::AggregateFunctionFeatures {
        is_decomposable: true,
        ..Default::default()
    };
    AggregateFunctionDescription::creator_with_features(
        Box::new(try_create_aggregate_bitmap_function::<BITMAP_NOT, BITMAP_AGG_COUNT>),
        features,
    )
}

pub fn aggregate_bitmap_or_count_function_desc() -> AggregateFunctionDescription {
    let features = super::AggregateFunctionFeatures {
        is_decomposable: true,
        ..Default::default()
    };
    AggregateFunctionDescription::creator_with_features(
        Box::new(try_create_aggregate_bitmap_function::<BITMAP_OR, BITMAP_AGG_COUNT>),
        features,
    )
}

pub fn aggregate_bitmap_xor_count_function_desc() -> AggregateFunctionDescription {
    let features = super::AggregateFunctionFeatures {
        is_decomposable: true,
        ..Default::default()
    };
    AggregateFunctionDescription::creator_with_features(
        Box::new(try_create_aggregate_bitmap_function::<BITMAP_XOR, BITMAP_AGG_COUNT>),
        features,
    )
}

pub fn aggregate_bitmap_xor_function_desc() -> AggregateFunctionDescription {
    let features = super::AggregateFunctionFeatures {
        is_decomposable: true,
        ..Default::default()
    };
    AggregateFunctionDescription::creator_with_features(
        Box::new(try_create_aggregate_bitmap_function::<BITMAP_XOR, BITMAP_AGG_RAW>),
        features,
    )
}

pub fn aggregate_bitmap_union_function_desc() -> AggregateFunctionDescription {
    let features = super::AggregateFunctionFeatures {
        is_decomposable: true,
        ..Default::default()
    };
    AggregateFunctionDescription::creator_with_features(
        Box::new(try_create_aggregate_bitmap_function::<BITMAP_OR, BITMAP_AGG_RAW>),
        features,
    )
}

pub fn aggregate_bitmap_intersect_function_desc() -> AggregateFunctionDescription {
    let features = super::AggregateFunctionFeatures {
        is_decomposable: true,
        ..Default::default()
    };
    AggregateFunctionDescription::creator_with_features(
        Box::new(try_create_aggregate_bitmap_function::<BITMAP_AND, BITMAP_AGG_RAW>),
        features,
    )
}

pub fn aggregate_bitmap_intersect_count_function_desc() -> AggregateFunctionDescription {
    let features = super::AggregateFunctionFeatures {
        is_decomposable: true,
        ..Default::default()
    };
    AggregateFunctionDescription::creator_with_features(
        Box::new(try_create_aggregate_bitmap_intersect_count_function),
        features,
    )
}

pub fn aggregate_bitmap_construct_agg_function_desc() -> AggregateFunctionDescription {
    let features = super::AggregateFunctionFeatures {
        returns_default_when_only_null: true,
        is_decomposable: true,
        ..Default::default()
    };
    AggregateFunctionDescription::creator_with_features(
        Box::new(try_create_bitmap_construct_agg_function),
        features,
    )
}
