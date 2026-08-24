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
use std::cmp::Ordering;
use std::io::BufRead;
use std::marker::PhantomData;
use std::ops::BitAndAssign;
use std::ops::BitOrAssign;
use std::ops::BitXorAssign;
use std::ops::SubAssign;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggrState;
use databend_common_expression::AggrStateType;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::StateSerdeItem;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::BitmapType;
use databend_common_expression::types::BuilderExt;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DecimalDataKind;
use databend_common_expression::types::DecimalType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::types::ValueType;
use databend_common_expression::types::i256;
use databend_common_expression::types::number::Number;
use databend_common_expression::types::*;
use databend_common_expression::with_decimal_mapped_type;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::with_unsigned_integer_mapped_type;
use databend_common_io::HybridBitmap;
use databend_common_io::bitmap::BitmapRhs;
use databend_common_io::deserialize_bitmap;
use databend_common_io::prelude::BinaryWrite;
use num_traits::AsPrimitive;

use super::super::common::extract_number_param;
use super::FunctionFactory;
use super::adaptors::*;
use crate::with_simple_no_number_mapped_type;

struct BitmapBuilder;

impl BitmapBuilder {
    fn register(registry: &mut AggregateFunctionRegistry) {
        Self::construct_route().register(registry);
        Self::bitmap_route::<BITMAP_AND, BITMAP_COUNT>().register(registry);
        Self::bitmap_route::<BITMAP_NOT, BITMAP_COUNT>().register(registry);
        Self::bitmap_route::<BITMAP_OR, BITMAP_COUNT>().register(registry);
        Self::bitmap_route::<BITMAP_XOR, BITMAP_COUNT>().register(registry);
        Self::bitmap_route::<BITMAP_OR, BITMAP_RAW>().register(registry);
        Self::bitmap_route::<BITMAP_AND, BITMAP_RAW>().register(registry);
        Self::bitmap_route::<BITMAP_XOR, BITMAP_RAW>().register(registry);
        Self::intersect_count_route().register(registry);
    }
}

inventory::submit! {
    FunctionFactory {
        register: BitmapBuilder::register,
    }
}

impl BitmapBuilder {
    fn bitmap_numeric_arguments() -> AggregateArgumentsPattern {
        AggregateArgumentsPattern::fixed(vec![AggregateArgumentPattern::any_numeric()])
    }

    fn bitmap_arguments() -> AggregateArgumentsPattern {
        AggregateArgumentsPattern::fixed(vec![AggregateArgumentPattern::exact(DataType::Bitmap)])
    }

    fn bitmap_intersect_count_arguments() -> AggregateArgumentsPattern {
        AggregateArgumentsPattern::fixed(vec![
            AggregateArgumentPattern::exact(DataType::Bitmap),
            AggregateArgumentPattern::any(),
        ])
    }

    const BITMAP_CONSTRUCT_AGG_FEATURES: FunctionFeatures = FunctionFeatures {
        is_decomposable: true,
        supports_filter: false,
        sort_policy: SortPolicy::Unsupported,
        distinct_policy: DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "constructs a bitmap from unsigned integer values",
        definition: "bitmap_construct_agg(expr)",
        example: "select bitmap_construct_agg(number) from numbers(10)",
    };

    const BITMAP_AND_COUNT_FEATURES: FunctionFeatures = FunctionFeatures {
        is_decomposable: true,
        supports_filter: false,
        sort_policy: SortPolicy::Unsupported,
        distinct_policy: DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "counts bits in the intersection of bitmap values",
        definition: "bitmap_and_count(bitmap)",
        example: "select bitmap_and_count(bitmap_col) from t",
    };

    const BITMAP_NOT_COUNT_FEATURES: FunctionFeatures = FunctionFeatures {
        is_decomposable: true,
        supports_filter: false,
        sort_policy: SortPolicy::Unsupported,
        distinct_policy: DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "counts bits after subtracting subsequent bitmap values from the first",
        definition: "bitmap_not_count(bitmap)",
        example: "select bitmap_not_count(bitmap_col) from t",
    };

    const BITMAP_OR_COUNT_FEATURES: FunctionFeatures = FunctionFeatures {
        is_decomposable: true,
        supports_filter: false,
        sort_policy: SortPolicy::Unsupported,
        distinct_policy: DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "counts bits in the union of bitmap values",
        definition: "bitmap_or_count(bitmap)",
        example: "select bitmap_or_count(bitmap_col) from t",
    };

    const BITMAP_XOR_COUNT_FEATURES: FunctionFeatures = FunctionFeatures {
        is_decomposable: true,
        supports_filter: false,
        sort_policy: SortPolicy::Unsupported,
        distinct_policy: DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "counts bits in the xor of bitmap values",
        definition: "bitmap_xor_count(bitmap)",
        example: "select bitmap_xor_count(bitmap_col) from t",
    };

    const BITMAP_UNION_FEATURES: FunctionFeatures = FunctionFeatures {
        is_decomposable: true,
        supports_filter: false,
        sort_policy: SortPolicy::Unsupported,
        distinct_policy: DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "returns the union of bitmap values",
        definition: "bitmap_union(bitmap)",
        example: "select bitmap_union(bitmap_col) from t",
    };

    const BITMAP_INTERSECT_FEATURES: FunctionFeatures = FunctionFeatures {
        is_decomposable: true,
        supports_filter: false,
        sort_policy: SortPolicy::Unsupported,
        distinct_policy: DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "returns the intersection of bitmap values",
        definition: "bitmap_intersect(bitmap)",
        example: "select bitmap_intersect(bitmap_col) from t",
    };

    const BITMAP_XOR_AGG_FEATURES: FunctionFeatures = FunctionFeatures {
        is_decomposable: true,
        supports_filter: false,
        sort_policy: SortPolicy::Unsupported,
        distinct_policy: DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "returns the xor of bitmap values",
        definition: "bitmap_xor_agg(bitmap)",
        example: "select bitmap_xor_agg(bitmap_col) from t",
    };

    const INTERSECT_COUNT_FEATURES: FunctionFeatures = FunctionFeatures {
        is_decomposable: true,
        supports_filter: false,
        sort_policy: SortPolicy::Unsupported,
        distinct_policy: DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "counts bits in the intersection of filtered bitmap values",
        definition: "intersect_count(params...)(bitmap, expr)",
        example: "select intersect_count(1, 2)(bitmap_col, key) from t",
    };

    fn bitmap_features<const OP_TYPE: u8, const RESULT_TYPE: u8>() -> FunctionFeatures {
        match (OP_TYPE, RESULT_TYPE) {
            (BITMAP_AND, BITMAP_COUNT) => Self::BITMAP_AND_COUNT_FEATURES,
            (BITMAP_NOT, BITMAP_COUNT) => Self::BITMAP_NOT_COUNT_FEATURES,
            (BITMAP_OR, BITMAP_COUNT) => Self::BITMAP_OR_COUNT_FEATURES,
            (BITMAP_XOR, BITMAP_COUNT) => Self::BITMAP_XOR_COUNT_FEATURES,
            (BITMAP_OR, BITMAP_RAW) => Self::BITMAP_UNION_FEATURES,
            (BITMAP_AND, BITMAP_RAW) => Self::BITMAP_INTERSECT_FEATURES,
            (BITMAP_XOR, BITMAP_RAW) => Self::BITMAP_XOR_AGG_FEATURES,
            _ => unreachable!(),
        }
    }

    fn bitmap_names<const OP_TYPE: u8, const RESULT_TYPE: u8>() -> &'static [&'static str] {
        match (OP_TYPE, RESULT_TYPE) {
            (BITMAP_AND, BITMAP_COUNT) => &["bitmap_and_count"],
            (BITMAP_NOT, BITMAP_COUNT) => &["bitmap_not_count"],
            (BITMAP_OR, BITMAP_COUNT) => &["bitmap_or_count"],
            (BITMAP_XOR, BITMAP_COUNT) => &["bitmap_xor_count"],
            (BITMAP_OR, BITMAP_RAW) => &["bitmap_union", "bitmap_or_agg"],
            (BITMAP_AND, BITMAP_RAW) => &["bitmap_intersect", "bitmap_and_agg"],
            (BITMAP_XOR, BITMAP_RAW) => &["bitmap_xor_agg"],
            _ => unreachable!(),
        }
    }

    fn bitmap_distinct_is_alias<const OP_TYPE: u8, const RESULT_TYPE: u8>() -> bool {
        matches!(
            (OP_TYPE, RESULT_TYPE),
            (BITMAP_AND, BITMAP_COUNT)
                | (BITMAP_OR, BITMAP_COUNT)
                | (BITMAP_OR, BITMAP_RAW)
                | (BITMAP_AND, BITMAP_RAW)
        )
    }
}

const BITMAP_AND: u8 = 0;
const BITMAP_OR: u8 = 1;
const BITMAP_XOR: u8 = 2;
const BITMAP_NOT: u8 = 3;

const BITMAP_RAW: u8 = 0;
const BITMAP_COUNT: u8 = 1;

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

macro_rules! with_bitmap_result_mapped_type {
    (| $t:tt | $($tail:tt)*) => {
        match_template::match_template! {
            $t = [
                BITMAP_RAW   => BitmapRawResult,
                BITMAP_COUNT => BitmapCountResult,
            ],
            $($tail)*
        }
    }
}

#[derive(Default)]
pub struct AggregateBitmapState {
    rb: Option<HybridBitmap>,
}

impl AggregateBitmapState {
    pub fn state_description() -> AggregateStateDescription {
        AggregateStateDescription::new(vec![AggrStateType::Custom(Layout::new::<Self>())], vec![
            StateSerdeItem::Binary(None),
        ])
        .with_manual_drop(true)
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

    fn insert_many<I>(&mut self, values: I)
    where I: IntoIterator<Item = u64> {
        let mut values = values.into_iter().collect::<Vec<_>>();
        if values.is_empty() {
            return;
        }

        let len = values.len();
        values.sort_unstable();
        values.dedup();
        if values.len() * 4 > len * 3 {
            for value in values {
                self.insert(value);
            }
            return;
        }

        self.add_bitmap::<BitmapOrOp>(HybridBitmap::from_iter(values));
    }

    fn add<OP>(&mut self, other: &[u8]) -> Result<()>
    where OP: BitmapOperate {
        match &mut self.rb {
            Some(rb) => OP::operate_buf(rb, other),
            None => {
                self.rb = Some(deserialize_bitmap(other)?);
                Ok(())
            }
        }
    }

    fn add_bitmap<OP>(&mut self, other: HybridBitmap)
    where OP: BitmapOperate {
        match &mut self.rb {
            Some(rb) => OP::operate(rb, other),
            None => {
                self.rb = Some(other);
            }
        }
    }

    fn serialize(&self, builder: &mut ColumnBuilder) -> Result<()> {
        let binary_builder = builder.as_binary_mut().unwrap();
        let flag = u8::from(self.rb.is_some());
        binary_builder.data.write_scalar(&flag)?;
        if let Some(rb) = &self.rb {
            rb.serialize_into(&mut binary_builder.data)?;
        }
        binary_builder.commit_row();
        Ok(())
    }

    fn merge_serialized<OP>(&mut self, mut data: &[u8]) -> Result<()>
    where OP: BitmapOperate {
        let flag = data[0];
        data.consume(1);
        if flag == 1 {
            self.add::<OP>(data)?;
        }
        Ok(())
    }

    fn merge_owned<OP>(&mut self, rhs: &mut Self)
    where OP: BitmapOperate {
        if let Some(rb) = rhs.rb.take() {
            self.add_bitmap::<OP>(rb);
        }
    }
}

trait BitmapOperate: Send + Sync + 'static {
    fn operate(lhs: &mut HybridBitmap, rhs: HybridBitmap);

    fn operate_buf(lhs: &mut HybridBitmap, rhs: &[u8]) -> Result<()>;
}

struct BitmapAndOp;
struct BitmapOrOp;
struct BitmapXorOp;
struct BitmapNotOp;

impl BitmapOperate for BitmapAndOp {
    fn operate(lhs: &mut HybridBitmap, rhs: HybridBitmap) {
        lhs.bitand_assign(rhs);
    }

    fn operate_buf(lhs: &mut HybridBitmap, rhs: &[u8]) -> Result<()> {
        lhs.bitand_assign_rhs(BitmapRhs::Serialized(rhs))
    }
}

impl BitmapOperate for BitmapOrOp {
    fn operate(lhs: &mut HybridBitmap, rhs: HybridBitmap) {
        lhs.bitor_assign(rhs);
    }

    fn operate_buf(lhs: &mut HybridBitmap, rhs: &[u8]) -> Result<()> {
        lhs.bitor_assign_rhs(BitmapRhs::Serialized(rhs))
    }
}

impl BitmapOperate for BitmapXorOp {
    fn operate(lhs: &mut HybridBitmap, rhs: HybridBitmap) {
        lhs.bitxor_assign(rhs);
    }

    fn operate_buf(lhs: &mut HybridBitmap, rhs: &[u8]) -> Result<()> {
        lhs.bitxor_assign_rhs(BitmapRhs::Serialized(rhs))
    }
}

impl BitmapOperate for BitmapNotOp {
    fn operate(lhs: &mut HybridBitmap, rhs: HybridBitmap) {
        lhs.sub_assign(rhs);
    }

    fn operate_buf(lhs: &mut HybridBitmap, rhs: &[u8]) -> Result<()> {
        lhs.sub_assign_rhs(BitmapRhs::Serialized(rhs))
    }
}

trait BitmapResult: Send + Sync + 'static {
    fn return_type() -> DataType;

    fn push_result(state: &AggregateBitmapState, builder: &mut ColumnBuilder) -> Result<()>;
}

struct BitmapCountResult;
struct BitmapRawResult;

impl BitmapResult for BitmapCountResult {
    fn return_type() -> DataType {
        UInt64Type::data_type()
    }

    fn push_result(state: &AggregateBitmapState, builder: &mut ColumnBuilder) -> Result<()> {
        let mut builder = UInt64Type::downcast_builder(builder);
        builder.push_item(state.rb.as_ref().map(|rb| rb.len()).unwrap_or(0));
        Ok(())
    }
}

impl BitmapResult for BitmapRawResult {
    fn return_type() -> DataType {
        BitmapType::data_type()
    }

    fn push_result(state: &AggregateBitmapState, builder: &mut ColumnBuilder) -> Result<()> {
        let mut builder = BitmapType::downcast_builder(builder);
        if let Some(rb) = &state.rb {
            rb.serialize_into(&mut builder.data)?;
        }
        builder.commit_row();
        Ok(())
    }
}

struct AggregateBitmapImplementation<OP, R> {
    _p: PhantomData<fn(OP, R)>,
}

impl<OP, R> Default for AggregateBitmapImplementation<OP, R> {
    fn default() -> Self {
        Self { _p: PhantomData }
    }
}

impl<OP, R> AggrImpl for AggregateBitmapImplementation<OP, R>
where
    OP: BitmapOperate,
    R: BitmapResult,
{
    fn init_state(&self, state: AggrState<'_>) {
        state.write(AggregateBitmapState::default);
    }

    fn accumulate(&self, input: AccumulateInput<'_>) -> Result<()> {
        let values = input.columns[0].downcast::<BitmapType>().unwrap();
        let state = input.state.get::<AggregateBitmapState>();
        match input.validity {
            Some(validity) => {
                for (value, valid) in values.iter().zip(validity.iter()) {
                    if valid {
                        state.add::<OP>(value)?;
                    }
                }
            }
            None => {
                for value in values.iter() {
                    state.add::<OP>(value)?;
                }
            }
        }
        Ok(())
    }

    fn accumulate_keys(&self, input: AccumulateKeysInput<'_>) -> Result<()> {
        let values = input.columns[0].downcast::<BitmapType>().unwrap();
        for (row, state) in input.states.iter().enumerate() {
            state
                .get::<AggregateBitmapState>()
                .add::<OP>(values.index(row).unwrap())?;
        }
        Ok(())
    }

    fn accumulate_row(&self, input: AccumulateRowInput<'_>) -> Result<()> {
        let values = input.columns[0].downcast::<BitmapType>().unwrap();
        input
            .state
            .get::<AggregateBitmapState>()
            .add::<OP>(values.index(input.row).unwrap())?;
        Ok(())
    }

    fn serialize(&self, input: SerializeInput<'_>) -> Result<()> {
        serialize_bitmap_states(input)
    }

    fn merge_serialized(&self, input: MergeSerializedInput<'_>) -> Result<()> {
        merge_serialized_bitmap_states::<OP>(input)
    }

    fn merge_states(&self, input: MergeStatesInput<'_>) -> Result<()> {
        merge_bitmap_states::<OP>(input);
        Ok(())
    }

    fn merge_result(&self, input: MergeResultInput<'_>) -> Result<()> {
        R::push_result(input.state.get::<AggregateBitmapState>(), input.builder)
    }

    unsafe fn drop_state(&self, state: AggrState<'_>) {
        unsafe { drop_bitmap_state(state) };
    }
}

struct AggregateGroupBitmapImplementation<N, R> {
    _p: PhantomData<fn(N, R)>,
}

struct AggregateBitmapIntersectCountImplementation<T>
where T: AccessType
{
    filter_values: Vec<T::Scalar>,
}

impl<T> AggregateBitmapIntersectCountImplementation<T>
where T: AccessType
{
    fn new(filter_values: Vec<T::Scalar>) -> Self {
        Self { filter_values }
    }

    fn filter_row(&self, filter_entry: &BlockEntry, row: usize) -> bool {
        let filter_values = filter_entry.downcast::<T>().unwrap();
        let Some(value) = filter_values.index(row) else {
            return false;
        };

        let value = T::to_owned_scalar(value);
        self.filter_values.iter().any(|filter| {
            T::compare(T::to_scalar_ref(filter), T::to_scalar_ref(&value)) == Ordering::Equal
        })
    }
}

impl<T> AggrImpl for AggregateBitmapIntersectCountImplementation<T>
where
    T: AccessType,
    T::Scalar: Send + Sync,
{
    fn init_state(&self, state: AggrState<'_>) {
        state.write(AggregateBitmapState::default);
    }

    fn accumulate(&self, input: AccumulateInput<'_>) -> Result<()> {
        let bitmaps = input.columns[0].downcast::<BitmapType>().unwrap();
        let state = input.state.get::<AggregateBitmapState>();
        for row in 0..input.columns.num_rows() {
            if input
                .validity
                .is_some_and(|validity| !validity.get(row).unwrap())
            {
                continue;
            }
            if self.filter_row(&input.columns[1], row) {
                state.add::<BitmapAndOp>(bitmaps.index(row).unwrap())?;
            }
        }
        Ok(())
    }

    fn accumulate_keys(&self, input: AccumulateKeysInput<'_>) -> Result<()> {
        let bitmaps = input.columns[0].downcast::<BitmapType>().unwrap();
        for (row, state) in input.states.iter().enumerate() {
            if self.filter_row(&input.columns[1], row) {
                state
                    .get::<AggregateBitmapState>()
                    .add::<BitmapAndOp>(bitmaps.index(row).unwrap())?;
            }
        }
        Ok(())
    }

    fn accumulate_row(&self, input: AccumulateRowInput<'_>) -> Result<()> {
        if self.filter_row(&input.columns[1], input.row) {
            let bitmaps = input.columns[0].downcast::<BitmapType>().unwrap();
            input
                .state
                .get::<AggregateBitmapState>()
                .add::<BitmapAndOp>(bitmaps.index(input.row).unwrap())?;
        }
        Ok(())
    }

    fn serialize(&self, input: SerializeInput<'_>) -> Result<()> {
        serialize_bitmap_states(input)
    }

    fn merge_serialized(&self, input: MergeSerializedInput<'_>) -> Result<()> {
        merge_serialized_bitmap_states::<BitmapAndOp>(input)
    }

    fn merge_states(&self, input: MergeStatesInput<'_>) -> Result<()> {
        merge_bitmap_states::<BitmapAndOp>(input);
        Ok(())
    }

    fn merge_result(&self, input: MergeResultInput<'_>) -> Result<()> {
        BitmapCountResult::push_result(input.state.get::<AggregateBitmapState>(), input.builder)
    }

    unsafe fn drop_state(&self, state: AggrState<'_>) {
        unsafe { drop_bitmap_state(state) };
    }
}

impl<N, R> Default for AggregateGroupBitmapImplementation<N, R> {
    fn default() -> Self {
        Self { _p: PhantomData }
    }
}

impl<N, R> AggrImpl for AggregateGroupBitmapImplementation<N, R>
where
    N: Number + AsPrimitive<u64>,
    R: BitmapResult,
{
    fn init_state(&self, state: AggrState<'_>) {
        state.write(AggregateBitmapState::default);
    }

    fn accumulate(&self, input: AccumulateInput<'_>) -> Result<()> {
        let values = input.columns[0].downcast::<NumberType<N>>().unwrap();
        let state = input.state.get::<AggregateBitmapState>();
        match input.validity {
            Some(validity) => {
                for (value, valid) in values.iter().zip(validity.iter()) {
                    if valid {
                        state.insert(value.as_());
                    }
                }
            }
            None => {
                state.insert_many(values.iter().map(|value| value.as_()));
            }
        }
        Ok(())
    }

    fn accumulate_keys(&self, input: AccumulateKeysInput<'_>) -> Result<()> {
        let values = input.columns[0].downcast::<NumberType<N>>().unwrap();
        for (row, state) in input.states.iter().enumerate() {
            state
                .get::<AggregateBitmapState>()
                .insert(values.index(row).unwrap().as_());
        }
        Ok(())
    }

    fn accumulate_row(&self, input: AccumulateRowInput<'_>) -> Result<()> {
        let values = input.columns[0].downcast::<NumberType<N>>().unwrap();
        input
            .state
            .get::<AggregateBitmapState>()
            .insert(values.index(input.row).unwrap().as_());
        Ok(())
    }

    fn serialize(&self, input: SerializeInput<'_>) -> Result<()> {
        serialize_bitmap_states(input)
    }

    fn merge_serialized(&self, input: MergeSerializedInput<'_>) -> Result<()> {
        merge_serialized_bitmap_states::<BitmapOrOp>(input)
    }

    fn merge_states(&self, input: MergeStatesInput<'_>) -> Result<()> {
        merge_bitmap_states::<BitmapOrOp>(input);
        Ok(())
    }

    fn merge_result(&self, input: MergeResultInput<'_>) -> Result<()> {
        R::push_result(input.state.get::<AggregateBitmapState>(), input.builder)
    }

    unsafe fn drop_state(&self, state: AggrState<'_>) {
        unsafe { drop_bitmap_state(state) };
    }
}

impl BitmapBuilder {
    fn bitmap_route<const OP_TYPE: u8, const RESULT_TYPE: u8>() -> DirectNameRoute {
        let arguments = Self::bitmap_arguments();
        let features = Self::bitmap_features::<OP_TYPE, RESULT_TYPE>();
        let route = DirectNameRoute::new(
            Self::bitmap_names::<OP_TYPE, RESULT_TYPE>(),
            arguments,
            features,
            NullPolicy::Skip,
        )
        .with_validator(Self::validate_bitmap_request)
        .then(MergeRoute::multi_arg(
            false,
            Self::create_bitmap::<OP_TYPE, RESULT_TYPE>,
        ))
        .then(MergeRoute::multi_arg(
            true,
            Self::create_bitmap::<OP_TYPE, RESULT_TYPE>,
        ))
        .then(PlainRoute::multi_arg(
            Self::create_bitmap::<OP_TYPE, RESULT_TYPE>,
        ))
        .then(IfRoute::multi_arg(
            Self::create_bitmap::<OP_TYPE, RESULT_TYPE>,
        ))
        .then(StateRoute::multi_arg(
            Self::create_bitmap::<OP_TYPE, RESULT_TYPE>,
        ));
        if Self::bitmap_distinct_is_alias::<OP_TYPE, RESULT_TYPE>() {
            route.then(DistinctAliasRoute::multi_arg(
                Self::create_bitmap::<OP_TYPE, RESULT_TYPE>,
            ))
        } else {
            route
        }
    }

    fn validate_bitmap_request(request: &AggregateFunctionRequest<'_>) -> Result<()> {
        if request.params.is_empty() {
            Ok(())
        } else {
            Err(ErrorCode::BadArguments(format!(
                "{} expects no parameters",
                request.name
            )))
        }
    }

    fn create_bitmap<const OP_TYPE: u8, const RESULT_TYPE: u8>(
        build: MultiArgBuildContext<'_, impl CombinatorImpl>,
    ) -> Result<AggregateFunctionRef> {
        let data_type = &build.args_type()[0];
        if data_type != &DataType::Bitmap {
            return Err(ErrorCode::BadDataValueType(format!(
                "{} does not support type '{:?}'",
                build.name(),
                build.args_type()[0]
            )));
        }

        with_bitmap_op_mapped_type!(|OP| match OP_TYPE {
            OP => with_bitmap_result_mapped_type!(|R| match RESULT_TYPE {
                R =>
                    Self::create_nullable_instance::<AggregateBitmapImplementation<OP, R>, R>(build),
                _ => unreachable!(),
            }),
            _ => unreachable!(),
        })
    }

    fn construct_route() -> DirectNameRoute {
        let arguments = Self::bitmap_numeric_arguments();
        let features = Self::BITMAP_CONSTRUCT_AGG_FEATURES;
        DirectNameRoute::new(
            &["bitmap_construct_agg", "group_bitmap"],
            arguments.clone(),
            features.clone(),
            NullPolicy::ReturnsDefaultWhenOnlyNull,
        )
        .then(MergeRoute::new(false, BitmapBuilder::create_group_bitmap))
        .then(MergeRoute::new(true, BitmapBuilder::create_group_bitmap))
        .then(PlainRoute::new(BitmapBuilder::create_group_bitmap))
        .then(IfRoute::new(BitmapBuilder::create_group_bitmap))
        .then(StateRoute::new(BitmapBuilder::create_group_bitmap))
        .then(DistinctAliasRoute::new(BitmapBuilder::create_group_bitmap))
    }

    fn intersect_count_route() -> DirectNameRoute {
        DirectNameRoute::new(
            &["intersect_count"],
            Self::bitmap_intersect_count_arguments(),
            Self::INTERSECT_COUNT_FEATURES,
            NullPolicy::Skip,
        )
        .then(MergeRoute::multi_arg(false, Self::create_intersect_count))
        .then(MergeRoute::multi_arg(true, Self::create_intersect_count))
        .then(PlainRoute::multi_arg(Self::create_intersect_count))
        .then(IfRoute::multi_arg(Self::create_intersect_count))
        .then(StateRoute::multi_arg(Self::create_intersect_count))
        .then(DistinctAliasRoute::multi_arg(Self::create_intersect_count))
    }

    fn create_intersect_count(
        build: MultiArgBuildContext<'_, impl CombinatorImpl>,
    ) -> Result<AggregateFunctionRef> {
        if !(1..=32).contains(&build.params().len()) {
            return Err(ErrorCode::BadArguments(format!(
                "{} expects between 1 and 32 parameters",
                build.name()
            )));
        }
        let [bitmap_type, filter_type] = build.args_type() else {
            unreachable!("bitmap_intersect_count descriptor must provide two arguments")
        };
        if bitmap_type != &DataType::Bitmap {
            return Err(ErrorCode::BadDataValueType(format!(
                "{} the first argument type mismatch, expect: '{:?}', but got: '{:?}'",
                build.name(),
                DataType::Bitmap,
                bitmap_type,
            )));
        }

        let filter_type = filter_type.clone();
        let display_name = build.name().to_string();
        let params = build.params().to_vec();
        with_simple_no_number_mapped_type!(|T| match filter_type {
            DataType::T => Self::create_intersect_count_instance::<T>(
                build,
                extract_params::<T>(&display_name, filter_type, &params)?,
            ),
            DataType::Number(number_type) => {
                with_number_mapped_type!(|N| match number_type {
                    NumberDataType::N => Self::create_intersect_count_instance::<NumberType<N>>(
                        build,
                        extract_number_params::<N>(&params)?,
                    ),
                })
            }
            DataType::Decimal(size) => {
                with_decimal_mapped_type!(|DECIMAL| match size.data_kind() {
                    DecimalDataKind::DECIMAL =>
                        Self::create_intersect_count_instance::<DecimalType<DECIMAL>>(
                            build,
                            extract_params::<DecimalType<DECIMAL>>(
                                &display_name,
                                filter_type,
                                &params,
                            )?,
                        ),
                })
            }
            _ => Self::create_intersect_count_instance::<AnyType>(
                build,
                extract_params::<AnyType>(&display_name, filter_type, &params)?,
            ),
        })
    }

    fn create_group_bitmap(
        build: DirectBuildContext<'_, impl CombinatorImpl>,
    ) -> Result<AggregateFunctionRef> {
        if !build.params().is_empty() {
            return Err(ErrorCode::BadArguments(format!(
                "{} expects no parameters",
                build.name()
            )));
        }

        let data_type = build.args_type()[0].remove_nullable();
        let DataType::Number(number_type) = data_type else {
            return Err(ErrorCode::BadDataValueType(format!(
                "{} does not support type '{:?}', expect unsigned integer",
                build.name(),
                data_type
            )));
        };

        with_unsigned_integer_mapped_type!(|N| match number_type {
            NumberDataType::N => Self::create_raw_instance::<
                AggregateGroupBitmapImplementation<N, BitmapRawResult>,
                BitmapRawResult,
            >(build),
            _ => Err(ErrorCode::BadDataValueType(format!(
                "{} does not support type '{:?}', expect unsigned integer",
                build.name(),
                data_type
            ))),
        })
    }

    fn create_nullable_instance<I, R>(
        build: MultiArgBuildContext<'_, impl CombinatorImpl>,
    ) -> Result<AggregateFunctionRef>
    where
        I: AggrImpl + Default,
        R: BitmapResult,
    {
        let return_type = R::return_type().wrap_nullable();
        let implementation = I::default();

        build.create_multi_arg_or_null(
            return_type,
            AggregateBitmapState::state_description(),
            implementation,
        )
    }

    fn create_raw_instance<I, R>(
        build: DirectBuildContext<'_, impl CombinatorImpl>,
    ) -> Result<AggregateFunctionRef>
    where
        I: AggrImpl + Default,
        R: BitmapResult,
    {
        let has_nullable_input = build.args_type().iter().any(DataType::is_nullable_or_null);
        let implementation = I::default();
        if has_nullable_input {
            return build.create(
                R::return_type(),
                AggregateBitmapState::state_description(),
                AggregateMultiArgSkipNullImplementation::new(implementation),
            );
        }
        build.create(
            R::return_type(),
            AggregateBitmapState::state_description(),
            implementation,
        )
    }

    fn create_intersect_count_instance<T>(
        build: MultiArgBuildContext<'_, impl CombinatorImpl>,
        filter_values: Vec<T::Scalar>,
    ) -> Result<AggregateFunctionRef>
    where
        T: AccessType,
        T::Scalar: Send + Sync,
    {
        build.create_multi_arg_or_null(
            UInt64Type::data_type().wrap_nullable(),
            AggregateBitmapState::state_description(),
            AggregateBitmapIntersectCountImplementation::<T>::new(filter_values),
        )
    }
}

fn extract_params<T>(
    display_name: &str,
    value_type: DataType,
    params: &[Scalar],
) -> Result<Vec<T::Scalar>>
where
    T: AccessType,
{
    let mut filter_values = Vec::with_capacity(params.len());
    for (index, param) in params.iter().enumerate() {
        match T::try_downcast_scalar(&param.as_ref()) {
            Ok(scalar) => filter_values.push(T::to_owned_scalar(scalar)),
            Err(_) => {
                return Err(ErrorCode::BadDataValueType(format!(
                    "{} param({}) type mismatch, expect: '{:?}', but got: '{:?}'",
                    display_name,
                    index,
                    value_type,
                    param.as_ref().infer_data_type()
                )));
            }
        }
    }
    Ok(filter_values)
}

fn extract_number_params<N>(params: &[Scalar]) -> Result<Vec<N>>
where N: Number {
    let mut values = Vec::with_capacity(params.len());
    for param in params {
        values.push(extract_number_param::<N>(param.clone())?);
    }
    Ok(values)
}

fn serialize_bitmap_states(input: SerializeInput<'_>) -> Result<()> {
    for state in input.states.iter() {
        state
            .get::<AggregateBitmapState>()
            .serialize(&mut input.builders[0])?;
    }
    Ok(())
}

fn merge_serialized_bitmap_states<OP>(input: MergeSerializedInput<'_>) -> Result<()>
where OP: BitmapOperate {
    for (row, state) in input.states.iter().enumerate() {
        if input.filter.is_some_and(|filter| !filter.get(row).unwrap()) {
            continue;
        }
        let ScalarRef::Binary(data) = super::serialized_scalar_at(input.state, row, 0) else {
            unreachable!()
        };
        state
            .get::<AggregateBitmapState>()
            .merge_serialized::<OP>(data)?;
    }
    Ok(())
}

fn merge_bitmap_states<OP>(input: MergeStatesInput<'_>)
where OP: BitmapOperate {
    input
        .state
        .get::<AggregateBitmapState>()
        .merge_owned::<OP>(input.rhs.get::<AggregateBitmapState>());
}

unsafe fn drop_bitmap_state(state: AggrState<'_>) {
    unsafe { std::ptr::drop_in_place(state.get::<AggregateBitmapState>()) };
}
