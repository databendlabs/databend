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
use std::fmt::Debug;
use std::marker::PhantomData;
use std::mem;
use std::sync::Arc;

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_column::binary::BinaryColumnBuilder;
use databend_common_exception::Result;
use databend_common_expression::AggrStateRegistry;
use databend_common_expression::AggrStateType;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ColumnView;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::StateSerdeItem;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::DataType;
use databend_common_expression::types::ValueType;
use databend_common_expression::types::date::CoreDate;
use databend_common_expression::types::decimal::*;
use databend_common_expression::types::empty_array::CoreEmptyArray;
use databend_common_expression::types::empty_map::CoreEmptyMap;
use databend_common_expression::types::i256;
use databend_common_expression::types::interval::CoreInterval;
use databend_common_expression::types::null::CoreNull;
use databend_common_expression::types::number::*;
use databend_common_expression::types::simple_type::SimpleType;
use databend_common_expression::types::simple_type::SimpleValueType;
use databend_common_expression::types::string::StringColumnBuilder;
use databend_common_expression::types::timestamp::CoreTimestamp;
use databend_common_expression::types::zero_size_type::ZeroSizeType;
use databend_common_expression::types::zero_size_type::ZeroSizeValueType;
use databend_common_expression::types::*;
use databend_common_expression::with_decimal_mapped_type;
use databend_common_expression::with_number_mapped_type;

use super::AggrState;
use super::AggrStateLoc;
use super::AggregateFunction;
use super::AggregateFunctionDescription;
use super::AggregateFunctionFeatures;
use super::AggregateFunctionSortDesc;
use super::SerializeInfo;
use super::StateAddr;
use super::StateSerde;
use super::aggregate_scalar_state::ScalarStateFunc;
use super::assert_params;
use super::assert_unary_arguments;
use super::batch_merge1;
use super::batch_serialize1;

#[derive(Debug)]
struct ArrayAggStateAny<T>
where T: ValueType
{
    values: Vec<T::Scalar>,
}

impl<T> Default for ArrayAggStateAny<T>
where T: ValueType
{
    fn default() -> Self {
        Self { values: Vec::new() }
    }
}

impl<T> ScalarStateFunc<T> for ArrayAggStateAny<T>
where T: ValueType
{
    fn new() -> Self {
        Self::default()
    }

    fn add(&mut self, other: Option<T::ScalarRef<'_>>) {
        self.values.push(T::to_owned_scalar(other.unwrap()));
    }

    fn add_batch(&mut self, column: ColumnView<T>, _validity: Option<&Bitmap>) -> Result<()> {
        if column.is_empty() {
            return Ok(());
        }

        for val in column.iter() {
            self.values.push(T::to_owned_scalar(val));
        }

        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.values.extend_from_slice(&rhs.values);
        Ok(())
    }

    fn merge_result(&mut self, builder: &mut ColumnBuilder) -> Result<()> {
        let data_type = builder.data_type();
        let inner_type = data_type.as_array().unwrap();

        let mut inner_builder = ColumnBuilder::with_capacity(inner_type, self.values.len());
        let values = mem::take(&mut self.values);
        for value in values.into_iter() {
            let val = T::upcast_scalar_with_type(value, inner_type);
            inner_builder.push(val.as_ref());
        }
        let array_value = ScalarRef::Array(inner_builder.build());
        builder.push(array_value);
        Ok(())
    }
}

impl<T> StateSerde for ArrayAggStateAny<T>
where
    Self: ScalarStateFunc<T>,
    T: ValueType,
{
    fn serialize_type(info: Option<&dyn SerializeInfo>) -> Vec<StateSerdeItem> {
        let return_type = info
            .and_then(|data| data.as_any().downcast_ref::<DataType>())
            .cloned()
            .unwrap();
        vec![return_type.into()]
    }

    fn batch_serialize(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        batch_serialize1::<ArrayType<T>, Self, _>(places, loc, builders, |state, builder| {
            for v in &state.values {
                builder.put_item(T::to_scalar_ref(v));
            }
            builder.commit_row();
            Ok(())
        })
    }

    fn batch_merge(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        state: &BlockEntry,
        filter: Option<&Bitmap>,
    ) -> Result<()> {
        batch_merge1::<ArrayType<T>, Self, _>(places, loc, state, filter, |state, values| {
            for val in T::iter_column(&values) {
                state.values.push(T::to_owned_scalar(val));
            }
            Ok(())
        })
    }
}

#[derive(Debug)]
struct ArrayAggStateSimple<T>
where T: SimpleType
{
    values: Vec<T::Scalar>,
}

impl<T: Debug + SimpleType> Default for ArrayAggStateSimple<T> {
    fn default() -> Self {
        Self { values: Vec::new() }
    }
}

impl<T> ScalarStateFunc<SimpleValueType<T>> for ArrayAggStateSimple<T>
where T: SimpleType + Debug
{
    fn new() -> Self {
        Self::default()
    }

    fn add(&mut self, other: Option<T::Scalar>) {
        if let Some(scalar) = other {
            self.values.push(scalar);
        }
    }

    fn add_batch(
        &mut self,
        column: ColumnView<SimpleValueType<T>>,
        validity: Option<&Bitmap>,
    ) -> Result<()> {
        if column.is_empty() {
            return Ok(());
        }

        if let Some(validity) = validity {
            for (value, valid) in column.iter().zip(validity) {
                if valid {
                    self.values.push(value);
                }
            }
        } else {
            self.values.extend(column.iter());
        }
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.values.extend_from_slice(&rhs.values);

        Ok(())
    }

    fn merge_result(&mut self, builder: &mut ColumnBuilder) -> Result<()> {
        let data_type = builder.data_type();
        let inner_type = data_type.as_array().unwrap();

        let column = mem::take(&mut self.values).into();
        let item = SimpleValueType::<T>::upcast_column_with_type(column, inner_type);

        builder.push(ScalarRef::Array(item));
        Ok(())
    }
}

impl<T> StateSerde for ArrayAggStateSimple<T>
where T: SimpleType
{
    fn serialize_type(info: Option<&dyn SerializeInfo>) -> Vec<StateSerdeItem> {
        let data_type = info
            .and_then(|data| data.as_any().downcast_ref::<DataType>())
            .and_then(|ty| ty.as_array())
            .unwrap()
            .remove_nullable();

        vec![DataType::Array(Box::new(data_type)).into()]
    }

    fn batch_serialize(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        batch_serialize1::<ArrayType<SimpleValueType<T>>, Self, _>(
            places,
            loc,
            builders,
            |state, builder| {
                for value in &state.values {
                    builder.put_item(*value)
                }
                builder.commit_row();
                Ok(())
            },
        )
    }

    fn batch_merge(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        state: &BlockEntry,
        filter: Option<&Bitmap>,
    ) -> Result<()> {
        batch_merge1::<ArrayType<SimpleValueType<T>>, Self, _>(
            places,
            loc,
            state,
            filter,
            |state, data| {
                let rhs = Self {
                    values: SimpleValueType::<T>::column_to_builder(data),
                };
                <Self as ScalarStateFunc<SimpleValueType<T>>>::merge(state, &rhs)
            },
        )
    }
}

#[derive(Debug)]
struct ArrayAggStateZST<const IS_NULL: bool> {
    validity: MutableBitmap,
}

impl<V, const IS_NULL: bool> ScalarStateFunc<ZeroSizeValueType<V>> for ArrayAggStateZST<IS_NULL>
where V: ZeroSizeType
{
    fn new() -> Self {
        Self {
            validity: Default::default(),
        }
    }

    fn add(&mut self, other: Option<()>) {
        if !IS_NULL && other.is_some() {
            self.validity.push(true);
        }
    }

    fn add_batch(
        &mut self,
        column: ColumnView<ZeroSizeValueType<V>>,
        validity: Option<&Bitmap>,
    ) -> Result<()> {
        if IS_NULL || column.is_empty() {
            return Ok(());
        }

        if let Some(validity) = validity {
            for valid in validity {
                if valid {
                    self.validity.push(true);
                }
            }
        } else {
            let length = match column {
                ColumnView::Const(_, n) => n,
                ColumnView::Column(n) => n,
            };
            self.validity.extend_constant(length, true);
        }
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.validity
            .extend_from_slice(rhs.validity.as_slice(), 0, rhs.validity.len());

        Ok(())
    }

    fn merge_result(&mut self, builder: &mut ColumnBuilder) -> Result<()> {
        let item = V::upcast_column(self.validity.len());
        builder.push(ScalarRef::Array(item));
        Ok(())
    }
}

impl<const IS_NULL: bool> StateSerde for ArrayAggStateZST<IS_NULL> {
    fn serialize_type(_: Option<&dyn SerializeInfo>) -> Vec<StateSerdeItem> {
        vec![ArrayType::<BooleanType>::data_type().into()]
    }

    fn batch_serialize(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        batch_serialize1::<ArrayType<BooleanType>, Self, _>(
            places,
            loc,
            builders,
            |state, builder| {
                builder.push_item(state.validity.clone().freeze());
                Ok(())
            },
        )
    }

    fn batch_merge(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        state: &BlockEntry,
        filter: Option<&Bitmap>,
    ) -> Result<()> {
        batch_merge1::<ArrayType<BooleanType>, Self, _>(
            places,
            loc,
            state,
            filter,
            |state, data| {
                state.validity.extend_from_bitmap(&data);
                Ok(())
            },
        )
    }
}

#[derive(Debug)]
struct ArrayAggStateBinary<T>
where T: ArgType
{
    builder: BinaryColumnBuilder,
    _phantom: PhantomData<T>,
}

impl<T: Debug + ArgType> Default for ArrayAggStateBinary<T> {
    fn default() -> Self {
        Self {
            builder: BinaryColumnBuilder::with_capacity(0, 0),
            _phantom: PhantomData,
        }
    }
}

impl<T> ScalarStateFunc<T> for ArrayAggStateBinary<T>
where T: ArgType + Debug + std::marker::Send
{
    fn new() -> Self {
        Self::default()
    }

    fn add(&mut self, other: Option<T::ScalarRef<'_>>) {
        if let Some(scalar) = other {
            let val = T::upcast_scalar(T::to_owned_scalar(scalar));
            self.builder.put_slice(val.as_bytes().unwrap());
            self.builder.commit_row();
        }
    }

    fn add_batch(&mut self, column: ColumnView<T>, validity: Option<&Bitmap>) -> Result<()> {
        let length = column.len();
        if length == 0 {
            return Ok(());
        }

        if let Some(validity) = validity {
            for (scalar, valid) in column.iter().zip(validity) {
                if valid {
                    let val = T::upcast_scalar(T::to_owned_scalar(scalar));
                    self.builder.put_slice(val.as_bytes().unwrap());
                    self.builder.commit_row();
                }
            }
        } else {
            for scalar in column.iter() {
                let val = T::upcast_scalar(T::to_owned_scalar(scalar));
                self.builder.put_slice(val.as_bytes().unwrap());
                self.builder.commit_row();
            }
        }

        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.builder.append_column(&rhs.builder.clone().build());

        Ok(())
    }

    fn merge_result(&mut self, builder: &mut ColumnBuilder) -> Result<()> {
        let data_type = builder.data_type();
        let inner_type = data_type.as_array().unwrap();

        let binary_column = self.builder.clone().build();
        let item = match &**inner_type {
            DataType::String => {
                let builder = StringColumnBuilder::try_from_bin_column(binary_column)?;
                Column::String(builder.build())
            }
            DataType::Binary => Column::Binary(binary_column),
            DataType::Bitmap => Column::Bitmap(binary_column),
            DataType::Variant => Column::Variant(binary_column),
            DataType::Geometry => Column::Geometry(binary_column),
            DataType::Geography => Column::Geography(GeographyColumn(binary_column)),
            _ => unreachable!(),
        };

        builder.push(ScalarRef::Array(item));
        Ok(())
    }
}

impl<T> StateSerde for ArrayAggStateBinary<T>
where T: ArgType + std::marker::Send
{
    fn serialize_type(info: Option<&dyn SerializeInfo>) -> Vec<StateSerdeItem> {
        let data_type = info
            .and_then(|data| data.as_any().downcast_ref::<DataType>())
            .and_then(|ty| ty.as_array())
            .unwrap()
            .remove_nullable();

        vec![DataType::Array(Box::new(data_type)).into()]
    }

    fn batch_serialize(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        batch_serialize1::<ArrayType<T>, Self, _>(places, loc, builders, |state, builder| {
            let binary_column = state.builder.clone().build();
            let offsets = vec![0, binary_column.len() as u64];

            let column = match T::data_type() {
                DataType::String => {
                    let builder = StringColumnBuilder::try_from_bin_column(binary_column)?;
                    Column::String(builder.build())
                }
                DataType::Binary => Column::Binary(binary_column),
                DataType::Bitmap => Column::Bitmap(binary_column),
                DataType::Variant => Column::Variant(binary_column),
                DataType::Geometry => Column::Geometry(binary_column),
                DataType::Geography => Column::Geography(GeographyColumn(binary_column)),
                _ => unreachable!(),
            };
            let column = T::try_downcast_column(&column).unwrap();
            let array_column = ArrayColumn::new(column, offsets.into());
            builder.append_column(&array_column);

            Ok(())
        })
    }

    fn batch_merge(
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        state: &BlockEntry,
        filter: Option<&Bitmap>,
    ) -> Result<()> {
        batch_merge1::<ArrayType<T>, Self, _>(places, loc, state, filter, |state, data| {
            let builder = T::column_to_builder(data);
            let column_builder = T::try_upcast_column_builder(builder, &T::data_type()).unwrap();
            let binary_builder = match column_builder {
                ColumnBuilder::String(string_builder) => {
                    let mut builder = BinaryColumnBuilder::with_capacity(
                        string_builder.len(),
                        string_builder.memory_size(),
                    );
                    for i in 0..string_builder.len() {
                        let s = unsafe { string_builder.index_unchecked(i) };
                        builder.put_str(s);
                        builder.commit_row();
                    }
                    builder
                }
                ColumnBuilder::Binary(builder)
                | ColumnBuilder::Bitmap(builder)
                | ColumnBuilder::Variant(builder)
                | ColumnBuilder::Geometry(builder)
                | ColumnBuilder::Geography(builder) => builder,
                _ => unreachable!(),
            };

            let rhs = Self {
                builder: binary_builder,
                _phantom: PhantomData,
            };

            state.merge(&rhs)
        })
    }
}

#[derive(Clone)]
struct AggregateArrayAggFunction<T, State> {
    display_name: String,
    return_type: DataType,
    _p: PhantomData<fn(T, State)>,
}

impl<T, State> AggregateFunction for AggregateArrayAggFunction<T, State>
where
    T: AccessType,
    State: ScalarStateFunc<T>,
{
    fn name(&self) -> &str {
        "AggregateArrayAggFunction"
    }

    fn return_type(&self) -> Result<DataType> {
        Ok(self.return_type.clone())
    }

    fn init_state(&self, place: AggrState) {
        place.write(State::new);
    }

    fn register_state(&self, registry: &mut AggrStateRegistry) {
        registry.register(AggrStateType::Custom(Layout::new::<State>()));
    }

    fn accumulate(
        &self,
        place: AggrState,
        columns: ProjectedBlock,
        _validity: Option<&Bitmap>,
        _input_rows: usize,
    ) -> Result<()> {
        let state = place.get::<State>();
        match &columns[0] {
            BlockEntry::Const(Scalar::Null, DataType::Nullable(_), _) => Ok(()),
            entry @ BlockEntry::Const(_, _, _) => {
                let column = entry.clone().remove_nullable().downcast().unwrap();
                state.add_batch(column, None)
            }
            BlockEntry::Column(Column::Nullable(box nullable_column)) => {
                let c = T::try_downcast_column(nullable_column.column()).unwrap();
                state.add_batch(ColumnView::Column(c), Some(nullable_column.validity()))
            }
            entry => state.add_batch(entry.downcast().unwrap(), None),
        }
    }

    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        block: ProjectedBlock,
        _input_rows: usize,
    ) -> Result<()> {
        let entry = &block[0];
        if entry.data_type().is_nullable() {
            entry
                .downcast::<NullableType<T>>()
                .unwrap()
                .iter()
                .zip(places.iter())
                .for_each(|(v, place)| {
                    let state = AggrState::new(*place, loc).get::<State>();
                    state.add(v)
                });
        } else {
            entry
                .downcast::<T>()
                .unwrap()
                .iter()
                .zip(places.iter())
                .for_each(|(v, place)| {
                    let state = AggrState::new(*place, loc).get::<State>();
                    state.add(Some(v))
                });
        }
        Ok(())
    }

    fn accumulate_row(&self, place: AggrState, block: ProjectedBlock, row: usize) -> Result<()> {
        let state = place.get::<State>();
        let entry = &block[0];

        if entry.data_type().is_nullable() {
            let view = entry.downcast::<NullableType<T>>().unwrap();
            let v = view.index(row).unwrap();
            state.add(v);
        } else {
            let view = entry.downcast::<T>().unwrap();
            let v = view.index(row).unwrap();
            state.add(Some(v));
        }

        Ok(())
    }

    fn serialize_type(&self) -> Vec<StateSerdeItem> {
        State::serialize_type(Some(&self.return_type))
    }

    fn batch_serialize(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        builders: &mut [ColumnBuilder],
    ) -> Result<()> {
        State::batch_serialize(places, loc, builders)
    }

    fn batch_merge(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        state: &BlockEntry,
        filter: Option<&Bitmap>,
    ) -> Result<()> {
        State::batch_merge(places, loc, state, filter)
    }

    fn merge_states(&self, place: AggrState, rhs: AggrState) -> Result<()> {
        let state = place.get::<State>();
        let other = rhs.get::<State>();
        state.merge(other)
    }

    fn merge_result(
        &self,
        place: AggrState,
        _read_only: bool,
        builder: &mut ColumnBuilder,
    ) -> Result<()> {
        let state = place.get::<State>();
        state.merge_result(builder)
    }

    fn need_manual_drop_state(&self) -> bool {
        true
    }

    unsafe fn drop_state(&self, place: AggrState) {
        let state = place.get::<State>();
        unsafe { std::ptr::drop_in_place(state) };
    }
}

impl<T, State> fmt::Display for AggregateArrayAggFunction<T, State> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

impl<T, State> AggregateArrayAggFunction<T, State>
where
    T: ValueType,
    State: ScalarStateFunc<T>,
{
    fn create(display_name: &str, return_type: DataType) -> Result<Arc<dyn AggregateFunction>> {
        let func = AggregateArrayAggFunction::<T, State> {
            display_name: display_name.to_string(),
            return_type,
            _p: PhantomData,
        };
        Ok(Arc::new(func))
    }
}

fn try_create_aggregate_array_agg_function(
    display_name: &str,
    params: Vec<Scalar>,
    argument_types: Vec<DataType>,
    _sort_descs: Vec<AggregateFunctionSortDesc>,
) -> Result<Arc<dyn AggregateFunction>> {
    assert_params(display_name, params.len(), 0)?;
    assert_unary_arguments(display_name, argument_types.len())?;
    let data_type = argument_types[0].clone();
    let not_null_type = data_type.remove_nullable();
    let return_type = DataType::Array(Box::new(not_null_type.clone()));

    fn simple<V>(display_name: &str, return_type: DataType) -> Result<Arc<dyn AggregateFunction>>
    where
        V: SimpleType,
        V::Scalar: BorshSerialize + BorshDeserialize,
    {
        AggregateArrayAggFunction::<SimpleValueType<V>, ArrayAggStateSimple<V>>::create(
            display_name,
            return_type,
        )
    }

    type ArrayAggrZST<V, const N: bool> =
        AggregateArrayAggFunction<ZeroSizeValueType<V>, ArrayAggStateZST<N>>;

    match not_null_type {
        DataType::Number(num_type) => {
            with_number_mapped_type!(|NUM| match num_type {
                NumberDataType::NUM => {
                    simple::<CoreNumber<NUM>>(display_name, return_type)
                }
            })
        }
        DataType::Decimal(size) => {
            with_decimal_mapped_type!(|DECIMAL| match size.data_kind() {
                DecimalDataKind::DECIMAL => {
                    simple::<CoreDecimal<DECIMAL>>(display_name, return_type)
                }
            })
        }
        DataType::Date => simple::<CoreDate>(display_name, return_type),
        DataType::Timestamp => simple::<CoreTimestamp>(display_name, return_type),
        DataType::Interval => simple::<CoreInterval>(display_name, return_type),

        DataType::Null => ArrayAggrZST::<CoreNull, true>::create(display_name, return_type),
        DataType::EmptyArray => {
            ArrayAggrZST::<CoreEmptyArray, false>::create(display_name, return_type)
        }
        DataType::EmptyMap => {
            ArrayAggrZST::<CoreEmptyMap, false>::create(display_name, return_type)
        }

        DataType::Boolean => {
            type State = ArrayAggStateAny<BooleanType>;
            AggregateArrayAggFunction::<BooleanType, State>::create(display_name, return_type)
        }
        DataType::String => {
            type State = ArrayAggStateBinary<StringType>;
            AggregateArrayAggFunction::<StringType, State>::create(display_name, return_type)
        }
        DataType::Binary => {
            type State = ArrayAggStateBinary<BinaryType>;
            AggregateArrayAggFunction::<BinaryType, State>::create(display_name, return_type)
        }
        DataType::Bitmap => {
            type State = ArrayAggStateBinary<BitmapType>;
            AggregateArrayAggFunction::<BitmapType, State>::create(display_name, return_type)
        }
        DataType::Variant => {
            type State = ArrayAggStateBinary<VariantType>;
            AggregateArrayAggFunction::<VariantType, State>::create(display_name, return_type)
        }
        DataType::Geometry => {
            type State = ArrayAggStateBinary<GeometryType>;
            AggregateArrayAggFunction::<GeometryType, State>::create(display_name, return_type)
        }
        DataType::Geography => {
            type State = ArrayAggStateBinary<GeographyType>;
            AggregateArrayAggFunction::<GeographyType, State>::create(display_name, return_type)
        }

        DataType::Nullable(_) | DataType::Generic(_) => unreachable!(),
        _ => {
            type State = ArrayAggStateAny<AnyType>;
            AggregateArrayAggFunction::<AnyType, State>::create(display_name, return_type)
        }
    }
}

pub fn aggregate_array_agg_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator_with_features(
        Box::new(try_create_aggregate_array_agg_function),
        AggregateFunctionFeatures {
            allow_sort: true,
            keep_nullable: true,
            ..Default::default()
        },
    )
}
