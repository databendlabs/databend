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
use databend_common_exception::Result;
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
use databend_common_expression::types::timestamp::CoreTimestamp;
use databend_common_expression::types::zero_size_type::ZeroSizeType;
use databend_common_expression::types::zero_size_type::ZeroSizeValueType;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::DataType;
use databend_common_expression::types::ValueType;
use databend_common_expression::types::*;
use databend_common_expression::with_number_mapped_type;
use databend_common_expression::AggrStateRegistry;
use databend_common_expression::AggrStateType;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::InputColumns;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;

use super::aggregate_function_factory::AggregateFunctionDescription;
use super::aggregate_scalar_state::ScalarStateFunc;
use super::AggregateFunctionSortDesc;
use super::StateAddr;
use crate::aggregates::assert_unary_arguments;
use crate::aggregates::AggrState;
use crate::aggregates::AggrStateLoc;
use crate::aggregates::AggregateFunction;

#[derive(BorshSerialize, BorshDeserialize, Debug)]
struct ArrayAggStateAny<T>
where
    T: ValueType,
    T::Scalar: BorshSerialize + BorshDeserialize,
{
    values: Vec<T::Scalar>,
}

impl<T> Default for ArrayAggStateAny<T>
where
    T: ValueType,
    T::Scalar: BorshSerialize + BorshDeserialize,
{
    fn default() -> Self {
        Self { values: Vec::new() }
    }
}

impl<T> ScalarStateFunc<T> for ArrayAggStateAny<T>
where
    T: ValueType,
    T::Scalar: BorshSerialize + BorshDeserialize + Send,
{
    fn new() -> Self {
        Self::default()
    }

    fn add(&mut self, other: Option<T::ScalarRef<'_>>) {
        self.values.push(T::to_owned_scalar(other.unwrap()));
    }

    fn add_batch(&mut self, column: &T::Column, _validity: Option<&Bitmap>) -> Result<()> {
        let column_len = T::column_len(column);
        if column_len == 0 {
            return Ok(());
        }
        let column_iter = T::iter_column(column);
        for val in column_iter {
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

#[derive(BorshSerialize, BorshDeserialize, Debug)]
struct NullableArrayAggStateAny<T>
where
    T: ValueType,
    T::Scalar: BorshSerialize + BorshDeserialize,
{
    values: Vec<Option<T::Scalar>>,
}

impl<T> Default for NullableArrayAggStateAny<T>
where
    T: ValueType,
    T::Scalar: BorshSerialize + BorshDeserialize,
{
    fn default() -> Self {
        Self { values: Vec::new() }
    }
}

impl<T> ScalarStateFunc<T> for NullableArrayAggStateAny<T>
where
    T: ValueType,
    T::Scalar: BorshSerialize + BorshDeserialize + Send,
{
    fn new() -> Self {
        Self::default()
    }

    fn add(&mut self, other: Option<T::ScalarRef<'_>>) {
        match other {
            Some(other) => {
                self.values.push(Some(T::to_owned_scalar(other)));
            }
            None => {
                self.values.push(None);
            }
        }
    }

    fn add_batch(&mut self, column: &T::Column, validity: Option<&Bitmap>) -> Result<()> {
        let column_len = T::column_len(column);
        if column_len == 0 {
            return Ok(());
        }
        let column_iter = T::iter_column(column);
        if let Some(validity) = validity {
            for (val, valid) in column_iter.zip(validity.iter()) {
                if valid {
                    self.values.push(Some(T::to_owned_scalar(val)));
                } else {
                    self.values.push(None);
                }
            }
        } else {
            for val in column_iter {
                self.values.push(Some(T::to_owned_scalar(val)));
            }
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
        for value in &self.values {
            match value {
                Some(value) => {
                    let val =
                        T::upcast_scalar_with_type(value.clone(), &inner_type.remove_nullable());
                    inner_builder.push(val.as_ref());
                }
                None => {
                    inner_builder.push(ScalarRef::Null);
                }
            }
        }

        let array_value = ScalarRef::Array(inner_builder.build());
        builder.push(array_value);
        Ok(())
    }
}

#[derive(Debug)]
struct ArrayAggStateSimple<T, const NULLABLE: bool>
where T: Debug
{
    values: Vec<T>,
    validity: MutableBitmap,
}

impl<T, const NULLABLE: bool> BorshSerialize for ArrayAggStateSimple<T, NULLABLE>
where T: Debug + BorshSerialize
{
    fn serialize<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        if NULLABLE {
            (
                &self.values,
                Column::Boolean(self.validity.clone().freeze()),
            )
                .serialize(writer)
        } else {
            self.values.serialize(writer)
        }
    }
}

impl<T, const NULLABLE: bool> BorshDeserialize for ArrayAggStateSimple<T, NULLABLE>
where T: Debug + BorshDeserialize
{
    fn deserialize_reader<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self> {
        if NULLABLE {
            let (values, Column::Boolean(validity)) = BorshDeserialize::deserialize_reader(reader)?
            else {
                unreachable!()
            };
            Ok(Self {
                values,
                validity: validity.make_mut(),
            })
        } else {
            let values = BorshDeserialize::deserialize_reader(reader)?;
            Ok(Self {
                values,
                ..Default::default()
            })
        }
    }
}

impl<T: Debug, const NULLABLE: bool> Default for ArrayAggStateSimple<T, NULLABLE> {
    fn default() -> Self {
        Self {
            values: Vec::new(),
            validity: MutableBitmap::new(),
        }
    }
}

impl<V, const NULLABLE: bool> ScalarStateFunc<SimpleValueType<V>>
    for ArrayAggStateSimple<V::Scalar, NULLABLE>
where
    V: SimpleType,
    Self: BorshSerialize + BorshDeserialize,
{
    fn new() -> Self {
        Self::default()
    }

    fn add(&mut self, other: Option<V::Scalar>) {
        match other {
            Some(scalar) => {
                self.values.push(scalar);
                if NULLABLE {
                    self.validity.push(true);
                }
            }
            None if NULLABLE => {
                self.values.push(V::Scalar::default());
                self.validity.push(false);
            }
            _ => unreachable!(),
        }
    }

    fn add_batch(&mut self, column: &Buffer<V::Scalar>, validity: Option<&Bitmap>) -> Result<()> {
        let length = column.len();
        if length == 0 {
            return Ok(());
        }

        if let Some(validity) = validity {
            for (value, valid) in column.iter().zip(validity) {
                if valid {
                    self.values.push(*value);
                    self.validity.push(true);
                } else {
                    self.values.push(V::Scalar::default());
                    self.validity.push(false);
                }
            }
        } else {
            self.values.extend(column.iter().copied());
            if NULLABLE {
                self.validity.extend_constant(length, true);
            }
        }
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.values.extend_from_slice(&rhs.values);
        self.validity
            .extend_from_slice(rhs.validity.as_slice(), 0, rhs.validity.len());

        Ok(())
    }

    fn merge_result(&mut self, builder: &mut ColumnBuilder) -> Result<()> {
        let data_type = builder.data_type();
        let inner_type = data_type.as_array().unwrap();

        let column = mem::take(&mut self.values).into();
        let item = if !NULLABLE {
            SimpleValueType::<V>::upcast_column_with_type(column, inner_type)
        } else {
            let column = SimpleValueType::<V>::upcast_column_with_type(
                column,
                &inner_type.remove_nullable(),
            );
            Column::Nullable(Box::new(NullableColumn::new(
                column,
                mem::take(&mut self.validity).freeze(),
            )))
        };

        builder.push(ScalarRef::Array(item));
        Ok(())
    }
}

#[derive(Debug)]
struct ArrayAggStateZST<const NULLABLE: bool> {
    validity: MutableBitmap,
}

impl<const NULLABLE: bool> BorshSerialize for ArrayAggStateZST<NULLABLE> {
    fn serialize<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        Column::Boolean(self.validity.clone().freeze()).serialize(writer)
    }
}

impl<const NULLABLE: bool> BorshDeserialize for ArrayAggStateZST<NULLABLE> {
    fn deserialize_reader<R: std::io::Read>(reader: &mut R) -> std::io::Result<Self> {
        let Column::Boolean(validity) = BorshDeserialize::deserialize_reader(reader)? else {
            unreachable!()
        };
        Ok(Self {
            validity: validity.make_mut(),
        })
    }
}

impl<V, const NULLABLE: bool> ScalarStateFunc<ZeroSizeValueType<V>> for ArrayAggStateZST<NULLABLE>
where
    V: ZeroSizeType,
    Self: BorshSerialize + BorshDeserialize,
{
    fn new() -> Self {
        Self {
            validity: Default::default(),
        }
    }

    fn add(&mut self, other: Option<()>) {
        if other.is_some() {
            self.validity.push(true);
        } else if !NULLABLE {
            unreachable!()
        } else {
            self.validity.push(false);
        }
    }

    fn add_batch(&mut self, length: &usize, validity: Option<&Bitmap>) -> Result<()> {
        if *length == 0 {
            return Ok(());
        }

        if let Some(validity) = validity {
            for valid in validity {
                if valid {
                    self.validity.push(true);
                } else if !NULLABLE {
                    unreachable!()
                } else {
                    self.validity.push(false);
                }
            }
        } else {
            self.validity.extend_constant(*length, true);
        }
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.validity
            .extend_from_slice(rhs.validity.as_slice(), 0, rhs.validity.len());

        Ok(())
    }

    fn merge_result(&mut self, builder: &mut ColumnBuilder) -> Result<()> {
        let item = if !NULLABLE {
            V::upcast_column(self.validity.len())
        } else {
            Column::Nullable(Box::new(NullableColumn::new(
                V::upcast_column(self.validity.len()),
                mem::take(&mut self.validity).freeze(),
            )))
        };

        builder.push(ScalarRef::Array(item));
        Ok(())
    }
}

#[derive(Clone)]
pub struct AggregateArrayAggFunction<T, State> {
    display_name: String,
    return_type: DataType,
    _t: PhantomData<T>,
    _state: PhantomData<State>,
}

unsafe impl<T, State> Send for AggregateArrayAggFunction<T, State> {}
unsafe impl<T, State> Sync for AggregateArrayAggFunction<T, State> {}

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
        columns: InputColumns,
        _validity: Option<&Bitmap>,
        _input_rows: usize,
    ) -> Result<()> {
        let state = place.get::<State>();
        match &columns[0] {
            Column::Nullable(box nullable_column) => {
                let column = T::try_downcast_column(&nullable_column.column).unwrap();
                state.add_batch(&column, Some(&nullable_column.validity))
            }
            _ => {
                let column = T::try_downcast_column(&columns[0]).unwrap();
                state.add_batch(&column, None)
            }
        }
    }

    fn accumulate_keys(
        &self,
        places: &[StateAddr],
        loc: &[AggrStateLoc],
        columns: InputColumns,
        _input_rows: usize,
    ) -> Result<()> {
        match &columns[0] {
            Column::Nullable(box nullable_column) => {
                let column = T::try_downcast_column(&nullable_column.column).unwrap();
                let column_iter = T::iter_column(&column);
                column_iter
                    .zip(nullable_column.validity.iter().zip(places.iter()))
                    .for_each(|(v, (valid, place))| {
                        let state = AggrState::new(*place, loc).get::<State>();
                        if valid {
                            state.add(Some(v.clone()))
                        } else {
                            state.add(None)
                        }
                    });
            }
            _ => {
                let column = T::try_downcast_column(&columns[0]).unwrap();
                let column_iter = T::iter_column(&column);
                column_iter.zip(places.iter()).for_each(|(v, place)| {
                    let state = AggrState::new(*place, loc).get::<State>();
                    state.add(Some(v.clone()))
                });
            }
        }

        Ok(())
    }

    fn accumulate_row(&self, place: AggrState, columns: InputColumns, row: usize) -> Result<()> {
        let state = place.get::<State>();
        match &columns[0] {
            Column::Nullable(box nullable_column) => {
                let valid = nullable_column.validity.get_bit(row);
                if valid {
                    let column = T::try_downcast_column(&nullable_column.column).unwrap();
                    let v = T::index_column(&column, row);
                    state.add(v);
                } else {
                    state.add(None);
                }
            }
            _ => {
                let column = T::try_downcast_column(&columns[0]).unwrap();
                let v = T::index_column(&column, row);
                state.add(v);
            }
        }

        Ok(())
    }

    fn serialize(&self, place: AggrState, writer: &mut Vec<u8>) -> Result<()> {
        let state = place.get::<State>();
        Ok(state.serialize(writer)?)
    }

    fn merge(&self, place: AggrState, reader: &mut &[u8]) -> Result<()> {
        let state = place.get::<State>();
        let rhs = State::deserialize_reader(reader)?;

        state.merge(&rhs)
    }

    fn merge_states(&self, place: AggrState, rhs: AggrState) -> Result<()> {
        let state = place.get::<State>();
        let other = rhs.get::<State>();
        state.merge(other)
    }

    fn merge_result(&self, place: AggrState, builder: &mut ColumnBuilder) -> Result<()> {
        let state = place.get::<State>();
        state.merge_result(builder)
    }

    fn need_manual_drop_state(&self) -> bool {
        true
    }

    unsafe fn drop_state(&self, place: AggrState) {
        let state = place.get::<State>();
        std::ptr::drop_in_place(state);
    }
}

impl<T, State> fmt::Display for AggregateArrayAggFunction<T, State> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", self.display_name)
    }
}

impl<T, State> AggregateArrayAggFunction<T, State>
where
    T: ValueType + Send + Sync,
    State: ScalarStateFunc<T>,
{
    fn create(display_name: &str, return_type: DataType) -> Result<Arc<dyn AggregateFunction>> {
        let func = AggregateArrayAggFunction::<T, State> {
            display_name: display_name.to_string(),
            return_type,
            _t: PhantomData,
            _state: PhantomData,
        };
        Ok(Arc::new(func))
    }
}

fn try_create_aggregate_array_agg_function(
    display_name: &str,
    _params: Vec<Scalar>,
    argument_types: Vec<DataType>,
    _sort_descs: Vec<AggregateFunctionSortDesc>,
) -> Result<Arc<dyn AggregateFunction>> {
    assert_unary_arguments(display_name, argument_types.len())?;
    let data_type = argument_types[0].clone();
    let is_nullable = data_type.is_nullable_or_null();
    let return_type = DataType::Array(Box::new(data_type.clone()));
    let not_null_type = data_type.remove_nullable();

    fn simple<V>(
        display_name: &str,
        return_type: DataType,
        nullable: bool,
    ) -> Result<Arc<dyn AggregateFunction>>
    where
        V: SimpleType + Send + Sync,
        V::Scalar: BorshSerialize + BorshDeserialize,
    {
        if nullable {
            AggregateArrayAggFunction::<
                SimpleValueType<V>,
                ArrayAggStateSimple<V::Scalar, true>,
            >::create(display_name, return_type)
        } else {
            AggregateArrayAggFunction::<
                SimpleValueType<V>,
                ArrayAggStateSimple<V::Scalar, false>,
            >::create(display_name, return_type)
        }
    }

    type ArrayAggrZST<V, const N: bool> =
        AggregateArrayAggFunction<ZeroSizeValueType<V>, ArrayAggStateZST<N>>;

    match not_null_type {
        DataType::Number(num_type) => {
            with_number_mapped_type!(|NUM| match num_type {
                NumberDataType::NUM => {
                    simple::<CoreNumber<NUM>>(display_name, return_type, is_nullable)
                }
            })
        }
        DataType::Decimal(size) => {
            if size.can_carried_by_128() {
                simple::<CoreDecimal<i128>>(display_name, return_type, is_nullable)
            } else {
                simple::<CoreDecimal<i256>>(display_name, return_type, is_nullable)
            }
        }
        DataType::Date => simple::<CoreDate>(display_name, return_type, is_nullable),
        DataType::Timestamp => simple::<CoreTimestamp>(display_name, return_type, is_nullable),
        DataType::Interval => simple::<CoreInterval>(display_name, return_type, is_nullable),

        DataType::Null => ArrayAggrZST::<CoreNull, false>::create(display_name, return_type),
        DataType::EmptyArray => {
            if is_nullable {
                ArrayAggrZST::<CoreEmptyArray, true>::create(display_name, return_type)
            } else {
                ArrayAggrZST::<CoreEmptyArray, false>::create(display_name, return_type)
            }
        }
        DataType::EmptyMap => {
            if is_nullable {
                ArrayAggrZST::<CoreEmptyMap, true>::create(display_name, return_type)
            } else {
                ArrayAggrZST::<CoreEmptyMap, false>::create(display_name, return_type)
            }
        }

        DataType::String => {
            if is_nullable {
                type State = NullableArrayAggStateAny<StringType>;
                AggregateArrayAggFunction::<StringType, State>::create(display_name, return_type)
            } else {
                type State = ArrayAggStateAny<StringType>;
                AggregateArrayAggFunction::<StringType, State>::create(display_name, return_type)
            }
        }
        DataType::Boolean => {
            if is_nullable {
                type State = NullableArrayAggStateAny<BooleanType>;
                AggregateArrayAggFunction::<BooleanType, State>::create(display_name, return_type)
            } else {
                type State = ArrayAggStateAny<BooleanType>;
                AggregateArrayAggFunction::<BooleanType, State>::create(display_name, return_type)
            }
        }

        DataType::Binary => {
            if is_nullable {
                type State = NullableArrayAggStateAny<BinaryType>;
                AggregateArrayAggFunction::<BinaryType, State>::create(display_name, return_type)
            } else {
                type State = ArrayAggStateAny<BinaryType>;
                AggregateArrayAggFunction::<BinaryType, State>::create(display_name, return_type)
            }
        }
        DataType::Bitmap => {
            if is_nullable {
                type State = NullableArrayAggStateAny<BitmapType>;
                AggregateArrayAggFunction::<BitmapType, State>::create(display_name, return_type)
            } else {
                type State = ArrayAggStateAny<BitmapType>;
                AggregateArrayAggFunction::<BitmapType, State>::create(display_name, return_type)
            }
        }
        DataType::Variant => {
            if is_nullable {
                type State = NullableArrayAggStateAny<VariantType>;
                AggregateArrayAggFunction::<VariantType, State>::create(display_name, return_type)
            } else {
                type State = ArrayAggStateAny<VariantType>;
                AggregateArrayAggFunction::<VariantType, State>::create(display_name, return_type)
            }
        }
        DataType::Geometry => {
            if is_nullable {
                type State = NullableArrayAggStateAny<GeometryType>;
                AggregateArrayAggFunction::<GeometryType, State>::create(display_name, return_type)
            } else {
                type State = ArrayAggStateAny<GeometryType>;
                AggregateArrayAggFunction::<GeometryType, State>::create(display_name, return_type)
            }
        }
        DataType::Geography => {
            if is_nullable {
                type State = NullableArrayAggStateAny<GeographyType>;
                AggregateArrayAggFunction::<GeographyType, State>::create(display_name, return_type)
            } else {
                type State = ArrayAggStateAny<GeographyType>;
                AggregateArrayAggFunction::<GeographyType, State>::create(display_name, return_type)
            }
        }

        DataType::Nullable(_) | DataType::Generic(_) => unreachable!(),
        _ => {
            if is_nullable {
                type State = NullableArrayAggStateAny<AnyType>;
                AggregateArrayAggFunction::<AnyType, State>::create(display_name, return_type)
            } else {
                type State = ArrayAggStateAny<AnyType>;
                AggregateArrayAggFunction::<AnyType, State>::create(display_name, return_type)
            }
        }
    }
}

pub fn aggregate_array_agg_function_desc() -> AggregateFunctionDescription {
    AggregateFunctionDescription::creator(Box::new(try_create_aggregate_array_agg_function))
}
