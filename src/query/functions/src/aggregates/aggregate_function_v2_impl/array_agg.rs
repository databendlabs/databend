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
use std::fmt::Debug;
use std::marker::PhantomData;
use std::mem;

use databend_common_column::binary::BinaryColumnBuilder;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggrStateType;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ColumnView;
use databend_common_expression::ScalarRef;
use databend_common_expression::StateSerdeItem;
use databend_common_expression::types::date::CoreDate;
use databend_common_expression::types::decimal::*;
use databend_common_expression::types::empty_array::CoreEmptyArray;
use databend_common_expression::types::empty_map::CoreEmptyMap;
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

use super::FunctionFactory;
use super::adaptors::*;

struct ArrayAggBuilder;

impl ArrayAggBuilder {
    fn register(registry: &mut AggregateFunctionRegistry) {
        Self::route().register(registry);
    }
}

inventory::submit! {
    FunctionFactory {
        register: ArrayAggBuilder::register,
    }
}

impl ArrayAggBuilder {
    fn array_agg_arguments() -> AggregateArgumentsPattern {
        AggregateArgumentsPattern::fixed(vec![AggregateArgumentPattern::any()])
    }

    const ARRAY_AGG_FEATURES: FunctionFeatures = FunctionFeatures {
        is_decomposable: false,
        sort_policy: SortPolicy::Optional,
        distinct_policy: DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "aggregates values into an array",
        definition: "array_agg(expr)",
        example: "select array_agg(number) from numbers(10)",
    };
}

#[derive(Clone, Debug)]
pub struct AggregateArrayAggStateAny<T>
where T: ValueType
{
    values: Vec<T::Scalar>,
}

impl<T> Default for AggregateArrayAggStateAny<T>
where T: ValueType
{
    fn default() -> Self {
        Self { values: Vec::new() }
    }
}

impl<T> AggregateArrayAggStateAny<T>
where T: ValueType
{
    fn add(&mut self, value: Option<T::ScalarRef<'_>>) {
        self.values.push(T::to_owned_scalar(value.unwrap()));
    }

    fn add_batch(&mut self, column: ColumnView<T>, _validity: Option<&Bitmap>) -> Result<()> {
        if column.is_empty() {
            return Ok(());
        }
        for value in column.iter() {
            self.values.push(T::to_owned_scalar(value));
        }
        Ok(())
    }

    fn merge_owned(&mut self, rhs: &mut Self) -> Result<()> {
        self.values.append(&mut rhs.values);
        Ok(())
    }

    fn merge_result(&mut self, builder: &mut ColumnBuilder) -> Result<()> {
        let data_type = builder.data_type();
        let inner_type = data_type.as_array().unwrap();

        let mut inner_builder = ColumnBuilder::with_capacity(inner_type, self.values.len());
        let values = mem::take(&mut self.values);
        for value in values {
            let value = T::upcast_scalar_with_type(value, inner_type);
            inner_builder.push(value.as_ref());
        }
        builder.push(ScalarRef::Array(inner_builder.build()));
        Ok(())
    }

    fn serialize(&self, builder: &mut ColumnBuilder) -> Result<()> {
        let mut builder = ArrayType::<T>::downcast_builder(builder);
        for value in &self.values {
            builder.put_item(T::to_scalar_ref(value));
        }
        builder.commit_row();
        Ok(())
    }

    fn merge_serialized(&mut self, value: ScalarRef<'_>) -> Result<()> {
        let values = ArrayType::<T>::try_downcast_scalar(&value)?;
        for value in T::iter_column(&values) {
            self.values.push(T::to_owned_scalar(value));
        }
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct AggregateArrayAggStateSimple<T>
where T: SimpleType
{
    values: Vec<T::Scalar>,
}

impl<T: Debug + SimpleType> Default for AggregateArrayAggStateSimple<T> {
    fn default() -> Self {
        Self { values: Vec::new() }
    }
}

impl<T> AggregateArrayAggStateSimple<T>
where T: SimpleType + Debug
{
    fn add(&mut self, value: Option<T::Scalar>) {
        if let Some(value) = value {
            self.values.push(value);
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
            for (value, valid) in column.iter().zip(validity.iter()) {
                if valid {
                    self.values.push(value);
                }
            }
        } else {
            self.values.extend(column.iter());
        }
        Ok(())
    }

    fn merge_owned(&mut self, rhs: &mut Self) -> Result<()> {
        self.values.append(&mut rhs.values);
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

    fn serialize(&self, builder: &mut ColumnBuilder) -> Result<()> {
        let mut builder = ArrayType::<SimpleValueType<T>>::downcast_builder(builder);
        for value in &self.values {
            builder.put_item(*value);
        }
        builder.commit_row();
        Ok(())
    }

    fn merge_serialized(&mut self, value: ScalarRef<'_>) -> Result<()> {
        let values = ArrayType::<SimpleValueType<T>>::try_downcast_scalar(&value)?;
        let mut rhs = Self {
            values: SimpleValueType::<T>::column_to_builder(values),
        };
        self.merge_owned(&mut rhs)
    }
}

#[derive(Clone, Debug)]
pub struct AggregateArrayAggStateZST<const IS_NULL: bool> {
    validity: MutableBitmap,
}

impl<const IS_NULL: bool> Default for AggregateArrayAggStateZST<IS_NULL> {
    fn default() -> Self {
        Self {
            validity: Default::default(),
        }
    }
}

impl<const IS_NULL: bool> AggregateArrayAggStateZST<IS_NULL> {
    fn add(&mut self, value: Option<()>) {
        if !IS_NULL && value.is_some() {
            self.validity.push(true);
        }
    }

    fn add_batch<V>(
        &mut self,
        column: ColumnView<ZeroSizeValueType<V>>,
        validity: Option<&Bitmap>,
    ) -> Result<()>
    where
        V: ZeroSizeType,
    {
        if IS_NULL || column.is_empty() {
            return Ok(());
        }

        if let Some(validity) = validity {
            for valid in validity.iter() {
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

    fn merge_owned(&mut self, rhs: &mut Self) -> Result<()> {
        self.validity
            .extend_from_slice(rhs.validity.as_slice(), 0, rhs.validity.len());
        rhs.validity.clear();
        Ok(())
    }

    fn merge_result<V>(&mut self, builder: &mut ColumnBuilder) -> Result<()>
    where V: ZeroSizeType {
        let item = V::upcast_column(self.validity.len());
        builder.push(ScalarRef::Array(item));
        Ok(())
    }

    fn serialize(&self, builder: &mut ColumnBuilder) -> Result<()> {
        let mut builder = ArrayType::<BooleanType>::downcast_builder(builder);
        builder.push_item(self.validity.clone().freeze());
        Ok(())
    }

    fn merge_serialized(&mut self, value: ScalarRef<'_>) -> Result<()> {
        let values = ArrayType::<BooleanType>::try_downcast_scalar(&value)?;
        self.validity.extend_from_bitmap(&values);
        Ok(())
    }
}

#[derive(Clone, Debug)]
pub struct AggregateArrayAggStateBinary<T>
where T: ArgType
{
    builder: BinaryColumnBuilder,
    _phantom: PhantomData<T>,
}

impl<T: Debug + ArgType> Default for AggregateArrayAggStateBinary<T> {
    fn default() -> Self {
        Self {
            builder: BinaryColumnBuilder::with_capacity(0, 0),
            _phantom: PhantomData,
        }
    }
}

impl<T> AggregateArrayAggStateBinary<T>
where T: ArgType + Debug + Send
{
    fn add(&mut self, value: Option<T::ScalarRef<'_>>) {
        if let Some(value) = value {
            let value = T::upcast_scalar(T::to_owned_scalar(value));
            self.builder.put_slice(value.as_bytes().unwrap());
            self.builder.commit_row();
        }
    }

    fn add_batch(&mut self, column: ColumnView<T>, validity: Option<&Bitmap>) -> Result<()> {
        if column.is_empty() {
            return Ok(());
        }

        if let Some(validity) = validity {
            for (value, valid) in column.iter().zip(validity.iter()) {
                if valid {
                    let value = T::upcast_scalar(T::to_owned_scalar(value));
                    self.builder.put_slice(value.as_bytes().unwrap());
                    self.builder.commit_row();
                }
            }
        } else {
            for value in column.iter() {
                let value = T::upcast_scalar(T::to_owned_scalar(value));
                self.builder.put_slice(value.as_bytes().unwrap());
                self.builder.commit_row();
            }
        }
        Ok(())
    }

    fn merge_owned(&mut self, rhs: &mut Self) -> Result<()> {
        let rhs_builder = mem::replace(&mut rhs.builder, BinaryColumnBuilder::with_capacity(0, 0));
        self.builder.append_column(&rhs_builder.build());
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

    fn serialize(&self, builder: &mut ColumnBuilder) -> Result<()> {
        let mut builder = ArrayType::<T>::downcast_builder(builder);
        let binary_column = self.builder.clone().build();
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
    }

    fn merge_serialized(&mut self, value: ScalarRef<'_>) -> Result<()> {
        let values = ArrayType::<T>::try_downcast_scalar(&value)?;
        let builder = T::column_to_builder(values);
        let column_builder = T::try_upcast_column_builder(builder, &T::data_type()).unwrap();
        let binary_builder = match column_builder {
            ColumnBuilder::String(string_builder) => {
                let mut builder = BinaryColumnBuilder::with_capacity(
                    string_builder.len(),
                    string_builder.memory_size(),
                );
                for index in 0..string_builder.len() {
                    let value = unsafe { string_builder.index_unchecked(index) };
                    builder.put_str(value);
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
        let mut rhs = Self {
            builder: binary_builder,
            _phantom: PhantomData,
        };
        self.merge_owned(&mut rhs)
    }
}

impl<T> AggregateUnaryState<T> for AggregateArrayAggStateAny<T>
where
    T: AccessType + ValueType,
    T::Scalar: Send + Sync,
{
    fn state_description(return_type: DataType) -> AggregateStateDescription {
        AggregateStateDescription::new(
            vec![AggrStateType::Custom(Layout::new::<Self>())],
            vec![StateSerdeItem::DataType(return_type)],
        )
        .with_manual_drop(true)
    }

    fn add(&mut self, value: Option<T::ScalarRef<'_>>) {
        self.add(value);
    }

    fn add_batch(&mut self, column: ColumnView<T>, validity: Option<&Bitmap>) -> Result<()> {
        self.add_batch(column, validity)
    }

    fn serialize(&self, builder: &mut ColumnBuilder) -> Result<()> {
        self.serialize(builder)
    }

    fn merge_serialized(&mut self, value: ScalarRef<'_>) -> Result<()> {
        self.merge_serialized(value)
    }

    fn merge_owned(&mut self, rhs: &mut Self) -> Result<()> {
        self.merge_owned(rhs)
    }

    fn merge_result(&mut self, builder: &mut ColumnBuilder) -> Result<()> {
        self.merge_result(builder)
    }
}

impl<T> AggregateUnaryState<SimpleValueType<T>> for AggregateArrayAggStateSimple<T>
where
    T: SimpleType + Debug,
    T::Scalar: Send + Sync,
{
    fn state_description(return_type: DataType) -> AggregateStateDescription {
        let data_type = return_type.as_array().unwrap().remove_nullable();
        AggregateStateDescription::new(
            vec![AggrStateType::Custom(Layout::new::<Self>())],
            vec![StateSerdeItem::DataType(DataType::Array(Box::new(
                data_type,
            )))],
        )
        .with_manual_drop(true)
    }

    fn add(&mut self, value: Option<T::Scalar>) {
        self.add(value);
    }

    fn add_batch(
        &mut self,
        column: ColumnView<SimpleValueType<T>>,
        validity: Option<&Bitmap>,
    ) -> Result<()> {
        self.add_batch(column, validity)
    }

    fn serialize(&self, builder: &mut ColumnBuilder) -> Result<()> {
        self.serialize(builder)
    }

    fn merge_serialized(&mut self, value: ScalarRef<'_>) -> Result<()> {
        self.merge_serialized(value)
    }

    fn merge_owned(&mut self, rhs: &mut Self) -> Result<()> {
        self.merge_owned(rhs)
    }

    fn merge_result(&mut self, builder: &mut ColumnBuilder) -> Result<()> {
        self.merge_result(builder)
    }
}

impl<V, const IS_NULL: bool> AggregateUnaryState<ZeroSizeValueType<V>>
    for AggregateArrayAggStateZST<IS_NULL>
where V: ZeroSizeType
{
    fn state_description(_return_type: DataType) -> AggregateStateDescription {
        AggregateStateDescription::new(
            vec![AggrStateType::Custom(Layout::new::<Self>())],
            vec![StateSerdeItem::DataType(
                ArrayType::<BooleanType>::data_type(),
            )],
        )
        .with_manual_drop(true)
    }

    fn add(&mut self, value: Option<()>) {
        self.add(value);
    }

    fn add_batch(
        &mut self,
        column: ColumnView<ZeroSizeValueType<V>>,
        validity: Option<&Bitmap>,
    ) -> Result<()> {
        self.add_batch::<V>(column, validity)
    }

    fn serialize(&self, builder: &mut ColumnBuilder) -> Result<()> {
        self.serialize(builder)
    }

    fn merge_serialized(&mut self, value: ScalarRef<'_>) -> Result<()> {
        self.merge_serialized(value)
    }

    fn merge_owned(&mut self, rhs: &mut Self) -> Result<()> {
        self.merge_owned(rhs)
    }

    fn merge_result(&mut self, builder: &mut ColumnBuilder) -> Result<()> {
        self.merge_result::<V>(builder)
    }
}

impl<T> AggregateUnaryState<T> for AggregateArrayAggStateBinary<T>
where T: ArgType + AccessType + Debug + Send + Sync
{
    fn state_description(return_type: DataType) -> AggregateStateDescription {
        let data_type = return_type.as_array().unwrap().remove_nullable();
        AggregateStateDescription::new(
            vec![AggrStateType::Custom(Layout::new::<Self>())],
            vec![StateSerdeItem::DataType(DataType::Array(Box::new(
                data_type,
            )))],
        )
        .with_manual_drop(true)
    }

    fn add(&mut self, value: Option<T::ScalarRef<'_>>) {
        self.add(value);
    }

    fn add_batch(&mut self, column: ColumnView<T>, validity: Option<&Bitmap>) -> Result<()> {
        self.add_batch(column, validity)
    }

    fn serialize(&self, builder: &mut ColumnBuilder) -> Result<()> {
        self.serialize(builder)
    }

    fn merge_serialized(&mut self, value: ScalarRef<'_>) -> Result<()> {
        self.merge_serialized(value)
    }

    fn merge_owned(&mut self, rhs: &mut Self) -> Result<()> {
        self.merge_owned(rhs)
    }

    fn merge_result(&mut self, builder: &mut ColumnBuilder) -> Result<()> {
        self.merge_result(builder)
    }
}

impl ArrayAggBuilder {
    fn route() -> DirectNameRoute {
        let arguments = Self::array_agg_arguments();
        let features = Self::ARRAY_AGG_FEATURES;
        DirectNameRoute::new(
            &["array_agg", "list"],
            arguments.clone(),
            features.clone(),
            NullPolicy::Keep,
        )
        .with_validator(Self::validate_request)
        .then(MergeRoute::new(false, ArrayAggBuilder::create))
        .then(MergeRoute::new(true, ArrayAggBuilder::create))
        .then(PlainRoute::new(ArrayAggBuilder::create))
        .then(IfRoute::new(ArrayAggBuilder::create))
        .then(StateRoute::new(ArrayAggBuilder::create))
    }

    fn validate_request(request: &AggregateFunctionRequest<'_>) -> Result<()> {
        if request.params.is_empty() {
            Ok(())
        } else {
            Err(ErrorCode::BadArguments(format!(
                "{} expects no parameters",
                request.name
            )))
        }
    }

    fn create(
        build: DirectBuildContext<'_, impl CombinatorImpl>,
    ) -> Result<AggregateFunctionRef> {
        let data_type = build.args_type()[0].clone();
        let not_null_type = data_type.remove_nullable();
        let return_type = DataType::Array(Box::new(not_null_type.clone()));

        fn simple<V>(
            build: DirectBuildContext<'_, impl CombinatorImpl>,
            return_type: DataType,
        ) -> Result<AggregateFunctionRef>
        where
            V: SimpleType + Debug,
            V::Scalar: Send + Sync,
        {
            ArrayAggBuilder::create_instance::<SimpleValueType<V>, AggregateArrayAggStateSimple<V>>(
                build,
                return_type,
            )
        }

        match not_null_type {
            DataType::Number(num_type) => {
                with_number_mapped_type!(|NUM| match num_type {
                    NumberDataType::NUM => simple::<CoreNumber<NUM>>(build, return_type),
                })
            }
            DataType::Decimal(size) => {
                with_decimal_mapped_type!(|DECIMAL| match size.data_kind() {
                    DecimalDataKind::DECIMAL => simple::<CoreDecimal<DECIMAL>>(build, return_type),
                })
            }
            DataType::Date => simple::<CoreDate>(build, return_type),
            DataType::Timestamp => simple::<CoreTimestamp>(build, return_type),
            DataType::Interval => simple::<CoreInterval>(build, return_type),
            DataType::Null => Self::create_instance::<
                ZeroSizeValueType<CoreNull>,
                AggregateArrayAggStateZST<true>,
            >(build, return_type),
            DataType::EmptyArray => Self::create_instance::<
                ZeroSizeValueType<CoreEmptyArray>,
                AggregateArrayAggStateZST<false>,
            >(build, return_type),
            DataType::EmptyMap => Self::create_instance::<
                ZeroSizeValueType<CoreEmptyMap>,
                AggregateArrayAggStateZST<false>,
            >(build, return_type),
            DataType::Boolean => Self::create_instance::<
                BooleanType,
                AggregateArrayAggStateAny<BooleanType>,
            >(build, return_type),
            DataType::String => Self::create_instance::<
                StringType,
                AggregateArrayAggStateBinary<StringType>,
            >(build, return_type),
            DataType::Binary => Self::create_instance::<
                BinaryType,
                AggregateArrayAggStateBinary<BinaryType>,
            >(build, return_type),
            DataType::Bitmap => Self::create_instance::<
                BitmapType,
                AggregateArrayAggStateBinary<BitmapType>,
            >(build, return_type),
            DataType::Variant => Self::create_instance::<
                VariantType,
                AggregateArrayAggStateBinary<VariantType>,
            >(build, return_type),
            DataType::Geometry => Self::create_instance::<
                GeometryType,
                AggregateArrayAggStateBinary<GeometryType>,
            >(build, return_type),
            DataType::Geography => Self::create_instance::<
                GeographyType,
                AggregateArrayAggStateBinary<GeographyType>,
            >(build, return_type),
            DataType::Nullable(_) | DataType::Generic(_) => unreachable!(),
            _ => Self::create_instance::<AnyType, AggregateArrayAggStateAny<AnyType>>(
                build,
                return_type,
            ),
        }
    }

    fn create_instance<T, State>(
        build: DirectBuildContext<'_, impl CombinatorImpl>,
        return_type: DataType,
    ) -> Result<AggregateFunctionRef>
    where
        T: AccessType,
        State: Default + AggregateUnaryState<T>,
    {
        build.create_ordered(
            return_type.clone(),
            State::state_description(return_type),
            AggregateUnaryStateImplementation::<T, State>::default(),
        )
    }
}
