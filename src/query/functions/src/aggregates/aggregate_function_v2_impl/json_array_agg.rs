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
use std::mem;

use borsh::BorshDeserialize;
use borsh::BorshSerialize;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggrStateType;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ColumnView;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::StateSerdeItem;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::DataType;
use databend_common_expression::types::ValueType;
use databend_common_expression::types::variant::cast_scalar_to_variant;
use jiff::tz::TimeZone;
use jsonb::OwnedJsonb;
use jsonb::RawJsonb;

use super::AggregateFunctionDefinition;
use super::AggregateFunctionV2Factory;
use super::adaptors_v2 as v2;
use super::adaptors_v2::AggregateUnaryState;
use super::adaptors_v2::AggregateUnaryStateImplementation;

struct JsonArrayAggBuilder;

impl JsonArrayAggBuilder {
    fn register(registry: &mut v2::AggregateFunctionRegistry) {
        let json_array_agg = AggregateFunctionDefinition::new(
            "json_array_agg",
            JsonArrayAggBuilder::json_array_agg_arguments(),
            JsonArrayAggBuilder::JSON_ARRAY_AGG_FEATURES,
            JsonArrayAggBuilder::try_create,
        );
        json_array_agg.register_with_combinators(registry, false);
        let json_agg = AggregateFunctionDefinition::new(
            "json_agg",
            JsonArrayAggBuilder::json_array_agg_arguments(),
            JsonArrayAggBuilder::JSON_AGG_FEATURES,
            JsonArrayAggBuilder::try_create,
        );
        json_agg.register_with_combinators(registry, false);
    }
}

inventory::submit! {
    AggregateFunctionV2Factory {
        register: JsonArrayAggBuilder::register,
    }
}

impl JsonArrayAggBuilder {
    fn json_array_agg_arguments() -> v2::AggregateArgumentsPattern {
        v2::AggregateArgumentsPattern::fixed(vec![v2::AggregateArgumentPattern::any()])
    }

    const JSON_ARRAY_AGG_FEATURES: v2::FunctionFeatures = v2::FunctionFeatures {
        is_decomposable: false,
        sort_policy: v2::SortPolicy::Unsupported,
        distinct_policy: v2::DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "aggregates values into a JSON array",
        definition: "json_array_agg(expr)",
        example: "select json_array_agg(number) from numbers(10)",
    };

    const JSON_AGG_FEATURES: v2::FunctionFeatures = v2::FunctionFeatures {
        is_decomposable: false,
        sort_policy: v2::SortPolicy::Unsupported,
        distinct_policy: v2::DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "aggregates values into a JSON array",
        definition: "json_agg(expr)",
        example: "select json_agg(number) from numbers(10)",
    };
}

#[derive(Clone, BorshSerialize, BorshDeserialize, Debug)]
pub struct JsonArrayAggState<T>
where
    T: ValueType,
    T::Scalar: BorshSerialize + BorshDeserialize,
{
    values: Vec<T::Scalar>,
}

impl<T> Default for JsonArrayAggState<T>
where
    T: ValueType,
    T::Scalar: BorshSerialize + BorshDeserialize,
{
    fn default() -> Self {
        Self { values: Vec::new() }
    }
}

impl<T> JsonArrayAggState<T>
where
    T: ValueType,
    T::Scalar: BorshSerialize + BorshDeserialize,
{
    pub fn state_description() -> v2::AggregateStateDescription {
        v2::AggregateStateDescription::new(
            vec![AggrStateType::Custom(Layout::new::<Self>())],
            vec![StateSerdeItem::Binary(None)],
        )
        .with_manual_drop(true)
    }

    fn add(&mut self, value: Option<T::ScalarRef<'_>>) {
        if let Some(value) = value {
            self.values.push(T::to_owned_scalar(value));
        }
    }

    fn add_batch(&mut self, column: ColumnView<T>, validity: Option<&Bitmap>) -> Result<()> {
        if column.is_empty() {
            return Ok(());
        }
        if let Some(validity) = validity {
            for (value, valid) in column.iter().zip(validity.iter()) {
                if valid {
                    self.values.push(T::to_owned_scalar(value));
                }
            }
        } else {
            for value in column.iter() {
                self.values.push(T::to_owned_scalar(value));
            }
        }
        Ok(())
    }

    fn merge_owned(&mut self, rhs: &mut Self) -> Result<()> {
        self.values.append(&mut rhs.values);
        Ok(())
    }

    fn merge_result(&mut self, builder: &mut ColumnBuilder) -> Result<()> {
        let timezone = TimeZone::UTC;
        let mut items = Vec::with_capacity(self.values.len());
        let values = mem::take(&mut self.values);
        let data_type = builder.data_type();
        for value in values {
            let value = T::upcast_scalar_with_type(value, &data_type);
            if value == Scalar::Null {
                continue;
            }
            let mut jsonb = Vec::new();
            cast_scalar_to_variant(value.as_ref(), &timezone, &mut jsonb, None);
            items.push(jsonb);
        }

        let owned_jsonb = OwnedJsonb::build_array(items.iter().map(|value| RawJsonb::new(value)))
            .map_err(|error| {
            ErrorCode::Internal(format!("failed to build array error: {:?}", error))
        })?;
        builder.push(ScalarRef::Variant(owned_jsonb.as_ref()));
        Ok(())
    }

    fn serialize(&self, builder: &mut ColumnBuilder) -> Result<()> {
        let binary_builder = builder.as_binary_mut().unwrap();
        BorshSerialize::serialize(self, &mut binary_builder.data)?;
        binary_builder.commit_row();
        Ok(())
    }

    fn merge_serialized(&mut self, value: ScalarRef<'_>) -> Result<()> {
        let ScalarRef::Binary(mut data) = value else {
            unreachable!()
        };
        let mut rhs = Self::deserialize_reader(&mut data)?;
        self.merge_owned(&mut rhs)
    }
}

impl<T> AggregateUnaryState<T> for JsonArrayAggState<T>
where
    T: ValueType,
    T::Scalar: BorshSerialize + BorshDeserialize + Clone + Send + Sync,
{
    fn state_description(_return_type: DataType) -> v2::AggregateStateDescription {
        Self::state_description()
    }

    fn add(&mut self, value: Option<T::ScalarRef<'_>>) {
        self.add(value)
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

impl JsonArrayAggBuilder {
    fn try_create(request: v2::AggregateFunctionRequest<'_>) -> Result<v2::AggregateFunctionRef> {
        if !request.params.is_empty() {
            return Err(ErrorCode::BadArguments(format!(
                "{} expects no parameters",
                request.name
            )));
        }

        v2::build_default_name_route_keep_nulls(request, &[v2::KeepNullNameRoute {
            names: &["json_array_agg", "json_agg"],
            features: Self::JSON_ARRAY_AGG_FEATURES,
            build: direct_aggregate_function_build_input_fns!(Self::create),
        }])
    }

    fn create(
        build: v2::DirectBuildContext<'_, impl v2::CombinatorImpl>,
    ) -> Result<v2::AggregateFunctionRef> {
        build.create(
            DataType::Variant,
            <JsonArrayAggState<AnyType> as AggregateUnaryState<AnyType>>::state_description(
                DataType::Variant,
            ),
            AggregateUnaryStateImplementation::<AnyType, JsonArrayAggState<AnyType>>::default(),
        )
    }
}
