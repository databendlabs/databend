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
use std::fmt::Display;
use std::fmt::Write;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggrStateType;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ScalarRef;
use databend_common_expression::StateSerdeItem;
use databend_common_expression::display::scalar_ref_to_string;
use databend_common_expression::types::AccessType;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::BuilderMut;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Number;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::ValueType;
use databend_common_expression::with_number_mapped_type;

use super::FunctionFactory;
use super::adaptors::*;

struct StringAggBuilder;

impl StringAggBuilder {
    fn register(registry: &mut AggregateFunctionRegistry) {
        Self::route().register(registry);
    }
}

inventory::submit! {
    FunctionFactory {
        register: StringAggBuilder::register,
    }
}

impl StringAggBuilder {
    fn string_agg_arguments() -> AggregateArgumentsPattern {
        AggregateArgumentsPattern::variadic(
            vec![AggregateArgumentPattern::any()],
            AggregateArgumentPattern::exact(DataType::String),
            0,
            Some(1),
        )
    }

    const STRING_AGG_FEATURES: FunctionFeatures = FunctionFeatures {
        is_decomposable: false,
        supports_filter: false,
        sort_policy: SortPolicy::Optional,
        distinct_policy: DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "concatenates input values into a string",
        definition: "string_agg(expr[, delimiter])",
        example: "select string_agg(name) from t",
    };
}

#[derive(Default)]
pub struct AggregateStringAggState {
    values: String,
}

impl AggregateStringAggState {
    pub fn state_description() -> AggregateStateDescription {
        AggregateStateDescription::new(vec![AggrStateType::Custom(Layout::new::<Self>())], vec![
            StateSerdeItem::DataType(StringType::data_type()),
        ])
        .with_manual_drop(true)
    }
}

impl<T> UnaryState<T, StringType> for AggregateStringAggState
where T: ToStringType
{
    type FunctionInfo = String;

    fn init(_function_info: &Self::FunctionInfo) -> Self {
        Self::default()
    }

    fn add(&mut self, value: T::ScalarRef<'_>, delimiter: &String) -> Result<()> {
        write!(self.values, "{}{delimiter}", T::format(&value)).unwrap();
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.values.push_str(&rhs.values);
        Ok(())
    }

    fn merge_owned(&mut self, rhs: &mut Self) -> Result<()> {
        self.values.push_str(&rhs.values);
        rhs.values.clear();
        Ok(())
    }

    fn merge_result(
        &mut self,
        mut builder: BuilderMut<'_, StringType>,
        delimiter: &String,
    ) -> Result<()> {
        if self.values.is_empty() {
            builder.put_and_commit("");
        } else {
            let len = self.values.len() - delimiter.len();
            builder.put_and_commit(&self.values[..len]);
        }
        Ok(())
    }

    fn serialize(
        &self,
        builder: &mut ColumnBuilder,
        _function_info: &Self::FunctionInfo,
    ) -> Result<()> {
        let mut builder = StringType::downcast_builder(builder);
        builder.put_and_commit(&self.values);
        Ok(())
    }

    fn merge_serialized(
        &mut self,
        value: ScalarRef<'_>,
        _function_info: &Self::FunctionInfo,
    ) -> Result<()> {
        let ScalarRef::String(value) = value else {
            unreachable!()
        };
        self.values.push_str(value);
        Ok(())
    }
}

trait ToStringType: AccessType {
    fn format(v: &Self::ScalarRef<'_>) -> impl Display;
}

impl ToStringType for BooleanType {
    fn format(v: &Self::ScalarRef<'_>) -> impl Display {
        v
    }
}

impl ToStringType for StringType {
    fn format(v: &Self::ScalarRef<'_>) -> impl Display {
        v
    }
}

impl<T: Number + Display> ToStringType for NumberType<T> {
    fn format(v: &Self::ScalarRef<'_>) -> impl Display {
        v
    }
}

impl ToStringType for AnyType {
    fn format(v: &Self::ScalarRef<'_>) -> impl Display {
        scalar_ref_to_string(v)
    }
}

impl StringAggBuilder {
    fn route() -> DirectNameRoute {
        let arguments = Self::string_agg_arguments();
        let features = Self::STRING_AGG_FEATURES;
        DirectNameRoute::new(
            &["string_agg", "listagg", "group_concat"],
            arguments.clone(),
            features.clone(),
            NullPolicy::Skip,
        )
        .then(MergeRoute::new(false, StringAggBuilder::create))
        .then(MergeRoute::new(true, StringAggBuilder::create))
        .then(PlainRoute::new(StringAggBuilder::create))
        .then(IfRoute::new(StringAggBuilder::create))
        .then(StateRoute::new(StringAggBuilder::create))
        .then(DistinctRoute::new(StringAggBuilder::create))
    }

    fn create(build: DirectBuildContext<'_, impl CombinatorImpl>) -> Result<AggregateFunctionRef> {
        let value_type = build.args_type()[0].remove_nullable();
        let delimiter = if build.params().len() == 1 {
            build.params()[0].as_string().unwrap().clone()
        } else {
            String::new()
        };

        match_template::match_template! {
            T = [
                Boolean => BooleanType,
                String => StringType,
            ],
            match value_type {
                DataType::T => Self::create_instance::<T>(build, delimiter),
                DataType::Number(num_type) => {
                    with_number_mapped_type!(|NUM| match num_type {
                        NumberDataType::NUM => Self::create_instance::<NumberType<NUM>>(
                            build,
                            delimiter,
                        )
                    })
                }
                DataType::Decimal(_)
                | DataType::Timestamp
                | DataType::Date
                | DataType::Variant
                | DataType::Interval => Self::create_instance::<AnyType>(
                    build,
                    delimiter,
                ),
                _ => Err(ErrorCode::BadDataValueType(format!(
                    "{} does not support type '{:?}'",
                    build.name(), value_type
                ))),
            }
        }
    }

    fn create_instance<T>(
        build: DirectBuildContext<'_, impl CombinatorImpl>,
        delimiter: String,
    ) -> Result<AggregateFunctionRef>
    where
        T: ToStringType + ValueType,
    {
        let state = AggregateStringAggState::state_description();
        let return_type = StringType::data_type();

        let inner =
            UnaryImpl::<AggregateStringAggState, T, StringType, false>::new(delimiter.into());
        let implementation = UnaryAggregateImplementation::new(UnaryOrNull::new(inner));
        build.create_ordered(
            return_type.wrap_nullable(),
            state.with_null_flag(),
            implementation,
        )
    }
}
