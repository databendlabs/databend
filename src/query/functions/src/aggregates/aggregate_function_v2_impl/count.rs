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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::AggrState;
use databend_common_expression::AggrStateType;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::ScalarRef;
use databend_common_expression::StateSerdeItem;
use databend_common_expression::types::AnyType;
use databend_common_expression::types::ArgType;
use databend_common_expression::types::Bitmap;
use databend_common_expression::types::BuilderExt;
use databend_common_expression::types::DataType;
use databend_common_expression::types::DateType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::types::UInt64Type;
use databend_common_expression::types::ValueType;
use databend_common_expression::utils::column_merge_validity;
use databend_common_expression::with_number_mapped_type;

use super::AggregateFunctionDefinition;
use super::AggregateFunctionV2Factory;
use super::adaptors_v2 as v2;
use super::adaptors_v2::UnaryState;

#[derive(Default)]
pub struct AggregateCountState {
    count: u64,
}

pub struct AggregateCountImplementation {
    has_argument: bool,
}

struct CountBuilder;

impl CountBuilder {
    fn register(registry: &mut v2::AggregateFunctionRegistry) {
        let count = Self::definition();
        count.register(registry);
        AggregateFunctionDefinition::new(
            "count_distinct",
            CountBuilder::count_distinct_arguments(),
            CountBuilder::COUNT_DISTINCT_FEATURES,
            CountBuilder::try_create_distinct,
        )
        .register(registry);
        AggregateFunctionDefinition::new(
            "count_if",
            v2::AggregateArgumentsPattern::if_condition(Self::count_arguments()),
            CountBuilder::COUNT_IF_FEATURES,
            CountBuilder::try_create,
        )
        .register(registry);
        AggregateFunctionDefinition::new(
            "count_state",
            v2::AggregateArgumentsPattern::variadic(
                vec![],
                v2::AggregateArgumentPattern::any(),
                0,
                Some(32),
            ),
            CountBuilder::COUNT_STATE_FEATURES,
            CountBuilder::try_create,
        )
        .register(registry);
    }
}

inventory::submit! {
    AggregateFunctionV2Factory {
        register: CountBuilder::register,
    }
}

impl CountBuilder {
    fn definition() -> AggregateFunctionDefinition {
        AggregateFunctionDefinition::new(
            "count",
            Self::count_arguments(),
            Self::COUNT_FEATURES,
            Self::try_create,
        )
    }

    fn count_arguments() -> v2::AggregateArgumentsPattern {
        v2::AggregateArgumentsPattern::one_of(vec![
            v2::AggregateArgumentsPattern::fixed(vec![]),
            v2::AggregateArgumentsPattern::fixed(vec![v2::AggregateArgumentPattern::any()]),
        ])
    }

    fn count_distinct_arguments() -> v2::AggregateArgumentsPattern {
        v2::AggregateArgumentsPattern::variadic(
            vec![],
            v2::AggregateArgumentPattern::any(),
            1,
            Some(32),
        )
    }

    const COUNT_FEATURES: v2::FunctionFeatures = v2::FunctionFeatures {
        is_decomposable: true,
        sort_policy: v2::SortPolicy::Unsupported,
        distinct_policy: v2::DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "counts input rows or non-null argument values",
        definition: "count([expr])",
        example: "select count(*) from numbers(10)",
    };

    const COUNT_DISTINCT_FEATURES: v2::FunctionFeatures = v2::FunctionFeatures {
        is_decomposable: true,
        sort_policy: v2::SortPolicy::Unsupported,
        distinct_policy: v2::DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "counts distinct non-null input rows",
        definition: "count_distinct(expr[, ...])",
        example: "select count_distinct(number) from numbers(10)",
    };

    const COUNT_IF_FEATURES: v2::FunctionFeatures = v2::FunctionFeatures {
        is_decomposable: true,
        sort_policy: v2::SortPolicy::Unsupported,
        distinct_policy: v2::DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "counts rows matching a boolean condition",
        definition: "count_if(cond)",
        example: "select count_if(number > 0) from numbers(10)",
    };

    const COUNT_STATE_FEATURES: v2::FunctionFeatures = v2::FunctionFeatures {
        is_decomposable: true,
        sort_policy: v2::SortPolicy::Unsupported,
        distinct_policy: v2::DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "returns the serialized aggregate state",
        definition: "aggregate_state(args...)",
        example: "select count_state(number) from numbers(10)",
    };
}

impl CountBuilder {
    fn try_create_distinct(
        request: v2::AggregateFunctionRequest<'_>,
    ) -> Result<v2::AggregateFunctionRef> {
        let route = v2::AggregateFunctionNameRoutePath::root(request);
        if let Some(route) = route.names(&["count_distinct"]) {
            return route.build_with_direct_input(Self::COUNT_DISTINCT_FEATURES, |build| {
                create_distinct_count_function(build, true)
            });
        }
        route.unknown()
    }

    fn try_create(request: v2::AggregateFunctionRequest<'_>) -> Result<v2::AggregateFunctionRef> {
        let route = v2::AggregateFunctionNameRoutePath::root(request);

        if let Some(route) = route.names(&["count"]) {
            if let Some(function) = route.plain_null_argument_result(true)? {
                return Ok(function);
            }
            return route
                .plain_or_null()
                .build_with_direct_input(Self::COUNT_FEATURES, Self::create);
        }

        if let Some(route) = route.names(&["count_if"]) {
            if let Some(function) = route.null_argument_result(true)? {
                return Ok(function);
            }
            return route
                .if_combinator(v2::NullPolicy::ReturnsDefaultWhenOnlyNull, true)?
                .build_with_direct_input(Self::COUNT_IF_FEATURES, Self::create);
        }

        if let Some(route) = route.names(&["count_state"]) {
            if let Some(function) = route.state_null_argument_result() {
                return Ok(function);
            }
            let state_plan = route.state_nullable_input_plan(true);
            return route
                .state_combinator(state_plan)
                .build_with_direct_input(Self::COUNT_STATE_FEATURES, Self::create);
        }

        route.unknown()
    }

    fn create(
        build: v2::DirectBuildContext<'_, impl v2::CombinatorImpl>,
    ) -> Result<v2::AggregateFunctionRef> {
        let has_argument = !build.args_type().is_empty();

        build.create(
            UInt64Type::data_type(),
            AggregateCountImplementation::state_description(),
            AggregateCountImplementation::new(has_argument),
        )
    }

    fn distinct_state_description() -> v2::AggregateStateDescription {
        v2::AggregateStateDescription::new(
            vec![
                AggrStateType::Custom(Layout::new::<v2::AggregateDistinctState>()),
                AggrStateType::Custom(Layout::new::<AggregateCountState>()),
            ],
            vec![
                StateSerdeItem::DataType(DataType::Array(Box::new(DataType::Binary))),
                StateSerdeItem::DataType(UInt64Type::data_type()),
            ],
        )
        .with_manual_drop(true)
    }
}

pub(super) fn create_distinct_count_function(
    build: v2::DirectBuildContext<'_, impl v2::CombinatorImpl>,
    count_argument: bool,
) -> Result<v2::AggregateFunctionRef> {
    if !build.params().is_empty() {
        return Err(ErrorCode::BadArguments(format!(
            "{} expects no parameters",
            build.name()
        )));
    }

    if build.args_type().len() == 1 {
        return create_unary_distinct_count_function(build);
    }

    let state = CountBuilder::distinct_state_description();
    let args_type = build.args_type().to_vec();
    let implementation =
        v2::AggregateMultiArgSkipNullImplementation::new(v2::AggregateDistinctImplementation::<
            false,
        >::new(
            AggregateCountImplementation::new(count_argument),
            args_type,
        ));

    build.create(UInt64Type::data_type(), state, implementation)
}

fn create_unary_distinct_count_function(
    build: v2::DirectBuildContext<'_, impl v2::CombinatorImpl>,
) -> Result<v2::AggregateFunctionRef> {
    let data_type = build.args_type()[0].remove_nullable();
    with_number_mapped_type!(|NUM_TYPE| match data_type {
        DataType::Number(NumberDataType::NUM_TYPE) => {
            create_unary_distinct_count_function_typed::<NumberType<NUM_TYPE>>(build)
        }
        DataType::String => create_unary_distinct_count_function_typed::<StringType>(build),
        DataType::Date => create_unary_distinct_count_function_typed::<DateType>(build),
        DataType::Timestamp => create_unary_distinct_count_function_typed::<TimestampType>(build),
        _ => create_unary_distinct_count_function_typed::<AnyType>(build),
    })
}

fn create_unary_distinct_count_function_typed<T>(
    build: v2::DirectBuildContext<'_, impl v2::CombinatorImpl>,
) -> Result<v2::AggregateFunctionRef>
where T: ValueType {
    let state = AggregateCountImplementation::state_description();
    let distinct_arg_type = build.args_type()[0].remove_nullable();
    let implementation =
        v2::UnaryAggregateImplementation::new(v2::UnarySkipNull::new(v2::UnaryDistinct::new(
            v2::UnaryImpl::<AggregateCountState, T, UInt64Type, false>::new(().into()),
            distinct_arg_type,
        )));
    build.create(
        UInt64Type::data_type(),
        v2::unary_distinct_state_description(&state),
        implementation,
    )
}

impl AggregateCountImplementation {
    pub fn new(has_argument: bool) -> Self {
        Self { has_argument }
    }

    pub fn state_description() -> v2::AggregateStateDescription {
        v2::AggregateStateDescription::new(
            vec![AggrStateType::Custom(Layout::new::<AggregateCountState>())],
            vec![StateSerdeItem::DataType(UInt64Type::data_type())],
        )
    }

    fn argument_validity(columns: ProjectedBlock<'_>, validity: Option<&Bitmap>) -> Option<Bitmap> {
        columns.iter().fold(validity.cloned(), |acc, entry| {
            column_merge_validity(&entry.clone(), acc)
        })
    }

    fn count_valid_rows(rows: usize, validity: Option<&Bitmap>) -> u64 {
        (rows - validity.map(Bitmap::null_count).unwrap_or(0)) as u64
    }

    fn row_is_valid(columns: ProjectedBlock<'_>, row: usize) -> bool {
        Self::argument_validity(columns, None).is_none_or(|validity| validity.get(row).unwrap())
    }
}

impl v2::AggrImpl for AggregateCountImplementation {
    fn init_state(&self, state: AggrState<'_>) {
        state.write(AggregateCountState::default);
    }

    fn accumulate(&self, input: v2::AccumulateInput<'_>) -> Result<()> {
        if !self.has_argument {
            return Err(ErrorCode::BadArguments("count(*) expects rows-only input"));
        }

        let validity = Self::argument_validity(input.columns, input.validity);
        input.state.get::<AggregateCountState>().count +=
            Self::count_valid_rows(input.columns.num_rows(), validity.as_ref());
        Ok(())
    }

    fn accumulate_keys(&self, input: v2::AccumulateKeysInput<'_>) -> Result<()> {
        if !self.has_argument {
            return self.accumulate_row_count_keys(v2::AccumulateRowCountKeysInput {
                states: input.states,
            });
        }

        let validity = Self::argument_validity(input.columns, None);
        for (row, state) in input.states.iter().enumerate() {
            if validity
                .as_ref()
                .is_none_or(|validity| validity.get(row).unwrap())
            {
                state.get::<AggregateCountState>().count += 1;
            }
        }
        Ok(())
    }

    fn accumulate_row(&self, input: v2::AccumulateRowInput<'_>) -> Result<()> {
        if !self.has_argument || Self::row_is_valid(input.columns, input.row) {
            input.state.get::<AggregateCountState>().count += 1;
        }
        Ok(())
    }

    fn accumulate_row_count(&self, input: v2::AccumulateRowCountInput<'_>) -> Result<()> {
        if self.has_argument {
            return Err(ErrorCode::BadArguments("count(expr) expects column input"));
        }
        input.state.get::<AggregateCountState>().count += input.rows as u64;
        Ok(())
    }

    fn accumulate_row_count_keys(&self, input: v2::AccumulateRowCountKeysInput<'_>) -> Result<()> {
        if self.has_argument {
            return Err(ErrorCode::BadArguments("count(expr) expects column input"));
        }
        for state in input.states.iter() {
            state.get::<AggregateCountState>().count += 1;
        }
        Ok(())
    }

    fn serialize(&self, input: v2::SerializeInput<'_>) -> Result<()> {
        for state in input.states.iter() {
            let state = state.get::<AggregateCountState>();
            input.builders[0].push(ScalarRef::Number(NumberScalar::UInt64(state.count)));
        }
        Ok(())
    }

    fn merge_serialized(&self, input: v2::MergeSerializedInput<'_>) -> Result<()> {
        for (row, state) in input.states.iter().enumerate() {
            if input.filter.is_some_and(|filter| !filter.get(row).unwrap()) {
                continue;
            }
            let ScalarRef::Number(NumberScalar::UInt64(count)) =
                super::serialized_scalar_at(input.state, row, 0)
            else {
                unreachable!()
            };
            state.get::<AggregateCountState>().count += count;
        }
        Ok(())
    }

    fn merge_states(&self, input: v2::MergeStatesInput<'_>) -> Result<()> {
        input.state.get::<AggregateCountState>().count +=
            input.rhs.get::<AggregateCountState>().count;
        Ok(())
    }

    fn merge_result(&self, input: v2::MergeResultInput<'_>) -> Result<()> {
        input.builder.push(ScalarRef::Number(NumberScalar::UInt64(
            input.state.get::<AggregateCountState>().count,
        )));
        Ok(())
    }

    unsafe fn drop_state(&self, state: AggrState<'_>) {
        unsafe { std::ptr::drop_in_place(state.get::<AggregateCountState>()) };
    }
}

impl<T> UnaryState<T, UInt64Type> for AggregateCountState
where T: ValueType
{
    type FunctionInfo = ();

    fn init(_function_info: &Self::FunctionInfo) -> Self {
        Self::default()
    }

    fn add(&mut self, _value: T::ScalarRef<'_>, _function_info: &Self::FunctionInfo) -> Result<()> {
        self.count += 1;
        Ok(())
    }

    fn merge(&mut self, rhs: &Self) -> Result<()> {
        self.count += rhs.count;
        Ok(())
    }

    fn merge_result(
        &mut self,
        mut builder: <UInt64Type as ValueType>::ColumnBuilderMut<'_>,
        _function_info: &Self::FunctionInfo,
    ) -> Result<()> {
        builder.push_item(self.count);
        Ok(())
    }

    fn serialize(
        &self,
        builder: &mut ColumnBuilder,
        _function_info: &Self::FunctionInfo,
    ) -> Result<()> {
        builder.push(ScalarRef::Number(NumberScalar::UInt64(self.count)));
        Ok(())
    }

    fn merge_serialized(
        &mut self,
        value: ScalarRef<'_>,
        _function_info: &Self::FunctionInfo,
    ) -> Result<()> {
        let ScalarRef::Number(NumberScalar::UInt64(count)) = value else {
            unreachable!()
        };
        self.count += count;
        Ok(())
    }
}
