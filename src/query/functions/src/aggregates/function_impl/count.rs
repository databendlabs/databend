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
use databend_common_expression::Scalar;
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

use super::AggregateRegistration;
use super::adaptors::*;

#[derive(Default)]
pub struct AggregateCountState {
    count: u64,
}

pub struct CountEval {
    has_argument: bool,
}

struct CountBuilder;

impl CountBuilder {
    fn legacy_signatures(_params: &[Scalar], state_type: &DataType) -> Vec<Vec<DataType>> {
        match state_type {
            DataType::Tuple(fields)
                if matches!(
                    fields.first(),
                    Some(DataType::Number(NumberDataType::UInt64))
                ) =>
            {
                vec![vec![UInt64Type::data_type()]]
            }
            _ => Vec::new(),
        }
    }

    fn register(registry: &mut AggregateRegistry) {
        let state_arguments =
            ArgumentsPattern::variadic(vec![], ArgumentPattern::any(), 0, Some(32));
        NameRoute::new(
            &["count"],
            Self::count_arguments(),
            Self::COUNT_FEATURES,
            NullPolicy::ReturnsDefaultWhenOnlyNull,
        )
        .with_distinct_target("count_distinct")
        .then(
            MergeRoute::new(false, Self::create)
                .with_legacy_signature_resolver(Self::legacy_signatures),
        )
        .then(
            MergeRoute::new(true, Self::create)
                .with_legacy_signature_resolver(Self::legacy_signatures),
        )
        .then(PlainRoute::new(Self::create))
        .then(IfRoute::direct(Self::create).with_features(Self::COUNT_IF_FEATURES))
        .then(
            StateRoute::direct(Self::create)
                .with_arguments(state_arguments)
                .with_features(Self::COUNT_STATE_FEATURES),
        )
        .register(registry);
        NameRoute::new(
            &["count_distinct"],
            CountBuilder::count_distinct_arguments(),
            CountBuilder::COUNT_DISTINCT_FEATURES,
            NullPolicy::Keep,
        )
        .then(PlainRoute::new(Self::create_distinct))
        .register(registry);
    }
}

inventory::submit! {
    AggregateRegistration {
        register: CountBuilder::register,
    }
}

impl CountBuilder {
    fn count_arguments() -> ArgumentsPattern {
        ArgumentsPattern::one_of(vec![
            ArgumentsPattern::fixed(vec![]),
            ArgumentsPattern::fixed(vec![ArgumentPattern::any()]),
        ])
    }

    fn count_distinct_arguments() -> ArgumentsPattern {
        ArgumentsPattern::variadic(vec![], ArgumentPattern::any(), 1, Some(32))
    }

    const COUNT_FEATURES: AggregateFeatures = AggregateFeatures {
        is_decomposable: true,
        supports_filter: false,
        sort_policy: SortPolicy::Unsupported,
        distinct_policy: DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "counts input rows or non-null argument values",
        definition: "count([expr])",
        example: "select count(*) from numbers(10)",
    };

    const COUNT_DISTINCT_FEATURES: AggregateFeatures = AggregateFeatures {
        is_decomposable: true,
        supports_filter: false,
        sort_policy: SortPolicy::Unsupported,
        distinct_policy: DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "counts distinct non-null input rows",
        definition: "count_distinct(expr[, ...])",
        example: "select count_distinct(number) from numbers(10)",
    };

    const COUNT_IF_FEATURES: AggregateFeatures = AggregateFeatures {
        is_decomposable: true,
        supports_filter: false,
        sort_policy: SortPolicy::Unsupported,
        distinct_policy: DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "counts rows matching a boolean condition",
        definition: "count_if(cond)",
        example: "select count_if(number > 0) from numbers(10)",
    };

    const COUNT_STATE_FEATURES: AggregateFeatures = AggregateFeatures {
        is_decomposable: true,
        supports_filter: false,
        sort_policy: SortPolicy::Unsupported,
        distinct_policy: DistinctPolicy::Unsupported,
        category: "Aggregate",
        description: "returns the serialized aggregate state",
        definition: "aggregate_state(args...)",
        example: "select count_state(number) from numbers(10)",
    };
}

impl CountBuilder {
    fn create_distinct(build: DirectBuildContext<'_, impl Combinator>) -> Result<AggregateCallRef> {
        create_distinct_count_function(build, true)
    }

    fn create(build: DirectBuildContext<'_, impl Combinator>) -> Result<AggregateCallRef> {
        let has_argument = !build.args_type().is_empty();

        build.create(
            UInt64Type::data_type(),
            CountEval::state_description(),
            CountEval::new(has_argument),
        )
    }

    fn distinct_state_description() -> AggregateStateDescription {
        AggregateStateDescription::new(
            vec![
                AggrStateType::Custom(Layout::new::<AggregateDistinctState>()),
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
    build: DirectBuildContext<'_, impl Combinator>,
    count_argument: bool,
) -> Result<AggregateCallRef> {
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
    let eval = MultiArgSkipNullEval::new(DistinctEval::<false>::new(
        CountEval::new(count_argument),
        args_type,
    ));

    build.create(UInt64Type::data_type(), state, eval)
}

fn create_unary_distinct_count_function(
    build: DirectBuildContext<'_, impl Combinator>,
) -> Result<AggregateCallRef> {
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
    build: DirectBuildContext<'_, impl Combinator>,
) -> Result<AggregateCallRef>
where T: ValueType {
    let state = CountEval::state_description();
    let distinct_arg_type = build.args_type()[0].remove_nullable();
    let eval = UnaryEvalAdapter::new(UnarySkipNull::new(UnaryDistinct::new(
        UnaryStateEval::<AggregateCountState, T, UInt64Type, false>::new(().into()),
        distinct_arg_type,
    )));
    build.create(
        UInt64Type::data_type(),
        unary_distinct_state_description(&state),
        eval,
    )
}

impl CountEval {
    pub fn new(has_argument: bool) -> Self {
        Self { has_argument }
    }

    pub fn state_description() -> AggregateStateDescription {
        AggregateStateDescription::new(
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

impl AggregateEval for CountEval {
    fn init_state(&self, state: AggrState<'_>) {
        state.write(AggregateCountState::default);
    }

    fn accumulate(&self, input: AccumulateInput<'_>) -> Result<()> {
        if !self.has_argument {
            return Err(ErrorCode::BadArguments("count(*) expects rows-only input"));
        }

        let validity = Self::argument_validity(input.columns, input.validity);
        input.state.get::<AggregateCountState>().count +=
            Self::count_valid_rows(input.columns.num_rows(), validity.as_ref());
        Ok(())
    }

    fn accumulate_keys(&self, input: AccumulateKeysInput<'_>) -> Result<()> {
        if !self.has_argument {
            return self.accumulate_row_count_keys(AccumulateRowCountKeysInput {
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

    fn accumulate_row(&self, input: AccumulateRowInput<'_>) -> Result<()> {
        if !self.has_argument || Self::row_is_valid(input.columns, input.row) {
            input.state.get::<AggregateCountState>().count += 1;
        }
        Ok(())
    }

    fn accumulate_row_count(&self, input: AccumulateRowCountInput<'_>) -> Result<()> {
        if self.has_argument {
            return Err(ErrorCode::BadArguments("count(expr) expects column input"));
        }
        input.state.get::<AggregateCountState>().count += input.rows as u64;
        Ok(())
    }

    fn accumulate_row_count_keys(&self, input: AccumulateRowCountKeysInput<'_>) -> Result<()> {
        if self.has_argument {
            return Err(ErrorCode::BadArguments("count(expr) expects column input"));
        }
        for state in input.states.iter() {
            state.get::<AggregateCountState>().count += 1;
        }
        Ok(())
    }

    fn serialize(&self, input: SerializeInput<'_>) -> Result<()> {
        for state in input.states.iter() {
            let state = state.get::<AggregateCountState>();
            input.builders[0].push(ScalarRef::Number(NumberScalar::UInt64(state.count)));
        }
        Ok(())
    }

    fn merge_serialized(&self, input: MergeSerializedInput<'_>) -> Result<()> {
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

    fn merge_states(&self, input: MergeStatesInput<'_>) -> Result<()> {
        input.state.get::<AggregateCountState>().count +=
            input.rhs.get::<AggregateCountState>().count;
        Ok(())
    }

    fn merge_result(&self, input: MergeResultInput<'_>) -> Result<()> {
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
