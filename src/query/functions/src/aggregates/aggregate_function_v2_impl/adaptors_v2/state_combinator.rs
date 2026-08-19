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
use std::sync::Arc;

use databend_common_column::bitmap::Bitmap;
use databend_common_exception::Result;
use databend_common_expression::AggrState;
use databend_common_expression::AggrStateType;
use databend_common_expression::BlockEntry;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ProjectedBlock;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::StateSerdeItem;
use databend_common_expression::StateSerdeType;
use databend_common_expression::types::AggregateFunctionParam;
use databend_common_expression::types::AggregateStateDataType;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::utils::column_merge_validity;

use crate::aggregates::aggregate_function_v2_impl::adaptors_v2 as v2;

pub(crate) fn aggregate_state_data_type(
    function_name: &str,
    params: &[databend_common_expression::Scalar],
    argument_types: Vec<DataType>,
    physical_type: DataType,
) -> Result<DataType> {
    let params = params
        .iter()
        .cloned()
        .map(AggregateFunctionParam::try_from)
        .collect::<Result<Vec<_>>>()?;
    Ok(DataType::AggregateState(Box::new(AggregateStateDataType {
        function_name: function_name.to_string(),
        params,
        argument_types,
        state_type: Box::new(physical_type),
    })))
}

pub(crate) struct AggregateStateImplementation<I> {
    nested: I,
    strip_nullable_input: bool,
    nullable_input_result_flag: bool,
}

impl<I> AggregateStateImplementation<I> {
    pub(crate) fn new(
        nested: I,
        strip_nullable_input: bool,
        nullable_input_result_flag: bool,
    ) -> Self {
        Self {
            nested,
            strip_nullable_input,
            nullable_input_result_flag,
        }
    }

    fn nested_state<'a>(&self, state: AggrState<'a>) -> AggrState<'a> {
        if self.nullable_input_result_flag {
            state.remove_last_loc()
        } else {
            state
        }
    }

    fn nested_states<'a>(&self, states: v2::AggregateStateSet<'a>) -> v2::AggregateStateSet<'a> {
        if self.nullable_input_result_flag {
            states.without_last_loc()
        } else {
            states
        }
    }

    fn nullable_input_flag(state: AggrState<'_>) -> &mut u8 {
        debug_assert!(state.loc.last().unwrap().is_bool());
        state
            .addr
            .next(state.loc.last().unwrap().offset())
            .get::<u8>()
    }

    fn mark_nullable_input_seen(rows: usize, validity: Option<&Bitmap>) -> bool {
        rows > 0 && validity.is_none_or(|validity| validity.null_count() != rows)
    }

    fn strip_nullable_columns(
        columns: ProjectedBlock<'_>,
        validity: Option<Bitmap>,
    ) -> (Vec<BlockEntry>, Option<Bitmap>) {
        let mut not_null_columns = Vec::with_capacity(columns.len());
        let mut validity = validity;
        for entry in columns.iter() {
            validity = column_merge_validity(entry, validity);
            not_null_columns.push(entry.clone().remove_nullable());
        }
        (not_null_columns, Bitmap::map_all_sets_to_none(validity))
    }
}

impl<I> v2::AggrImpl for AggregateStateImplementation<I>
where I: v2::AggrImpl
{
    fn init_state(&self, state: AggrState<'_>) {
        if self.nullable_input_result_flag {
            *Self::nullable_input_flag(state) = 0;
        }
        self.nested.init_state(self.nested_state(state))
    }

    fn accumulate(&self, input: v2::AccumulateInput<'_>) -> Result<()> {
        if self.strip_nullable_input {
            let (columns, validity) =
                Self::strip_nullable_columns(input.columns, input.validity.cloned());
            if self.nullable_input_result_flag
                && Self::mark_nullable_input_seen(input.columns.num_rows(), validity.as_ref())
            {
                *Self::nullable_input_flag(input.state) = 1;
            }
            return self.nested.accumulate(v2::AccumulateInput {
                state: self.nested_state(input.state),
                columns: (&columns).into(),
                order_by: input.order_by,
                validity: validity.as_ref(),
            });
        }
        self.nested.accumulate(input)
    }

    fn accumulate_keys(&self, input: v2::AccumulateKeysInput<'_>) -> Result<()> {
        if self.strip_nullable_input {
            let (columns, validity) = Self::strip_nullable_columns(input.columns, None);
            let columns: ProjectedBlock<'_> = (&columns).into();
            if let Some(validity) = validity {
                for (row, state) in input.states.iter().enumerate() {
                    if validity.get(row).unwrap() {
                        if self.nullable_input_result_flag {
                            *Self::nullable_input_flag(state) = 1;
                        }
                        self.nested.accumulate_row(v2::AccumulateRowInput {
                            state: self.nested_state(state),
                            columns,
                            row,
                        })?;
                    }
                }
                return Ok(());
            }
            if self.nullable_input_result_flag {
                for state in input.states.iter() {
                    *Self::nullable_input_flag(state) = 1;
                }
            }
            return self.nested.accumulate_keys(v2::AccumulateKeysInput {
                states: self.nested_states(input.states),
                columns,
                order_by: input.order_by,
            });
        }
        self.nested.accumulate_keys(input)
    }

    fn accumulate_row(&self, input: v2::AccumulateRowInput<'_>) -> Result<()> {
        if self.strip_nullable_input {
            let (columns, validity) = Self::strip_nullable_columns(input.columns, None);
            if validity
                .as_ref()
                .is_some_and(|validity| !validity.get(input.row).unwrap())
            {
                return Ok(());
            }
            if self.nullable_input_result_flag {
                *Self::nullable_input_flag(input.state) = 1;
            }
            return self.nested.accumulate_row(v2::AccumulateRowInput {
                state: self.nested_state(input.state),
                columns: (&columns).into(),
                row: input.row,
            });
        }
        self.nested.accumulate_row(input)
    }

    fn accumulate_row_count(&self, input: v2::AccumulateRowCountInput<'_>) -> Result<()> {
        if self.nullable_input_result_flag {
            if input.rows > 0 {
                *Self::nullable_input_flag(input.state) = 1;
            }
            return self
                .nested
                .accumulate_row_count(v2::AccumulateRowCountInput {
                    state: self.nested_state(input.state),
                    rows: input.rows,
                });
        }
        self.nested.accumulate_row_count(input)
    }

    fn accumulate_row_count_keys(&self, input: v2::AccumulateRowCountKeysInput<'_>) -> Result<()> {
        if self.nullable_input_result_flag {
            for state in input.states.iter() {
                *Self::nullable_input_flag(state) = 1;
            }
            return self
                .nested
                .accumulate_row_count_keys(v2::AccumulateRowCountKeysInput {
                    states: self.nested_states(input.states),
                });
        }
        self.nested.accumulate_row_count_keys(input)
    }

    fn serialize(&self, input: v2::SerializeInput<'_>) -> Result<()> {
        if self.nullable_input_result_flag {
            let (nested_builders, flag_builder) =
                input.builders.split_at_mut(input.builders.len() - 1);
            for state in input.states.iter() {
                flag_builder[0].push(ScalarRef::Boolean(*Self::nullable_input_flag(state) != 0));
            }
            return self.nested.serialize(v2::SerializeInput {
                states: self.nested_states(input.states),
                builders: nested_builders,
            });
        }
        self.nested.serialize(input)
    }

    fn merge_serialized(&self, input: v2::MergeSerializedInput<'_>) -> Result<()> {
        if self.nullable_input_result_flag {
            let field_count = v2::serialized_field_count(input.state);
            let flag_field = field_count - 1;
            let flag_filter =
                v2::combined_serialized_flag_filter(input.state, input.filter, flag_field);
            for (row, state) in input.states.iter().enumerate() {
                if flag_filter
                    .as_ref()
                    .is_none_or(|filter| filter.get(row).unwrap())
                {
                    *Self::nullable_input_flag(state) = 1;
                }
            }
            let nested_state = v2::project_serialized_fields(input.state, 0, flag_field);
            return self.nested.merge_serialized(v2::MergeSerializedInput {
                states: self.nested_states(input.states),
                state: &nested_state,
                filter: flag_filter.as_ref(),
            });
        }
        self.nested.merge_serialized(input)
    }

    fn merge_states(&self, input: v2::MergeStatesInput<'_>) -> Result<()> {
        if self.nullable_input_result_flag {
            if *Self::nullable_input_flag(input.rhs) != 0 {
                *Self::nullable_input_flag(input.state) = 1;
            }
            return self.nested.merge_states(v2::MergeStatesInput {
                state: self.nested_state(input.state),
                rhs: self.nested_state(input.rhs),
            });
        }
        self.nested.merge_states(input)
    }

    fn merge_result(&self, input: v2::MergeResultInput<'_>) -> Result<()> {
        let places = [input.state.addr];
        let builders = input.builder.as_tuple_mut().unwrap().as_mut_slice();
        if self.nullable_input_result_flag {
            let (nested_builders, flag_builder) = builders.split_at_mut(builders.len() - 1);
            flag_builder[0].push(ScalarRef::Boolean(
                *Self::nullable_input_flag(input.state) != 0,
            ));
            return self.nested.serialize(v2::SerializeInput {
                states: v2::AggregateStateSet::new(&places, input.state.loc).without_last_loc(),
                builders: nested_builders,
            });
        }
        self.nested.serialize(v2::SerializeInput {
            states: v2::AggregateStateSet::new(&places, input.state.loc),
            builders,
        })
    }

    fn merge_result_read_only(&self, input: v2::MergeResultInput<'_>) -> Result<()> {
        self.merge_result(input)
    }

    unsafe fn drop_state(&self, state: AggrState<'_>) {
        unsafe { self.nested.drop_state(self.nested_state(state)) };
    }
}

pub(crate) fn nullable_input_state_description(
    state: &v2::AggregateStateDescription,
) -> v2::AggregateStateDescription {
    let mut fields = state.fields().to_vec();
    fields.push(AggrStateType::Bool);
    let mut serde_items = state.serde_items().to_vec();
    serde_items.push(StateSerdeItem::DataType(DataType::Boolean));
    v2::AggregateStateDescription::new(fields, serde_items)
        .with_manual_drop(state.need_manual_drop())
}

pub(crate) fn create_state_null_result_function(
    request: v2::AggregateFunctionRequest<'_>,
    returns_default_when_only_null: bool,
) -> Result<v2::AggregateFunctionRef> {
    let (data_type, result) = if returns_default_when_only_null {
        (
            DataType::Number(NumberDataType::UInt64),
            Scalar::Number(NumberScalar::UInt64(0)),
        )
    } else {
        (DataType::Null, Scalar::Null)
    };
    let serde_item = StateSerdeItem::DataType(data_type.clone());
    let physical_type = StateSerdeType::new(vec![serde_item.clone()]).data_type();
    let function_name = request
        .name
        .strip_suffix("_state")
        .expect("state combinator names must end with _state");
    let return_type = aggregate_state_data_type(
        function_name,
        request.params,
        request.args_type.to_vec(),
        physical_type,
    )?;
    let signature = v2::AggregateFunctionSignature {
        name: request.name.to_string(),
        params: request.params.to_vec(),
        args_type: request.args_type.to_vec(),
        distinct: request.distinct,
        order_by: request.order_by.to_vec(),
        return_type,
    };
    let state =
        v2::AggregateStateDescription::new(vec![AggrStateType::Custom(Layout::new::<u8>())], vec![
            serde_item,
        ]);
    Ok(Arc::new(v2::AggregateFunction::new(
        signature,
        AggregateStateNullResultImplementation::FEATURES,
        state,
        AggregateStateNullResultImplementation { result },
    )))
}

struct AggregateStateNullResultImplementation {
    result: Scalar,
}

impl AggregateStateNullResultImplementation {
    const FEATURES: v2::FunctionFeatures = v2::FunctionFeatures {
        is_decomposable: false,
        sort_policy: v2::SortPolicy::Unsupported,
        distinct_policy: v2::DistinctPolicy::Unsupported,
        category: "",
        description: "",
        definition: "",
        example: "",
    };

    fn push_result(&self, builder: &mut ColumnBuilder) {
        builder.push(self.result.as_ref());
    }
}

impl v2::AggrImpl for AggregateStateNullResultImplementation {
    fn init_state(&self, _state: AggrState<'_>) {}

    fn accumulate(&self, _input: v2::AccumulateInput<'_>) -> Result<()> {
        Ok(())
    }

    fn accumulate_keys(&self, _input: v2::AccumulateKeysInput<'_>) -> Result<()> {
        Ok(())
    }

    fn accumulate_row(&self, _input: v2::AccumulateRowInput<'_>) -> Result<()> {
        Ok(())
    }

    fn accumulate_row_count(&self, _input: v2::AccumulateRowCountInput<'_>) -> Result<()> {
        Ok(())
    }

    fn accumulate_row_count_keys(&self, _input: v2::AccumulateRowCountKeysInput<'_>) -> Result<()> {
        Ok(())
    }

    fn serialize(&self, input: v2::SerializeInput<'_>) -> Result<()> {
        for _state in input.states.iter() {
            self.push_result(&mut input.builders[0]);
        }
        Ok(())
    }

    fn merge_serialized(&self, _input: v2::MergeSerializedInput<'_>) -> Result<()> {
        Ok(())
    }

    fn merge_states(&self, _input: v2::MergeStatesInput<'_>) -> Result<()> {
        Ok(())
    }

    fn merge_result(&self, input: v2::MergeResultInput<'_>) -> Result<()> {
        if let Some(fields) = input.builder.as_tuple_mut() {
            self.push_result(&mut fields[0]);
        } else {
            self.push_result(input.builder);
        }
        Ok(())
    }

    fn merge_result_read_only(&self, input: v2::MergeResultInput<'_>) -> Result<()> {
        self.merge_result(input)
    }

    unsafe fn drop_state(&self, _state: AggrState<'_>) {}
}
