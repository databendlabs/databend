// Copyright 2026 Databend Labs
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

//! Per-family expression declarations and immutable historical state samples.
use databend_common_expression::aggregate::aggregate_function::RawAggregateCall;
use databend_common_expression::types::DataType;
use databend_common_functions::aggregates::AGGR_REGISTRY;
use declaration::*;

use super::aggregate_state_baseline_support as declaration;

pub fn check(cases: Vec<Case>) {
    let mut signatures = std::collections::HashSet::new();
    for case in cases {
        match case {
            Case::Metadata {
                expression,
                arguments,
                result,
                state,
            } => {
                let call = PreparedCall::new(expression, arguments, state);
                assert!(signatures.insert((call.expression, call.arguments.clone())));
                assert_ne!(call.name(), "avg");
                check_metadata(&call, data_type(result));
            }
            Case::Samples {
                expression,
                arguments,
                result,
                state,
                samples,
            } => {
                let call = PreparedCall::new(expression, arguments, state);
                assert!(signatures.insert((call.expression, call.arguments.clone())));
                assert_ne!(call.name(), "avg");
                check_metadata(&call, data_type(result));
                check_samples(&call, &samples);
            }
        }
    }
}

fn check_samples(call: &PreparedCall, samples: &[Sample]) {
    assert!(!samples.is_empty(), "{}: no state samples", call.expression);
    let mut labels = std::collections::HashSet::new();
    for sample in samples {
        assert!(labels.insert(sample.label));
        assert_eq!(sample.inputs.len(), call.arguments.len());
        let rows = sample.inputs.first().map_or(0, |column| column.len());
        for (column, expected_type) in sample.inputs.iter().zip(&call.arguments) {
            assert_eq!(column.len(), rows);
            assert_eq!(&column.data_type(), expected_type);
        }
    }
    read_states(call, samples);
}

fn check_metadata(case: &PreparedCall, result_type: DataType) {
    let mut checked = 0;
    let mut failed = 0;
    let name = case.name();
    let old_state = case.state_type();
    for (route, args, result) in [
        (name.clone(), case.arguments.clone(), &result_type),
        (format!("{name}_state"), case.arguments.clone(), &old_state),
        (
            format!("{name}_merge"),
            vec![old_state.clone()],
            &result_type,
        ),
        (
            format!("{name}_merge_state"),
            vec![old_state.clone()],
            &old_state,
        ),
    ] {
        checked += 1;
        let resolved = AGGR_REGISTRY.resolve(RawAggregateCall {
            name: &route,
            params: &[],
            args_type: &args,
            distinct: false,
            order_by: &[],
        });
        let error = match resolved {
            Err(error) => Some(error.to_string()),
            Ok(function) => {
                if function.signature().return_type != *result {
                    Some(format!(
                        "result: expected {result:?}, got {:?}",
                        function.signature().return_type
                    ))
                } else if route == name && function.state_data_type() != case.state {
                    Some(format!(
                        "state: expected {:?}, got {:?}",
                        case.state,
                        function.state_data_type()
                    ))
                } else {
                    None
                }
            }
        };
        if let Some(error) = error {
            failed += 1;
            eprintln!(
                "{} {:?} / {route}: {error}",
                case.expression, case.arguments
            );
        }
    }
    assert_eq!(failed, 0, "{failed}/{checked} routes differ");
}

fn read_states(case: &PreparedCall, samples: &[Sample]) {
    use databend_common_expression::BlockEntry;
    use databend_common_expression::ColumnBuilder;

    use super::aggregate_function_v2_support::eval_v2_aggr;

    let name = case.name();
    let name = name.as_str();
    for sample in samples {
        let state_type = case.state_type();
        let old_state = sample.state.clone();
        let expected = sample.result.clone();
        let entries = sample
            .inputs
            .iter()
            .cloned()
            .map(BlockEntry::from)
            .collect::<Vec<_>>();
        let rows = sample.inputs.first().map_or(0, |column| column.len());
        for serialized in [false, true] {
            let (result, _) = eval_v2_aggr(name, &entries, rows, serialized).unwrap();
            assert_eq!(
                result.index(0).unwrap().to_owned(),
                expected,
                "{name}: {}",
                sample.label
            );
        }
        // These saved samples use the same expected result for ordinary
        // evaluation and reading historical state.
        let (new_state, new_type) =
            eval_v2_aggr(&format!("{name}_state"), &entries, rows, false).unwrap();
        assert_eq!(new_type, state_type);
        // Compaction resolves the ordinary aggregate from stored metadata
        // and merges serialized states directly, without the _merge route.
        {
            use databend_common_expression::aggregate::aggregate_function::*;
            use databend_common_functions::aggregates::AGGR_REGISTRY;
            let args = entries
                .iter()
                .map(BlockEntry::data_type)
                .collect::<Vec<_>>();
            let function = AGGR_REGISTRY
                .resolve(RawAggregateCall {
                    name,
                    params: &[],
                    args_type: &args,
                    distinct: false,
                    order_by: &[],
                })
                .unwrap();
            assert_eq!(function.state_data_type(), case.state);
            let owner = AggregateStateOwner::new(vec![function.clone()]).unwrap();
            let old =
                BlockEntry::new_const_column(function.state_data_type(), old_state.clone(), 1);
            function
                .merge_serialized(MergeSerializedInput {
                    states: owner.state_set(0),
                    state: &old,
                    filter: None,
                })
                .unwrap();
            let merged = AggregateStateOwner::new(vec![function.clone()]).unwrap();
            function
                .merge_states(MergeStatesInput {
                    state: merged.state(0),
                    rhs: owner.state(0),
                })
                .unwrap();
            let mut builder = ColumnBuilder::with_capacity(&function.signature().return_type, 1);
            function
                .merge_result(MergeResultInput {
                    state: merged.state(0),
                    builder: &mut builder,
                })
                .unwrap();
            assert_eq!(builder.build().index(0).unwrap().to_owned(), expected);
            let merge_result = match &sample.merge_result {
                MergeResult::Skip => None,
                MergeResult::SameAsResult => Some(&sample.result),
                MergeResult::Value(result) => Some(result),
            };
            if let Some(expected) = merge_result {
                // Mix an old persisted state with a newly produced state,
                // rather than testing each generation only in isolation.
                let expected = expected.clone();
                let fresh = AggregateStateOwner::new(vec![function.clone()]).unwrap();
                let new = BlockEntry::new_const_column(
                    function.state_data_type(),
                    new_state.index(0).unwrap().to_owned(),
                    1,
                );
                function
                    .merge_serialized(MergeSerializedInput {
                        states: fresh.state_set(0),
                        state: &new,
                        filter: None,
                    })
                    .unwrap();
                function
                    .merge_serialized(MergeSerializedInput {
                        states: fresh.state_set(0),
                        state: &old,
                        filter: None,
                    })
                    .unwrap();
                let mut builder =
                    ColumnBuilder::with_capacity(&function.signature().return_type, 1);
                function
                    .merge_result(MergeResultInput {
                        state: fresh.state(0),
                        builder: &mut builder,
                    })
                    .unwrap();
                assert_eq!(
                    builder.build().index(0).unwrap().to_owned(),
                    expected,
                    "mixed old/new {name}: {}",
                    sample.label
                );
            }
        }
        // Old persisted bytes and freshly produced states must both remain
        // readable through the saved table's AggregateState metadata.
        for state in [old_state, new_state.index(0).unwrap().to_owned()] {
            let entry = BlockEntry::new_const_column(state_type.clone(), state, 1);
            for serialized in [false, true] {
                let (result, _) = eval_v2_aggr(
                    &format!("{name}_merge"),
                    std::slice::from_ref(&entry),
                    1,
                    serialized,
                )
                .unwrap();
                assert_eq!(
                    result.index(0).unwrap().to_owned(),
                    expected,
                    "{name}: {}",
                    sample.label
                );
            }
            let (merged, merged_type) =
                eval_v2_aggr(&format!("{name}_merge_state"), &[entry], 1, false).unwrap();
            assert_eq!(merged_type, state_type);
            let entry = BlockEntry::new_const_column(
                state_type.clone(),
                merged.index(0).unwrap().to_owned(),
                1,
            );
            let (result, _) = eval_v2_aggr(&format!("{name}_merge"), &[entry], 1, false).unwrap();
            assert_eq!(result.index(0).unwrap().to_owned(), expected);
        }
    }
}
