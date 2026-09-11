use std::io::Write;

use borsh::BorshDeserialize;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::FromData;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::StateSerdeItem;
use databend_common_expression::Symbol;
use databend_common_expression::aggregate_function::AccumulateInput;
use databend_common_expression::aggregate_function::AggregateBoundOrderByItem;
use databend_common_expression::aggregate_function::AggregateBoundOrderBySource;
use databend_common_expression::aggregate_function::AggregateStateOwner;
use databend_common_expression::aggregate_function::FunctionInputLayout;
use databend_common_expression::aggregate_function::MergeResultInput;
use databend_common_expression::aggregate_function::RawAggregateCall;
use databend_common_expression::aggregate_function::SerializeInput;
use databend_common_expression::types::BooleanType;
use databend_common_expression::types::DateType;
use databend_common_expression::types::Decimal64Type;
use databend_common_expression::types::DecimalSize;
use databend_common_expression::types::Int64Type;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_functions::aggregates::AGGR_REGISTRY;
use goldenfile::Mint;

use super::support::AggregationSimulator;
use super::support::eval_aggregate;
use super::support::simulate_two_groups_group_by;
use super::support::write_aggregate_expr_case;

fn run_string_agg_cases(file: &mut impl Write, simulator: impl AggregationSimulator) {
    let columns = [
        (
            "s",
            StringType::from_data(vec!["abc", "def", "opq", "xyz"]).into(),
        ),
        ("a", Int64Type::from_data(vec![4i64, 3, 2, 1]).into()),
        (
            "event",
            BooleanType::from_data(vec![true, false, true, false]).into(),
        ),
        (
            "dec",
            Decimal64Type::from_data_with_size(
                vec![400_i64, 300, 200, 100],
                Some(DecimalSize::new_unchecked(15, 2)),
            )
            .into(),
        ),
        ("date_col", DateType::from_data(vec![1, 2, 1, 3]).into()),
        ("ts", TimestampType::from_data(vec![10, 20, 10, 30]).into()),
        (
            "json",
            StringType::from_data(vec![r#"{"k":1}"#, r#"{"k":2}"#, r#"{"k":1}"#, r#"null"#]).into(),
        ),
        (
            "s_all_null",
            StringType::from_data_with_validity(vec!["a", "b", "c", "d"], vec![
                false, false, false, false,
            ])
            .into(),
        ),
        (
            "s_null",
            StringType::from_data_with_validity(vec!["a", "", "c", "d"], vec![
                true, false, true, true,
            ])
            .into(),
        ),
    ];
    let columns = columns.as_slice();

    write_aggregate_expr_case(file, "string_agg(s)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "string_agg(s)", columns, simulator, vec![
        AggregateBoundOrderByItem {
            index: Symbol::new(0),
            source: AggregateBoundOrderBySource::Argument { index: 0 },
            data_type: columns[0].1.data_type(),
            nulls_first: false,
            asc: false,
        },
    ]);
    write_aggregate_expr_case(file, "string_agg(NULL)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "string_agg(s_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "string_agg(s_all_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "string_agg(s, '|')", columns, simulator, vec![]);
    for expression in [
        "listagg(s, '|')",
        "group_concat(s, '|')",
        "string_agg_if(s, '|', event)",
        "listagg_if(s, '|', event)",
        "group_concat_if(s, '|', event)",
    ] {
        write_aggregate_expr_case(file, expression, columns, simulator, vec![]);
    }
    write_aggregate_expr_case(file, "string_agg(a, '|')", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "string_agg(event)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "string_agg(dec)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "string_agg(date_col)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "string_agg(ts)", columns, simulator, vec![]);
    write_aggregate_expr_case(
        file,
        "string_agg(parse_json(json))",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(file, "string_agg(NULL, '|')", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "string_agg(s_null, '-')", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "group_concat(s)", columns, simulator, vec![
        AggregateBoundOrderByItem {
            index: Symbol::new(0),
            source: AggregateBoundOrderBySource::Argument { index: 0 },
            data_type: columns[0].1.data_type(),
            nulls_first: false,
            asc: false,
        },
    ]);
    write_aggregate_expr_case(
        file,
        "string_agg(s_all_null, '|')",
        columns,
        simulator,
        vec![],
    );
}

#[test]
fn test_string_agg() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("string_agg.txt").unwrap();
    run_string_agg_cases(file, eval_aggregate);
}

#[test]
fn test_string_agg_group_by() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("string_agg_group_by.txt").unwrap();
    run_string_agg_cases(file, simulate_two_groups_group_by);
}

#[test]
fn test_string_agg_registers_distinct_aliases() {
    let params = [Scalar::String("|".to_string())];
    for name in [
        "string_agg_distinct",
        "listagg_distinct",
        "group_concat_distinct",
    ] {
        let function = AGGR_REGISTRY
            .resolve(RawAggregateCall {
                name,
                params: &params,
                args_type: &[databend_common_expression::types::DataType::String],
                distinct: false,
                order_by: &[],
            })
            .unwrap();
        assert_eq!(function.signature().name, name);
    }
}

#[test]
fn test_ordered_listagg_if_filters_before_sorting() -> Result<()> {
    let values: BlockEntry = StringType::from_data(vec!["abc", "def", "opq", "xyz"]).into();
    let condition: BlockEntry =
        BooleanType::from_data_with_validity(vec![true, false, false, true], vec![
            true, true, false, true,
        ])
        .into();
    let order_key: BlockEntry =
        Int64Type::from_data_with_validity(vec![3, 2, 1, 0], vec![true, true, true, false]).into();
    let args_type = [values.data_type(), condition.data_type()];
    let order_by = [AggregateBoundOrderByItem {
        index: Symbol::new(2),
        source: AggregateBoundOrderBySource::Derived,
        data_type: order_key.data_type(),
        nulls_first: false,
        asc: false,
    }];
    let params = [Scalar::String("|".to_string())];
    let function = AGGR_REGISTRY.resolve(RawAggregateCall {
        name: "listagg_if",
        params: &params,
        args_type: &args_type,
        distinct: false,
        order_by: &order_by,
    })?;
    let owner = AggregateStateOwner::new(vec![function.clone()])?;
    let columns = [values, condition, order_key];
    let input_layout = function.input_layout();
    assert_eq!(
        input_layout,
        &FunctionInputLayout::Projection(vec![0, 2, 1])
    );
    let columns = input_layout.project(&columns)?;
    function.accumulate(AccumulateInput {
        state: owner.state(0),
        columns: columns.as_ref().into(),
        validity: None,
    })?;

    let mut state_builders = function
        .state()
        .serde_items()
        .iter()
        .map(|item| match item {
            StateSerdeItem::DataType(data_type) => ColumnBuilder::with_capacity(data_type, 1),
            StateSerdeItem::Binary(_) => ColumnBuilder::with_capacity(
                &databend_common_expression::types::DataType::Binary,
                1,
            ),
        })
        .collect::<Vec<_>>();
    function.serialize(SerializeInput {
        states: owner.state_set(0),
        builders: &mut state_builders,
    })?;
    let sort_state = state_builders.remove(0).build();
    let ScalarRef::Binary(mut sort_state) = sort_state.index(0).unwrap() else {
        unreachable!("sort state must serialize as binary")
    };
    let buffered_columns = Vec::<Column>::deserialize(&mut sort_state)?;
    assert_eq!(buffered_columns.len(), 2);
    assert!(buffered_columns.iter().all(|column| column.len() == 2));

    let mut builder = ColumnBuilder::with_capacity(&function.signature().return_type, 1);
    function.merge_result(MergeResultInput {
        state: owner.state(0),
        builder: &mut builder,
    })?;
    let result = builder.build();
    assert_eq!(
        unsafe { result.index_unchecked(0) },
        ScalarRef::String("abc|xyz")
    );
    Ok(())
}

// string_agg.rs: String/Boolean/NumberType and AnyType formatting paths;
// nullable samples cover the outer presence flag. Aliases share this implementation.
#[test]
fn test_state_baselines() {
    use databend_common_expression::FromData;
    use databend_common_expression::types::StringType;

    use super::support::Case;
    use super::support::MergeResult;
    use super::support::Sample;
    use super::support::string;
    use super::support::tuple;

    super::support::check_state_baselines(vec![
        Case::Metadata {
            expression: "string_agg(x0)",
            arguments: vec!["Boolean"],
            result: "Nullable(String)",
            state: "Tuple(String, Boolean)",
        },
        Case::Metadata {
            expression: "string_agg(x0)",
            arguments: vec!["Float64"],
            result: "Nullable(String)",
            state: "Tuple(String, Boolean)",
        },
        Case::Metadata {
            expression: "string_agg(x0)",
            arguments: vec!["Int64"],
            result: "Nullable(String)",
            state: "Tuple(String, Boolean)",
        },
        Case::Metadata {
            expression: "string_agg(x0)",
            arguments: vec!["Variant"],
            result: "Nullable(String)",
            state: "Tuple(String, Boolean)",
        },
        Case::Metadata {
            expression: "string_agg(x0)",
            arguments: vec!["Decimal(15, 2)"],
            result: "Nullable(String)",
            state: "Tuple(String, Boolean)",
        },
        Case::Samples {
            expression: "string_agg(x0)",
            arguments: vec!["String"],
            result: "Nullable(String)",
            state: "Tuple(String, Boolean)",
            samples: vec![
                Sample {
                    label: "string/false/empty",
                    inputs: vec![StringType::from_data(Vec::<&str>::new())],
                    state: tuple(vec![string(""), Scalar::Boolean(false)]),
                    result: Scalar::Null,
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "string/false/mixed",
                    inputs: vec![StringType::from_data(vec!["-220", "", "110", "500", "900"])],
                    state: tuple(vec![string("-220110500900"), Scalar::Boolean(true)]),
                    result: string("-220110500900"),
                    merge_result: MergeResult::Value(string("-220110500900-220110500900")),
                },
            ],
        },
        Case::Samples {
            expression: "string_agg(x0)",
            arguments: vec!["Nullable(String)"],
            result: "Nullable(String)",
            state: "Tuple(String, Boolean, Boolean)",
            samples: vec![
                Sample {
                    label: "string/true/empty",
                    inputs: vec![StringType::from_opt_data(Vec::<Option<&str>>::new())],
                    state: tuple(vec![
                        string(""),
                        Scalar::Boolean(false),
                        Scalar::Boolean(false),
                    ]),
                    result: Scalar::Null,
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "string/true/all_null",
                    inputs: vec![StringType::from_opt_data(vec![None::<&str>; 2])],
                    state: tuple(vec![
                        string(""),
                        Scalar::Boolean(false),
                        Scalar::Boolean(true),
                    ]),
                    result: Scalar::Null,
                    merge_result: MergeResult::SameAsResult,
                },
                Sample {
                    label: "string/true/mixed",
                    inputs: vec![StringType::from_opt_data(vec![
                        Some("-220"),
                        None,
                        Some("110"),
                        Some("500"),
                        Some("900"),
                    ])],
                    state: tuple(vec![
                        string("-220110500900"),
                        Scalar::Boolean(true),
                        Scalar::Boolean(true),
                    ]),
                    result: string("-220110500900"),
                    merge_result: MergeResult::Value(string("-220110500900-220110500900")),
                },
            ],
        },
    ]);
}
