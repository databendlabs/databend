use std::io::Write;

use databend_common_expression::FromData;
use goldenfile::Mint;

use super::support::AggregationSimulator;
use super::support::eval_aggregate;
use super::support::simulate_two_groups_group_by;
use super::support::write_aggregate_expr_case;

fn run_markov_train_cases(file: &mut impl Write, simulator: impl AggregationSimulator) {
    let columns = [
        (
            "text",
            databend_common_expression::types::StringType::from_data(vec![
                "alpha", "alpine", "alpha", "alphabet",
            ])
            .into(),
        ),
        (
            "text_null",
            databend_common_expression::types::StringType::from_data_with_validity(
                vec!["alpha", "alpine", "alpha", "alphabet"],
                vec![true, false, true, true],
            )
            .into(),
        ),
        (
            "text_all_null",
            databend_common_expression::types::StringType::from_data_with_validity(
                vec!["alpha", "alpine", "alpha", "alphabet"],
                vec![false, false, false, false],
            )
            .into(),
        ),
        (
            "keep",
            databend_common_expression::types::BooleanType::from_data(vec![
                true, false, true, false,
            ])
            .into(),
        ),
    ];
    let columns = columns.as_slice();

    write_aggregate_expr_case(file, "markov_train(text)", columns, simulator, vec![]);
    write_aggregate_expr_case(file, "markov_train(1)(text)", columns, simulator, vec![]);
    write_aggregate_expr_case(
        file,
        "markov_train(1, 1, 0, 1, 0.5)(text)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(file, "markov_train(text_null)", columns, simulator, vec![]);
    write_aggregate_expr_case(
        file,
        "markov_train(text_all_null)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(file, "markov_train(NULL)", columns, simulator, vec![]);
    write_aggregate_expr_case(
        file,
        "markov_train_if(text, keep)",
        columns,
        simulator,
        vec![],
    );
    write_aggregate_expr_case(
        file,
        "markov_train_distinct(text)",
        columns,
        simulator,
        vec![],
    );
}

#[test]
fn test_markov_train() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("markov_train.txt").unwrap();
    run_markov_train_cases(file, eval_aggregate);
}

#[test]
fn test_markov_train_group_by() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("markov_train_group_by.txt").unwrap();
    run_markov_train_cases(file, simulate_two_groups_group_by);
}

// markov_train.rs requires String at execution. Non-String v1 factory-only
// signatures are not useful compatibility representatives and are omitted.
#[test]
fn test_state_baselines() {
    use super::support::Case;

    super::support::check_state_baselines(vec![
        Case::Metadata {
            expression: "markov_train(x0)",
            arguments: vec!["String"],
            result: "Nullable(Array(Tuple(UInt32, UInt32, UInt32, Map(UInt32, UInt32))))",
            state: "Tuple(Binary, Boolean)",
        },
        Case::Metadata {
            expression: "markov_train(x0)",
            arguments: vec!["Nullable(String)"],
            result: "Nullable(Array(Tuple(UInt32, UInt32, UInt32, Map(UInt32, UInt32))))",
            state: "Tuple(Binary, Boolean, Boolean)",
        },
    ]);
}

#[test]
fn test_markov_train_distinct_null_argument() -> databend_common_exception::Result<()> {
    use databend_common_expression::BlockEntry;
    use databend_common_expression::Scalar;
    use databend_common_expression::ScalarRef;
    use databend_common_expression::types::DataType;

    use super::support::eval_v2_aggr;

    let entry = BlockEntry::new_const_column(DataType::Null, Scalar::Null, 3);
    for with_serialize in [false, true] {
        let (result, _) = eval_v2_aggr(
            "markov_train_distinct",
            std::slice::from_ref(&entry),
            3,
            with_serialize,
        )?;
        assert_eq!(result.index(0).unwrap(), ScalarRef::Null);
    }
    Ok(())
}
