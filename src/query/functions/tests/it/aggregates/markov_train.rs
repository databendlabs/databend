use std::io::Write;

use databend_common_expression::FromData;
use goldenfile::Mint;

use super::aggregate_case_support::eval_legacy_aggregate;
use super::aggregate_simulation_support::AggregationSimulator;
use super::aggregate_simulation_support::simulate_two_groups_group_by;
use super::aggregate_simulation_support::write_aggregate_expr_case;

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
    run_markov_train_cases(file, eval_legacy_aggregate);
}

#[test]
fn test_markov_train_group_by() {
    let mut mint = Mint::new("tests/it/aggregates/testdata");
    let file = &mut mint.new_goldenfile("markov_train_group_by.txt").unwrap();
    run_markov_train_cases(file, simulate_two_groups_group_by);
}
