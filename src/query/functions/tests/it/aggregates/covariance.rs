use databend_common_exception::Result;
use databend_common_expression::FromData;
use databend_common_expression::types::UInt64Type;

use super::support::assert_single_float_close;
use super::support::eval_v2_aggr;

#[test]
fn test_v2_covariance_pop_matches_expected_formula() -> Result<()> {
    let entries = [
        UInt64Type::from_data(vec![1, 2, 3, 4]).into(),
        UInt64Type::from_data(vec![2, 4, 6, 8]).into(),
    ];
    let direct_v2 = eval_v2_aggr("covar_pop", &entries, 4, false)?;
    let serialized_v2 = eval_v2_aggr("covar_pop", &entries, 4, true)?;

    assert_single_float_close(&direct_v2, 2.5);
    assert_eq!(serialized_v2, direct_v2);
    Ok(())
}

#[test]
fn test_v2_covariance_samp_matches_expected_formula() -> Result<()> {
    let entries = [
        UInt64Type::from_data(vec![1, 2, 3, 4]).into(),
        UInt64Type::from_data(vec![2, 4, 6, 8]).into(),
    ];
    let direct_v2 = eval_v2_aggr("covar_samp", &entries, 4, false)?;
    let serialized_v2 = eval_v2_aggr("covar_samp", &entries, 4, true)?;

    assert_single_float_close(&direct_v2, 10.0 / 3.0);
    assert_eq!(serialized_v2, direct_v2);
    Ok(())
}

// covariance.rs: population/sample kernels share numeric state. One integer
// conversion and asymmetric/both-nullable inputs replace the type cross product.
#[test]
fn test_state_baselines() {
    use databend_common_expression::FromData;
    use databend_common_expression::Scalar;
    use databend_common_expression::types::Int64Type;

    use super::support::Case;
    use super::support::MergeResult;
    use super::support::Sample;
    use super::support::binary;
    use super::support::float64;
    use super::support::tuple;

    super::support::check_state_baselines(vec![
        Case::Metadata {
            expression: "covar_pop(x0, x1)",
            arguments: vec!["Float64", "Float64"],
            result: "Nullable(Float64)",
            state: "Tuple(Binary, Boolean)",
        },
        Case::Metadata {
            expression: "covar_pop(x0, x1)",
            arguments: vec!["Int64", "Int64"],
            result: "Nullable(Float64)",
            state: "Tuple(Binary, Boolean)",
        },
        Case::Metadata {
            expression: "covar_pop(x0, x1)",
            arguments: vec!["Nullable(Float64)", "Float64"],
            result: "Nullable(Float64)",
            state: "Tuple(Binary, Boolean, Boolean)",
        },
        Case::Metadata {
            expression: "covar_pop(x0, x1)",
            arguments: vec!["Nullable(Float64)", "Nullable(Float64)"],
            result: "Nullable(Float64)",
            state: "Tuple(Binary, Boolean, Boolean)",
        },
        Case::Metadata {
            expression: "covar_samp(x0, x1)",
            arguments: vec!["Float64", "Float64"],
            result: "Nullable(Float64)",
            state: "Tuple(Binary, Boolean)",
        },
        Case::Metadata {
            expression: "covar_samp(x0, x1)",
            arguments: vec!["Int64", "Int64"],
            result: "Nullable(Float64)",
            state: "Tuple(Binary, Boolean)",
        },
        Case::Metadata {
            expression: "covar_samp(x0, x1)",
            arguments: vec!["Nullable(Float64)", "Float64"],
            result: "Nullable(Float64)",
            state: "Tuple(Binary, Boolean, Boolean)",
        },
        Case::Metadata {
            expression: "covar_samp(x0, x1)",
            arguments: vec!["Nullable(Float64)", "Nullable(Float64)"],
            result: "Nullable(Float64)",
            state: "Tuple(Binary, Boolean, Boolean)",
        },
        Case::Samples {
            expression: "covar_pop(x0, x1)",
            arguments: vec!["Nullable(Int64)", "Nullable(Int64)"],
            result: "Nullable(Float64)",
            state: "Tuple(Binary, Boolean, Boolean)",
            samples: vec![
                Sample {
                    label: "empty",
                    inputs: vec![
                        Int64Type::from_opt_data(Vec::<Option<i64>>::new()),
                        Int64Type::from_opt_data(Vec::<Option<i64>>::new()),
                    ],
                    state: tuple(vec![
                        binary("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
                        Scalar::Boolean(false),
                        Scalar::Boolean(false),
                    ]),
                    result: Scalar::Null,
                    merge_result: MergeResult::Skip,
                },
                Sample {
                    label: "all_null",
                    inputs: vec![
                        Int64Type::from_opt_data(vec![None::<i64>; 2]),
                        Int64Type::from_opt_data(vec![None::<i64>; 2]),
                    ],
                    state: tuple(vec![
                        binary("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA="),
                        Scalar::Boolean(false),
                        Scalar::Boolean(true),
                    ]),
                    result: Scalar::Null,
                    merge_result: MergeResult::Skip,
                },
                Sample {
                    label: "mixed",
                    inputs: vec![
                        Int64Type::from_opt_data(vec![Some(2), None, Some(2), Some(5), Some(9)]),
                        Int64Type::from_opt_data(vec![Some(2), None, Some(2), Some(5), Some(9)]),
                    ],
                    state: tuple(vec![
                        binary("BAAAAAAAAAAAAAAAAIBAQAAAAAAAABJAAAAAAAAAEkA="),
                        Scalar::Boolean(true),
                        Scalar::Boolean(true),
                    ]),
                    result: float64(8.25),
                    merge_result: MergeResult::Skip,
                },
            ],
        },
    ]);
}
