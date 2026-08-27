use databend_common_exception::Result;
use databend_common_expression::FromData;
use databend_common_expression::types::UInt64Type;

use super::aggregate_function_v2_support::assert_single_float_close;
use super::aggregate_function_v2_support::eval_v2_aggr;

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
