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

use databend_common_exception::Result;
use databend_common_expression::Scalar;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_sql::optimizer::optimizers::operator::NormalizeDisjunctiveFilterOptimizer;
use databend_common_sql::planner::plans::FunctionCall;
use databend_common_sql::planner::plans::ScalarExpr;

use crate::sql::planner::optimizer::test_utils::*;

/// Runs the NormalizeDisjunctiveFilterOptimizer on the given predicates
fn run_optimizer(predicates: Vec<ScalarExpr>) -> Result<Vec<ScalarExpr>> {
    let optimizer = NormalizeDisjunctiveFilterOptimizer::new();
    optimizer.optimize(predicates)
}

/// Converts a ScalarExpr to a readable string representation
fn expr_to_string(expr: &ScalarExpr) -> String {
    match expr {
        ScalarExpr::ConstantExpr(c) => match &c.value {
            Scalar::Boolean(b) => format!("{}", b),
            Scalar::Number(NumberScalar::Int64(i)) => format!("{}", i),
            Scalar::String(s) => format!("\"{}\"", s),
            _ => format!("{:?}", c.value),
        },
        ScalarExpr::FunctionCall(func) => {
            if func.func_name == "column" {
                if let ScalarExpr::ConstantExpr(c) = &func.arguments[0] {
                    if let Scalar::String(s) = &c.value {
                        return format!("{}", s);
                    }
                }
                "column(?)".to_string()
            } else if func.func_name == "and" {
                if func.arguments.is_empty() {
                    "true".to_string() // Empty AND is true
                } else if func.arguments.len() == 1 {
                    expr_to_string(&func.arguments[0])
                } else {
                    format!(
                        "({} AND {})",
                        expr_to_string(&func.arguments[0]),
                        expr_to_string(&func.arguments[1])
                    )
                }
            } else if func.func_name == "or" {
                if func.arguments.is_empty() {
                    "false".to_string() // Empty OR is false
                } else if func.arguments.len() == 1 {
                    expr_to_string(&func.arguments[0])
                } else {
                    format!(
                        "({} OR {})",
                        expr_to_string(&func.arguments[0]),
                        expr_to_string(&func.arguments[1])
                    )
                }
            } else if func.func_name == "=" {
                format!(
                    "{} = {}",
                    expr_to_string(&func.arguments[0]),
                    expr_to_string(&func.arguments[1])
                )
            } else {
                format!(
                    "{}({})",
                    func.func_name,
                    func.arguments
                        .iter()
                        .map(expr_to_string)
                        .collect::<Vec<_>>()
                        .join(", ")
                )
            }
        }
        _ => format!("{:?}", expr),
    }
}

/// Helper function to print a list of expressions
fn print_exprs(exprs: &[ScalarExpr]) -> String {
    if exprs.is_empty() {
        return String::new();
    }

    if exprs.len() == 1 {
        return expr_to_string(&exprs[0]);
    }

    // For multiple expressions, create an OR function call to combine them
    let mut combined = format!("({})", expr_to_string(&exprs[0]));

    for expr in exprs.iter().skip(1) {
        combined = format!("({} OR {})", combined, expr_to_string(expr));
    }

    combined
}

/// Helper function to compare expressions before and after optimization
fn compare_exprs(
    before: &[ScalarExpr],
    after: &[ScalarExpr],
    expected_before: &str,
    expected_after: &str,
) -> Result<()> {
    let before_str = print_exprs(before);
    let after_str = print_exprs(after);

    // Check if the actual expressions match the expected patterns exactly
    assert_eq!(
        expected_before, before_str,
        "Expected before expression '{}' does not match actual result:\n{}",
        expected_before, before_str
    );

    assert_eq!(
        expected_after, after_str,
        "Expected after expression '{}' does not match actual result:\n{}",
        expected_after, after_str
    );

    // Check if before and after are equal when they should be
    if expected_before == expected_after {
        assert_eq!(
            before_str, after_str,
            "Before and after should be equal but they are not"
        );
    }

    Ok(())
}

// ===== Test Cases =====

#[test]
/// Test case for disjunctive normalization with columns from different tables
///
/// For example: (t1.a AND t2.b) OR (t1.a AND t2.c) => t1.a AND (t2.b OR t2.c)
///
/// Before optimization:
/// ```
/// (t1.a AND t2.b) OR (t1.a AND t2.c)
/// ```
///
/// After optimization:
/// ```
/// t1.a AND (t2.b OR t2.c)
/// ```
///
/// Optimization: Extracts common terms from both sides of the OR, even when columns are from different tables
fn test_different_table_columns() -> Result<()> {
    // Create a builder for expressions
    let mut builder = ExprBuilder::new();

    // Create test expressions for columns from different tables
    let t1_a = builder.column(
        "t1.a",
        1,
        "a",
        DataType::Number(NumberDataType::Int64),
        "t1",
        1,
    );

    let t2_b = builder.column(
        "t2.b",
        2,
        "b",
        DataType::Number(NumberDataType::Int64),
        "t2",
        2,
    );

    let t2_c = builder.column(
        "t2.c",
        3,
        "c",
        DataType::Number(NumberDataType::Int64),
        "t2",
        2,
    );

    // Create the expressions: (t1.a AND t2.b) OR (t1.a AND t2.c)
    let t1a_and_t2b = builder.and(t1_a.clone(), t2_b.clone());
    let t1a_and_t2c = builder.and(t1_a.clone(), t2_c.clone());
    let or_expr = builder.or(t1a_and_t2b, t1a_and_t2c);

    // Run the optimizer
    let before = vec![or_expr];
    let after = run_optimizer(before.clone())?;

    // We don't do exact string matching here because the actual output format
    // depends on the implementation details of the optimizer and might change.
    // Instead, we rely on the structural checks below to verify the correctness.

    // For bound column refs, we need to check the structure directly rather than using string patterns
    // since the debug output is verbose and contains implementation details

    // Verify that the optimizer extracted the common term (t1.a) and created an OR of t2.b and t2.c
    assert_eq!(
        after.len(),
        2,
        "Optimizer should produce exactly two expressions"
    );

    // Check if the first expression is t1.a
    if let ScalarExpr::BoundColumnRef(ref col_ref) = &after[0] {
        assert_eq!(col_ref.column.column_name, "a");
        assert_eq!(col_ref.column.table_name, Some("t1".to_string()));
    } else {
        panic!("First expression should be BoundColumnRef for t1.a");
    }

    // Check if the second expression is (t2.b OR t2.c)
    if let ScalarExpr::FunctionCall(ref func) = &after[1] {
        assert_eq!(func.func_name, "or");
        assert_eq!(func.arguments.len(), 2);

        // Check t2.b
        if let ScalarExpr::BoundColumnRef(ref col_ref) = &func.arguments[0] {
            assert_eq!(col_ref.column.column_name, "b");
            assert_eq!(col_ref.column.table_name, Some("t2".to_string()));
        } else {
            panic!("First OR argument should be BoundColumnRef for t2.b");
        }

        // Check t2.c
        if let ScalarExpr::BoundColumnRef(ref col_ref) = &func.arguments[1] {
            assert_eq!(col_ref.column.column_name, "c");
            assert_eq!(col_ref.column.table_name, Some("t2".to_string()));
        } else {
            panic!("Second OR argument should be BoundColumnRef for t2.c");
        }
    } else {
        panic!("Second expression should be an OR function call");
    }

    Ok(())
}

#[test]
/// Test case for basic disjunctive normalization
///
/// For example: (A AND B) OR (A AND C) => A AND (B OR C)
///
/// Before optimization:
/// ```
/// (A AND B) OR (A AND C)
/// ```
///
/// After optimization:
/// ```
/// A AND (B OR C)
/// ```
///
/// Optimization: Applies the inverse OR distributive law to extract common terms
fn test_basic_normalization() -> Result<()> {
    // Create a builder for expressions
    let builder = ExprBuilder::new();

    // Create test expressions
    let a = builder.column_by_name("A");
    let b = builder.column_by_name("B");
    let c = builder.column_by_name("C");

    let a_and_b = builder.and(a.clone(), b.clone());
    let a_and_c = builder.and(a.clone(), c.clone());
    let or_expr = builder.or(a_and_b, a_and_c);

    // Run the optimizer
    let before = vec![or_expr];
    let after = run_optimizer(before.clone())?;

    // Define expected before and after patterns
    let expected_before = "((A AND B) OR (A AND C))";
    let expected_after = "((A) OR (B OR C))";

    // Compare before and after optimization results
    compare_exprs(&before, &after, expected_before, expected_after)?;

    Ok(())
}

#[test]
/// Test case for nested disjunctive normalization
///
/// For example: ((A AND B) OR (A AND C)) OR (A AND D) => A AND (B OR C OR D)
///
/// Before optimization:
/// ```
/// ((A AND B) OR (A AND C)) OR (A AND D)
/// ```
///
/// After optimization:
/// ```
/// A AND (B OR C OR D)
/// ```
///
/// Optimization: Applies the inverse OR distributive law to extract common terms from nested OR expressions
fn test_nested_normalization() -> Result<()> {
    // Create a builder for expressions
    let builder = ExprBuilder::new();

    // Create test expressions
    let a = builder.column_by_name("A");
    let b = builder.column_by_name("B");
    let c = builder.column_by_name("C");
    let d = builder.column_by_name("D");

    let a_and_b = builder.and(a.clone(), b.clone());
    let a_and_c = builder.and(a.clone(), c.clone());
    let a_and_d = builder.and(a.clone(), d.clone());

    let inner_or = builder.or(a_and_b, a_and_c);
    let outer_or = builder.or(inner_or, a_and_d);

    // Run the optimizer
    let before = vec![outer_or];
    let after = run_optimizer(before.clone())?;

    // Define expected before and after patterns
    let expected_before = "(((A AND B) OR (A AND C)) OR (A AND D))";
    let expected_after = "((A) OR ((B OR C) OR D))";

    // Compare before and after optimization results
    compare_exprs(&before, &after, expected_before, expected_after)?;

    Ok(())
}

/// Test case for multiple common terms
///
/// For example: (A AND B AND C) OR (A AND B AND D) => (A AND B) AND (C OR D)
///
/// Before optimization:
/// ```
/// (A AND B AND C) OR (A AND B AND D)
/// ```
///
/// After optimization:
/// ```
/// (A AND B) AND (C OR D)
/// ```
///
/// Optimization: Extracts multiple common terms from both sides of the OR
#[test]
fn test_multiple_common_terms() -> Result<()> {
    // Create a builder for expressions
    let builder = ExprBuilder::new();

    // Create test expressions
    let a = builder.column_by_name("A");
    let b = builder.column_by_name("B");
    let c = builder.column_by_name("C");
    let d = builder.column_by_name("D");

    // Create (A AND B AND C) OR (A AND B AND D)
    let a_and_b = builder.and(a.clone(), b.clone());
    let a_and_b_and_c = builder.and(a_and_b.clone(), c.clone());
    let a_and_b_and_d = builder.and(a_and_b.clone(), d.clone());

    let or_expr = builder.or(a_and_b_and_c, a_and_b_and_d);

    // Run the optimizer
    let before = vec![or_expr];
    let after = run_optimizer(before.clone())?;

    // Define expected before and after patterns
    let expected_before = "(((A AND B) AND C) OR ((A AND B) AND D))";
    let expected_after = "(((A) OR B) OR (C OR D))";

    // Compare before and after optimization results
    compare_exprs(&before, &after, expected_before, expected_after)?;

    Ok(())
}

#[test]
/// Test case for no common terms
///
/// For example: (A AND B) OR (C AND D) => (A AND B) OR (C AND D) (no change)
///
/// Before optimization:
/// ```
/// (A AND B) OR (C AND D)
/// ```
///
/// After optimization:
/// ```
/// (A AND B) OR (C AND D)  (no change)
/// ```
///
/// Optimization: No change since there are no common terms to extract
fn test_no_common_terms() -> Result<()> {
    // Create a builder for expressions
    let builder = ExprBuilder::new();

    // Create test expressions
    let a = builder.column_by_name("A");
    let b = builder.column_by_name("B");
    let c = builder.column_by_name("C");
    let d = builder.column_by_name("D");

    let a_and_b = builder.and(a, b);
    let c_and_d = builder.and(c, d);

    let or_expr = builder.or(a_and_b, c_and_d);

    // Run the optimizer
    let before = vec![or_expr];
    let after = run_optimizer(before.clone())?;

    // Define expected before and after patterns
    let expected_before = "((A AND B) OR (C AND D))";
    let expected_after = "((A AND B) OR (C AND D))"; // Format changed but semantically the same

    // Compare before and after optimization results
    compare_exprs(&before, &after, expected_before, expected_after)?;

    Ok(())
}

#[test]
/// Test case for single term expressions
///
/// For example: A OR A => A
///
/// Before optimization:
/// ```
/// A OR A
/// ```
///
/// After optimization:
/// ```
/// A
/// ```
///
/// Optimization: Simplifies duplicate terms in OR expressions
fn test_single_term_expressions() -> Result<()> {
    // Create a builder for expressions
    let builder = ExprBuilder::new();

    // Create test expressions
    let a = builder.column_by_name("A");

    let or_expr = builder.or(a.clone(), a.clone());

    // Run the optimizer
    let before = vec![or_expr];
    let after = run_optimizer(before.clone())?;

    // Define expected before and after patterns
    let expected_before = "(A OR A)";
    let expected_after = "A"; // Simplified from A OR A

    // Compare before and after optimization results
    compare_exprs(&before, &after, expected_before, expected_after)?;

    Ok(())
}

#[test]
/// Test case for complex nested expressions
///
/// For example: ((A AND B) OR (A AND (C OR D))) => A AND (B OR (C OR D))
///
/// Before optimization:
/// ```
/// (A AND B) OR (A AND (C OR D))
/// ```
///
/// After optimization:
/// ```
/// A AND (B OR (C OR D))
/// ```
///
/// Optimization: Correctly handles nested OR expressions within AND expressions
fn test_complex_nested_expressions() -> Result<()> {
    // Create a builder for expressions
    let builder = ExprBuilder::new();

    // Create test expressions
    let a = builder.column_by_name("A");
    let b = builder.column_by_name("B");
    let c = builder.column_by_name("C");
    let d = builder.column_by_name("D");

    let c_or_d = builder.or(c, d);
    let a_and_b = builder.and(a.clone(), b);
    let a_and_c_or_d = builder.and(a.clone(), c_or_d);

    let or_expr = builder.or(a_and_b, a_and_c_or_d);

    // Run the optimizer
    let before = vec![or_expr];
    let after = run_optimizer(before.clone())?;

    // Define expected before and after patterns
    let expected_before = "((A AND B) OR (A AND (C OR D)))";
    let expected_after = "((A) OR (B OR (C OR D)))";

    // Compare before and after optimization results
    compare_exprs(&before, &after, expected_before, expected_after)?;

    Ok(())
}

#[test]
/// Test case for edge case with empty OR arguments
///
/// For example: () OR () => false
///
/// Before optimization:
/// ```
/// () OR ()
/// ```
///
/// After optimization:
/// ```
/// false
/// ```
///
/// Optimization: Handles empty OR arguments by returning false
/// Note: This is a potential bug as empty OR should evaluate to false
fn test_empty_or_arguments() -> Result<()> {
    // Create a builder for expressions
    let _builder = ExprBuilder::new();

    // Create an OR expression with empty arguments
    // This is not a valid expression in normal SQL but testing edge case handling
    // Since ExprBuilder doesn't directly support creating an empty OR function call,
    // we'll create it manually
    let or_expr = ScalarExpr::FunctionCall(FunctionCall {
        span: None,
        func_name: "or".to_string(),
        params: vec![],
        arguments: vec![],
    });

    // Run the optimizer
    let before = vec![or_expr];
    let after = run_optimizer(before.clone())?;

    // Define expected before and after patterns
    let expected_before = "false"; // Empty OR function call
    let expected_after = "false"; // Empty OR function call

    // Compare before and after optimization results
    // This might be a bug in the implementation if it doesn't handle this case
    compare_exprs(&before, &after, expected_before, expected_after)?;

    Ok(())
}

#[test]
/// Test case for edge case with a single OR argument
///
/// For example: OR(A) => A
///
/// Before optimization:
/// ```
/// OR(A)
/// ```
///
/// After optimization:
/// ```
/// A
/// ```
///
/// Optimization: Simplifies OR with a single argument to just that argument
fn test_single_or_argument() -> Result<()> {
    // Create a builder for expressions
    let builder = ExprBuilder::new();

    // Create an OR expression with a single argument
    let a = builder.column_by_name("A");

    // Since ExprBuilder doesn't directly support creating an OR function call with a single argument,
    // we'll create it manually
    let or_expr = ScalarExpr::FunctionCall(FunctionCall {
        span: None,
        func_name: "or".to_string(),
        params: vec![],
        arguments: vec![a],
    });

    // Run the optimizer
    let before = vec![or_expr];
    let after = run_optimizer(before.clone())?;

    // Define expected before and after patterns
    let expected_before = "A"; // Single argument OR function call
    let expected_after = "A"; // Single argument OR function call

    // Compare before and after optimization results
    compare_exprs(&before, &after, expected_before, expected_after)?;

    Ok(())
}

#[test]
/// Test case for mixed AND/OR expressions with no common terms
///
/// For example: (A OR B) OR (C AND D) => (A OR B) OR (C AND D) (no change)
///
/// Before optimization:
/// ```
/// (A OR B) OR (C AND D)
/// ```
///
/// After optimization:
/// ```
/// (A OR B) OR (C AND D)  (no change)
/// ```
///
/// Optimization: No change since there are no common AND terms to extract
fn test_mixed_and_or_no_common() -> Result<()> {
    // Create a builder for expressions
    let builder = ExprBuilder::new();

    // Create test expressions
    let a = builder.column_by_name("A");
    let b = builder.column_by_name("B");
    let c = builder.column_by_name("C");
    let d = builder.column_by_name("D");

    let a_or_b = builder.or(a, b);
    let c_and_d = builder.and(c, d);

    let or_expr = builder.or(a_or_b, c_and_d);

    // Run the optimizer
    let before = vec![or_expr];
    let after = run_optimizer(before.clone())?;

    // Define expected before and after patterns
    let expected_before = "((A OR B) OR (C AND D))";
    let expected_after = "((A OR B) OR (C AND D))";

    // Compare before and after optimization results
    compare_exprs(&before, &after, expected_before, expected_after)?;

    Ok(())
}

#[test]
/// Test case for potential bug with non-commutative expressions
///
/// For example: (A > B AND C) OR (C AND A > B) => (A > B AND C)
///
/// Before optimization:
/// ```
/// (A > B AND C) OR (C AND A > B)
/// ```
///
/// After optimization:
/// ```
/// (A > B AND C)
/// ```
///
/// Optimization: Should recognize that these expressions are equivalent despite different order
/// Potential bug: The optimizer might not recognize that (A > B AND C) is the same as (C AND A > B)
fn test_non_commutative_expressions() -> Result<()> {
    // Create a builder for expressions
    let builder = ExprBuilder::new();

    // Create test expressions
    let a = builder.column_by_name("A");
    let b = builder.column_by_name("B");
    let c = builder.column_by_name("C");

    // Create A > B using the ExprBuilder
    let a_gt_b = builder.gt(a.clone(), b.clone());

    let a_gt_b_and_c = builder.and(a_gt_b.clone(), c.clone());
    let c_and_a_gt_b = builder.and(c.clone(), a_gt_b.clone());

    let or_expr = builder.or(a_gt_b_and_c, c_and_a_gt_b);

    // Run the optimizer
    let before = vec![or_expr];
    let after = run_optimizer(before.clone())?;

    // Define expected before and after patterns
    let expected_before = "((gt(A, B) AND C) OR (C AND gt(A, B)))";
    let expected_after = "((gt(A, B)) OR C)"; // Format changed but semantically the same

    // Compare before and after optimization results
    // This is a potential bug - the optimizer should recognize these as the same expression
    // and simplify to just one of them
    compare_exprs(&before, &after, expected_before, expected_after)?;

    Ok(())
}

#[test]
/// Test case for optimization with constants
///
/// For example: (A AND true) OR (A AND false) => A
///
/// Before optimization:
/// ```
/// (A AND true) OR (A AND false)
/// ```
///
/// After optimization:
/// ```
/// A
/// ```
///
/// Optimization: Simplifies expressions with boolean constants
fn test_optimization_with_constants() -> Result<()> {
    // Create a builder for expressions
    let builder = ExprBuilder::new();

    // Create test expressions
    let a = builder.column_by_name("A");
    let true_const = builder.bool(true);
    let false_const = builder.bool(false);

    let a_and_true = builder.and(a.clone(), true_const);
    let a_and_false = builder.and(a.clone(), false_const);

    let or_expr = builder.or(a_and_true, a_and_false);

    // Run the optimizer
    let before = vec![or_expr];
    let after = run_optimizer(before.clone())?;

    // Define expected before and after patterns
    let expected_before = "((A AND true) OR (A AND false))";
    let expected_after = "((A) OR (true OR false))"; // Format changed but semantically the same

    // Compare before and after optimization results
    // This might be a bug if the optimizer doesn't handle boolean constants properly
    compare_exprs(&before, &after, expected_before, expected_after)?;

    Ok(())
}

#[test]
/// Test case for multiple OR expressions in a single predicate
///
/// For example: (A AND B) OR (A AND C) OR (A AND D) => A AND (B OR C OR D)
///
/// Before optimization:
/// ```
/// (A AND B) OR (A AND C) OR (A AND D)
/// ```
///
/// After optimization:
/// ```
/// A AND (B OR C OR D)
/// ```
///
/// Optimization: Extracts common terms from multiple OR expressions
fn test_multiple_or_expressions() -> Result<()> {
    // Create a builder for expressions
    let builder = ExprBuilder::new();

    // Create test expressions
    let a = builder.column_by_name("A");
    let b = builder.column_by_name("B");
    let c = builder.column_by_name("C");
    let d = builder.column_by_name("D");

    let a_and_b = builder.and(a.clone(), b);
    let a_and_c = builder.and(a.clone(), c);
    let a_and_d = builder.and(a.clone(), d);

    let or_1 = builder.or(a_and_b, a_and_c);
    let or_2 = builder.or(or_1, a_and_d);

    // Run the optimizer
    let before = vec![or_2];
    let after = run_optimizer(before.clone())?;

    // Define expected before and after patterns
    let expected_before = "(((A AND B) OR (A AND C)) OR (A AND D))";
    let expected_after = "((A) OR ((B OR C) OR D))";

    // Compare before and after optimization results
    compare_exprs(&before, &after, expected_before, expected_after)?;

    Ok(())
}

//=====

#[test]
/// Test boundary case in normalize_predicate_scalar
///
/// Potential issue: Function assumes args.len() >= 2 but doesn't explicitly verify
fn test_normalize_predicate_scalar_boundary() -> Result<()> {
    // Create a builder for expressions
    let builder = ExprBuilder::new();

    // Create an AND expression with only one argument
    let a = builder.column_by_name("A");

    // Since ExprBuilder doesn't directly support creating an AND function call with a single argument,
    // we'll create it manually
    let and_expr = ScalarExpr::FunctionCall(FunctionCall {
        span: None,
        func_name: "and".to_string(),
        params: vec![],
        arguments: vec![a.clone()],
    });

    // Run the optimizer
    let before = vec![and_expr];
    let after = run_optimizer(before.clone())?;

    // Expected result: Single argument AND should simplify to that argument
    let expected_before = "A";
    let expected_after = "A";

    // Compare before and after optimization results
    compare_exprs(&before, &after, expected_before, expected_after)?;

    Ok(())
}

#[test]
/// Test logical error in handling non-AND/OR expressions
///
/// Potential issue: In process_duplicate_or_exprs, when comparing shortest_exprs_len,
/// non-AND expressions are not considered
fn test_non_and_or_expressions() -> Result<()> {
    // Create a builder for expressions
    let builder = ExprBuilder::new();

    // Create test expressions
    let a = builder.column_by_name("A");
    let b = builder.column_by_name("B");
    let c = builder.column_by_name("C");

    // Create a non-AND/OR function call using eq from ExprBuilder
    let a_eq_b = builder.eq(a.clone(), b.clone());

    // Create (A = B) OR (A AND C)
    let a_and_c = builder.and(a.clone(), c);
    let or_expr = builder.or(a_eq_b, a_and_c);

    // Run the optimizer
    let before = vec![or_expr];
    let after = run_optimizer(before.clone())?;

    // Expected result: Should not extract common terms since A=B is not an AND expression
    let expected_before = "(eq(A, B) OR (A AND C))";
    let expected_after = "(eq(A, B) OR (A AND C))";

    // Compare before and after optimization results
    compare_exprs(&before, &after, expected_before, expected_after)?;

    Ok(())
}

#[test]
/// Test complex nested expressions that might lead to incorrect optimization
///
/// Potential issue: Complex nested expressions may cause inconsistent optimizer behavior
fn test_complex_nested_optimization() -> Result<()> {
    // Create a builder for expressions
    let builder = ExprBuilder::new();

    // Create test expressions
    let a = builder.column_by_name("A");
    let b = builder.column_by_name("B");
    let c = builder.column_by_name("C");
    let d = builder.column_by_name("D");
    let e = builder.column_by_name("E");

    // Create ((A AND B) OR (C AND D)) AND E
    let a_and_b = builder.and(a, b);
    let c_and_d = builder.and(c, d);
    let or_expr = builder.or(a_and_b, c_and_d);
    let and_expr = builder.and(or_expr, e);

    // Run the optimizer
    let before = vec![and_expr];
    let after = run_optimizer(before.clone())?;

    // Expected result: Optimizer should maintain the AND expression structure
    let expected_before = "(((A AND B) OR (C AND D)) AND E)";
    let expected_after = "((((A AND B) OR (C AND D))) OR E)";

    // Compare before and after optimization results
    compare_exprs(&before, &after, expected_before, expected_after)?;

    Ok(())
}

#[test]
/// Test handling of duplicate expressions
///
/// Potential issue: Optimizer may not correctly handle duplicate expressions
fn test_duplicate_expressions() -> Result<()> {
    // Create a builder for expressions
    let builder = ExprBuilder::new();

    // Create test expressions
    let a = builder.column_by_name("A");
    let b = builder.column_by_name("B");

    // Create (A AND B) OR (A AND B), should simplify to (A AND B)
    let a_and_b = builder.and(a.clone(), b.clone());
    let or_expr = builder.or(a_and_b.clone(), a_and_b.clone());

    // Run the optimizer
    let before = vec![or_expr];
    let after = run_optimizer(before.clone())?;

    // Expected result: Should simplify to a single (A AND B)
    let expected_before = "((A AND B) OR (A AND B))";
    let expected_after = "((A) OR B)";

    // Compare before and after optimization results
    compare_exprs(&before, &after, expected_before, expected_after)?;

    Ok(())
}

#[test]
/// Test special case: (A AND B) OR A => A
///
/// Potential issue: Optimizer may not correctly handle this special case
fn test_special_case_and_or() -> Result<()> {
    // Create a builder for expressions
    let builder = ExprBuilder::new();

    // Create test expressions
    let a = builder.column_by_name("A");
    let b = builder.column_by_name("B");

    // Create (A AND B) OR A, should simplify to A
    let a_and_b = builder.and(a.clone(), b);
    let or_expr = builder.or(a_and_b, a.clone());

    // Run the optimizer
    let before = vec![or_expr];
    let after = run_optimizer(before.clone())?;

    // Expected result: Should simplify to A
    let expected_before = "((A AND B) OR A)";
    let expected_after = "A";

    // Compare before and after optimization results
    compare_exprs(&before, &after, expected_before, expected_after)?;

    Ok(())
}

#[test]
/// Test handling of multiple predicates
///
/// Potential issue: Optimizer may not correctly handle multiple predicates
fn test_multiple_predicates() -> Result<()> {
    // Create a builder for expressions
    let builder = ExprBuilder::new();

    // Create test expressions
    let a = builder.column_by_name("A");
    let b = builder.column_by_name("B");
    let c = builder.column_by_name("C");
    let d = builder.column_by_name("D");

    // Create two predicates: (A AND B) OR (A AND C) and (B AND D) OR (C AND D)
    let a_and_b = builder.and(a.clone(), b.clone());
    let a_and_c = builder.and(a.clone(), c.clone());
    let or_expr1 = builder.or(a_and_b, a_and_c);

    let b_and_d = builder.and(b.clone(), d.clone());
    let c_and_d = builder.and(c.clone(), d.clone());
    let or_expr2 = builder.or(b_and_d, c_and_d);

    // Run the optimizer
    let before = vec![or_expr1, or_expr2];
    let after = run_optimizer(before.clone())?;

    // Expected result: Should optimize each predicate separately
    // First predicate: A AND (B OR C)
    // Second predicate: D AND (B OR C)
    assert_eq!(
        after.len(),
        4,
        "Should produce 4 expressions after optimization"
    );

    Ok(())
}

#[test]
/// Test extreme case: very deeply nested expressions
///
/// Potential issue: Deep nesting may cause stack overflow
fn test_deeply_nested_expressions() -> Result<()> {
    // Create a builder for expressions
    let builder = ExprBuilder::new();

    // Create a deeply nested expression
    let mut expr = builder.column_by_name("A");

    // Create a nested AND expression with depth 1000
    for i in 0..1000 {
        let new_col = builder.column_by_name(&format!("Col{}", i));
        expr = builder.and(expr, new_col);
    }

    // Create OR expression
    let or_expr = builder.or(expr.clone(), expr.clone());

    // Run the optimizer
    let before = vec![or_expr];
    let result = run_optimizer(before.clone());

    // Check if stack overflow occurred
    assert!(
        result.is_ok(),
        "Optimizer panicked with deeply nested expressions, possibly stack overflow"
    );

    Ok(())
}

#[test]
/// Test (A OR B) AND (A OR C) case
///
/// Potential issue: Optimizer may not correctly handle this distributive law case
fn test_distributive_law() -> Result<()> {
    // Create a builder for expressions
    let builder = ExprBuilder::new();

    // Create test expressions
    let a = builder.column_by_name("A");
    let b = builder.column_by_name("B");
    let c = builder.column_by_name("C");

    // Create (A OR B) AND (A OR C)
    let a_or_b = builder.or(a.clone(), b);
    let a_or_c = builder.or(a.clone(), c);
    let and_expr = builder.and(a_or_b, a_or_c);

    // Run the optimizer
    let before = vec![and_expr];
    let after = run_optimizer(before.clone())?;

    // Expected result: Optimizer should not change this expression as it focuses on inverse OR distributive law
    let expected_before = "((A OR B) AND (A OR C))";
    let expected_after = "(((A OR B)) OR (A OR C))";

    // Compare before and after optimization results
    compare_exprs(&before, &after, expected_before, expected_after)?;

    Ok(())
}
