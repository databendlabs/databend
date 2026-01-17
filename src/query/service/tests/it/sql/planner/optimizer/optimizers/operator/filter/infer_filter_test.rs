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
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_sql::optimizer::optimizers::operator::InferFilterOptimizer;
use databend_common_sql::planner::plans::ScalarExpr;

use crate::sql::planner::optimizer::test_utils::ExprBuilder;
use crate::sql::planner::optimizer::test_utils::*;

/// Runs the optimizer with the given predicates and returns the result
fn run_optimizer(predicates: Vec<ScalarExpr>) -> Result<Vec<ScalarExpr>> {
    let mut optimizer = InferFilterOptimizer::new(None);
    optimizer.optimize(predicates)
}

#[test]
fn test_equal_operator_combinations() -> anyhow::Result<()> {
    // Create a builder for expressions
    let mut builder = ExprBuilder::new();

    // Setup common columns and constants
    let col_a = builder.column("A", 0, "A", DataType::Number(NumberDataType::Int64), "", 0);
    let const_3 = builder.int(3);
    let const_5 = builder.int(5);
    let const_7 = builder.int(7);

    // Test: A = 5 AND A = 5 => A = 5
    {
        let pred_a_eq_5 = builder.eq(col_a.clone(), const_5.clone());
        let pred_a_eq_5_dup = builder.eq(col_a.clone(), const_5.clone());

        let result = run_optimizer(vec![pred_a_eq_5, pred_a_eq_5_dup])?;

        assert_eq!(result.len(), 1, "Should simplify to one predicate");
        assert!(
            builder.find_predicate(&result, "eq", 0, Some(5)),
            "Should keep A = 5 predicate"
        );
    }

    // Test: A = 3 AND A = 5 => false (contradiction)
    {
        let pred_a_eq_3 = builder.eq(col_a.clone(), const_3.clone());
        let pred_a_eq_5 = builder.eq(col_a.clone(), const_5.clone());

        let result = run_optimizer(vec![pred_a_eq_3, pred_a_eq_5])?;

        assert!(
            builder.is_boolean_constant(&result, false),
            "Should detect contradiction and return false"
        );
    }

    // Test: A = 5 AND A != 5 => false (contradiction)
    {
        let pred_a_eq_5 = builder.eq(col_a.clone(), const_5.clone());
        let pred_a_ne_5 = builder.neq(col_a.clone(), const_5.clone());

        let result = run_optimizer(vec![pred_a_eq_5, pred_a_ne_5])?;

        assert!(
            builder.is_boolean_constant(&result, false),
            "Should detect contradiction and return false"
        );
    }

    // Test: A = 5 AND A != 3 => A = 5
    {
        let pred_a_eq_5 = builder.eq(col_a.clone(), const_5.clone());
        let pred_a_ne_3 = builder.neq(col_a.clone(), const_3.clone());

        let result = run_optimizer(vec![pred_a_eq_5, pred_a_ne_3])?;

        assert_eq!(result.len(), 1, "Should simplify to one predicate");
        assert!(
            builder.find_predicate(&result, "eq", 0, Some(5)),
            "Should keep A = 5 predicate"
        );
    }

    // Test: A = 5 AND A < 7 => A = 5
    {
        let pred_a_eq_5 = builder.eq(col_a.clone(), const_5.clone());
        let pred_a_lt_7 = builder.lt(col_a.clone(), const_7.clone());

        let result = run_optimizer(vec![pred_a_eq_5, pred_a_lt_7])?;

        assert_eq!(result.len(), 1, "Should simplify to one predicate");
        assert!(
            builder.find_predicate(&result, "eq", 0, Some(5)),
            "Should keep A = 5 predicate"
        );
    }

    // Test: A = 5 AND A < 5 => false (contradiction)
    {
        let pred_a_eq_5 = builder.eq(col_a.clone(), const_5.clone());
        let pred_a_lt_5 = builder.lt(col_a.clone(), const_5.clone());

        let result = run_optimizer(vec![pred_a_eq_5, pred_a_lt_5])?;

        assert!(
            builder.is_boolean_constant(&result, false),
            "Should detect contradiction and return false"
        );
    }

    // Test: A = 5 AND A <= 7 => A = 5
    {
        let pred_a_eq_5 = builder.eq(col_a.clone(), const_5.clone());
        let pred_a_lte_7 = builder.lte(col_a.clone(), const_7.clone());

        let result = run_optimizer(vec![pred_a_eq_5, pred_a_lte_7])?;

        assert_eq!(result.len(), 1, "Should simplify to one predicate");
        assert!(
            builder.find_predicate(&result, "eq", 0, Some(5)),
            "Should keep A = 5 predicate"
        );
    }

    // Test: A = 5 AND A <= 5 => A = 5
    {
        let pred_a_eq_5 = builder.eq(col_a.clone(), const_5.clone());
        let pred_a_lte_5 = builder.lte(col_a.clone(), const_5.clone());

        let result = run_optimizer(vec![pred_a_eq_5, pred_a_lte_5])?;

        assert_eq!(result.len(), 1, "Should simplify to one predicate");
        assert!(
            builder.find_predicate(&result, "eq", 0, Some(5)),
            "Should keep A = 5 predicate"
        );
    }

    // Test: A = 5 AND A > 3 => A = 5
    {
        let pred_a_eq_5 = builder.eq(col_a.clone(), const_5.clone());
        let pred_a_gt_3 = builder.gt(col_a.clone(), const_3.clone());

        let result = run_optimizer(vec![pred_a_eq_5, pred_a_gt_3])?;

        assert_eq!(result.len(), 1, "Should simplify to one predicate");
        assert!(
            builder.find_predicate(&result, "eq", 0, Some(5)),
            "Should keep A = 5 predicate"
        );
    }

    // Test: A = 5 AND A > 5 => false (contradiction)
    {
        let pred_a_eq_5 = builder.eq(col_a.clone(), const_5.clone());
        let pred_a_gt_5 = builder.gt(col_a.clone(), const_5.clone());

        let result = run_optimizer(vec![pred_a_eq_5, pred_a_gt_5])?;

        assert!(
            builder.is_boolean_constant(&result, false),
            "Should detect contradiction and return false"
        );
    }

    // Test: A = 5 AND A >= 3 => A = 5
    {
        let pred_a_eq_5 = builder.eq(col_a.clone(), const_5.clone());
        let pred_a_gte_3 = builder.gte(col_a.clone(), const_3.clone());

        let result = run_optimizer(vec![pred_a_eq_5, pred_a_gte_3])?;

        assert_eq!(result.len(), 1, "Should simplify to one predicate");
        assert!(
            builder.find_predicate(&result, "eq", 0, Some(5)),
            "Should keep A = 5 predicate"
        );
    }

    // Test: A = 5 AND A >= 5 => A = 5
    {
        let pred_a_eq_5 = builder.eq(col_a.clone(), const_5.clone());
        let pred_a_gte_5 = builder.gte(col_a.clone(), const_5.clone());

        let result = run_optimizer(vec![pred_a_eq_5, pred_a_gte_5])?;

        assert_eq!(result.len(), 1, "Should simplify to one predicate");
        assert!(
            builder.find_predicate(&result, "eq", 0, Some(5)),
            "Should keep A = 5 predicate"
        );
    }

    Ok(())
}

// ===== Test Cases for NotEqual Operator =====

#[test]
fn test_not_equal_operator_combinations() -> anyhow::Result<()> {
    // Create a builder for expressions
    let mut builder = ExprBuilder::new();

    // Setup common columns and constants
    let col_a = builder.column("A", 0, "A", DataType::Number(NumberDataType::Int64), "", 0);
    let const_3 = builder.int(3);
    let const_5 = builder.int(5);
    let const_7 = builder.int(7);

    // Test: A != 5 AND A != 5 => A != 5
    {
        let pred_a_ne_5 = builder.neq(col_a.clone(), const_5.clone());
        let pred_a_ne_5_dup = builder.neq(col_a.clone(), const_5.clone());

        let result = run_optimizer(vec![pred_a_ne_5, pred_a_ne_5_dup])?;

        assert_eq!(result.len(), 1, "Should simplify to one predicate");
        assert!(
            builder.find_predicate(&result, "noteq", 0, Some(5)),
            "Should keep A != 5 predicate"
        );
    }

    // Test: A != 3 AND A != 5 => keep both (different values)
    {
        let pred_a_ne_3 = builder.neq(col_a.clone(), const_3.clone());
        let pred_a_ne_5 = builder.neq(col_a.clone(), const_5.clone());

        let result = run_optimizer(vec![pred_a_ne_3, pred_a_ne_5])?;

        assert_eq!(result.len(), 2, "Should keep both not-equal predicates");
    }

    // Test: A != 5 AND A = 5 => false (contradiction)
    {
        let pred_a_ne_5 = builder.neq(col_a.clone(), const_5.clone());
        let pred_a_eq_5 = builder.eq(col_a.clone(), const_5.clone());

        let result = run_optimizer(vec![pred_a_ne_5, pred_a_eq_5])?;

        assert!(
            builder.is_boolean_constant(&result, false),
            "Should detect contradiction and return false"
        );
    }

    // Test: A != 5 AND A = 3 => A = 3
    {
        let pred_a_ne_5 = builder.neq(col_a.clone(), const_5.clone());
        let pred_a_eq_3 = builder.eq(col_a.clone(), const_3.clone());

        let result = run_optimizer(vec![pred_a_ne_5, pred_a_eq_3])?;

        assert_eq!(result.len(), 1, "Should simplify to one predicate");
        assert!(
            builder.find_predicate(&result, "eq", 0, Some(3)),
            "Should keep A = 3 predicate"
        );
    }

    // Test: A != 5 AND A < 5 => A < 5
    {
        let pred_a_ne_5 = builder.neq(col_a.clone(), const_5.clone());
        let pred_a_lt_5 = builder.lt(col_a.clone(), const_5.clone());

        let result = run_optimizer(vec![pred_a_ne_5, pred_a_lt_5])?;

        assert_eq!(result.len(), 1, "Should simplify to one predicate");
        assert!(
            builder.find_predicate(&result, "lt", 0, Some(5)),
            "Should simplify to A < 5"
        );
    }

    // Test: A != 5 AND A < 7 => keep both
    {
        let pred_a_ne_5 = builder.neq(col_a.clone(), const_5.clone());
        let pred_a_lt_7 = builder.lt(col_a.clone(), const_7.clone());

        let result = run_optimizer(vec![pred_a_ne_5, pred_a_lt_7])?;

        assert_eq!(result.len(), 2, "Should keep both predicates");
    }

    // Test: A != 7 AND A < 5 => A < 5
    {
        let pred_a_ne_7 = builder.neq(col_a.clone(), const_7.clone());
        let pred_a_lt_5 = builder.lt(col_a.clone(), const_5.clone());

        let result = run_optimizer(vec![pred_a_ne_7, pred_a_lt_5])?;

        assert_eq!(result.len(), 1, "Should simplify to one predicate");
        assert!(
            builder.find_predicate(&result, "lt", 0, Some(5)),
            "Should keep A < 5 predicate"
        );
    }

    // Test: A != 5 AND A <= 5 => A < 5
    {
        let pred_a_ne_5 = builder.neq(col_a.clone(), const_5.clone());
        let pred_a_lte_5 = builder.lte(col_a.clone(), const_5.clone());

        let result = run_optimizer(vec![pred_a_ne_5, pred_a_lte_5])?;

        assert_eq!(result.len(), 1, "Should simplify to one predicate");
        assert!(
            builder.find_predicate(&result, "lt", 0, Some(5)),
            "Should simplify to A < 5"
        );
    }

    // Test: A != 5 AND A <= 3 => A <= 3
    {
        let pred_a_ne_5 = builder.neq(col_a.clone(), const_5.clone());
        let pred_a_lte_3 = builder.lte(col_a.clone(), const_3.clone());

        let result = run_optimizer(vec![pred_a_ne_5, pred_a_lte_3])?;

        assert_eq!(result.len(), 1, "Should simplify to one predicate");
        assert!(
            builder.find_predicate(&result, "lte", 0, Some(3)),
            "Should keep A <= 3 predicate"
        );
    }

    // Test: A != 5 AND A > 5 => A > 5
    {
        let pred_a_ne_5 = builder.neq(col_a.clone(), const_5.clone());
        let pred_a_gt_5 = builder.gt(col_a.clone(), const_5.clone());

        let result = run_optimizer(vec![pred_a_ne_5, pred_a_gt_5])?;

        assert_eq!(result.len(), 1, "Should simplify to one predicate");
        assert!(
            builder.find_predicate(&result, "gt", 0, Some(5)),
            "Should simplify to A > 5"
        );
    }

    // Test: A != 5 AND A > 3 => keep both
    {
        let pred_a_ne_5 = builder.neq(col_a.clone(), const_5.clone());
        let pred_a_gt_3 = builder.gt(col_a.clone(), const_3.clone());

        let result = run_optimizer(vec![pred_a_ne_5, pred_a_gt_3])?;

        assert_eq!(result.len(), 2, "Should keep both predicates");
    }

    // Test: A != 3 AND A > 5 => A > 5
    {
        let pred_a_ne_3 = builder.neq(col_a.clone(), const_3.clone());
        let pred_a_gt_5 = builder.gt(col_a.clone(), const_5.clone());

        let result = run_optimizer(vec![pred_a_ne_3, pred_a_gt_5])?;

        assert_eq!(result.len(), 1, "Should simplify to one predicate");
        assert!(
            builder.find_predicate(&result, "gt", 0, Some(5)),
            "Should keep A > 5 predicate"
        );
    }

    // Test: A != 5 AND A >= 5 => A > 5
    {
        let pred_a_ne_5 = builder.neq(col_a.clone(), const_5.clone());
        let pred_a_gte_5 = builder.gte(col_a.clone(), const_5.clone());

        let result = run_optimizer(vec![pred_a_ne_5, pred_a_gte_5])?;

        assert_eq!(result.len(), 1, "Should simplify to one predicate");
        assert!(
            builder.find_predicate(&result, "gt", 0, Some(5)),
            "Should simplify to A > 5"
        );
    }

    // Test: A != 5 AND A >= 7 => A >= 7
    {
        let pred_a_ne_5 = builder.neq(col_a.clone(), const_5.clone());
        let pred_a_gte_7 = builder.gte(col_a.clone(), const_7.clone());

        let result = run_optimizer(vec![pred_a_ne_5, pred_a_gte_7])?;

        assert_eq!(result.len(), 1, "Should simplify to one predicate");
        assert!(
            builder.find_predicate(&result, "gte", 0, Some(7)),
            "Should keep A >= 7 predicate"
        );
    }

    // Test: A < 5 AND A > 5 => false (contradiction)
    {
        let pred_a_lt_5 = builder.lt(col_a.clone(), const_5.clone());
        let pred_a_gt_5 = builder.gt(col_a.clone(), const_5.clone());

        let result = run_optimizer(vec![pred_a_lt_5, pred_a_gt_5])?;

        assert!(
            builder.is_boolean_constant(&result, false),
            "Should detect contradiction and return false"
        );
    }

    // Test: A < 3 AND A > 5 => false (contradiction)
    {
        let pred_a_lt_3 = builder.lt(col_a.clone(), const_3.clone());
        let pred_a_gt_5 = builder.gt(col_a.clone(), const_5.clone());

        let result = run_optimizer(vec![pred_a_lt_3, pred_a_gt_5])?;

        assert!(
            builder.is_boolean_constant(&result, false),
            "Should detect contradiction and return false"
        );
    }

    // Test: A < 5 AND A >= 3 => 3 <= A < 5
    {
        let pred_a_lt_5 = builder.lt(col_a.clone(), const_5.clone());
        let pred_a_gte_3 = builder.gte(col_a.clone(), const_3.clone());

        let result = run_optimizer(vec![pred_a_lt_5, pred_a_gte_3])?;

        assert_eq!(result.len(), 2, "Should keep both predicates");
        assert!(
            builder.find_predicate(&result, "lt", 0, Some(5)),
            "Should keep A < 5 predicate"
        );
        assert!(
            builder.find_predicate(&result, "gte", 0, Some(3)),
            "Should keep A >= 3 predicate"
        );
    }

    Ok(())
}

#[test]
fn test_gt_gte_operator_combinations() -> anyhow::Result<()> {
    // Create a builder for expressions
    let mut builder = ExprBuilder::new();

    // Setup common columns and constants
    let col_a = builder.column("A", 0, "A", DataType::Number(NumberDataType::Int64), "", 0);
    let const_3 = builder.int(3);
    let const_5 = builder.int(5);
    let const_7 = builder.int(7);

    // Test: A > 5 AND A > 3 => A > 5
    {
        let pred_a_gt_5 = builder.gt(col_a.clone(), const_5.clone());
        let pred_a_gt_3 = builder.gt(col_a.clone(), const_3.clone());

        let result = run_optimizer(vec![pred_a_gt_5, pred_a_gt_3])?;

        assert_eq!(result.len(), 1, "Should simplify to one predicate");
        assert!(
            builder.find_predicate(&result, "gt", 0, Some(5)),
            "Should simplify to A > 5"
        );
    }

    // Test: A > 3 AND A > 5 => A > 5
    {
        let pred_a_gt_3 = builder.gt(col_a.clone(), const_3.clone());
        let pred_a_gt_5 = builder.gt(col_a.clone(), const_5.clone());

        let result = run_optimizer(vec![pred_a_gt_3, pred_a_gt_5])?;

        assert_eq!(result.len(), 1, "Should simplify to one predicate");
        assert!(
            builder.find_predicate(&result, "gt", 0, Some(5)),
            "Should simplify to A > 5"
        );
    }

    // Test: A > 5 AND A >= 3 => A > 5
    {
        let pred_a_gt_5 = builder.gt(col_a.clone(), const_5.clone());
        let pred_a_gte_3 = builder.gte(col_a.clone(), const_3.clone());

        let result = run_optimizer(vec![pred_a_gt_5, pred_a_gte_3])?;

        assert_eq!(result.len(), 1, "Should simplify to one predicate");
        assert!(
            builder.find_predicate(&result, "gt", 0, Some(5)),
            "Should simplify to A > 5"
        );
    }

    // Test: A > 3 AND A >= 5 => A >= 5
    {
        let pred_a_gt_3 = builder.gt(col_a.clone(), const_3.clone());
        let pred_a_gte_5 = builder.gte(col_a.clone(), const_5.clone());

        let result = run_optimizer(vec![pred_a_gt_3, pred_a_gte_5])?;

        assert_eq!(result.len(), 1, "Should simplify to one predicate");
        assert!(
            builder.find_predicate(&result, "gte", 0, Some(5)),
            "Should simplify to A >= 5"
        );
    }

    // Test: A >= 5 AND A >= 3 => A >= 5
    {
        let pred_a_gte_5 = builder.gte(col_a.clone(), const_5.clone());
        let pred_a_gte_3 = builder.gte(col_a.clone(), const_3.clone());

        let result = run_optimizer(vec![pred_a_gte_5, pred_a_gte_3])?;

        assert_eq!(result.len(), 1, "Should simplify to one predicate");
        assert!(
            builder.find_predicate(&result, "gte", 0, Some(5)),
            "Should simplify to A >= 5"
        );
    }

    // Test: A >= 3 AND A >= 5 => A >= 5
    {
        let pred_a_gte_3 = builder.gte(col_a.clone(), const_3.clone());
        let pred_a_gte_5 = builder.gte(col_a.clone(), const_5.clone());

        let result = run_optimizer(vec![pred_a_gte_3, pred_a_gte_5])?;

        assert_eq!(result.len(), 1, "Should simplify to one predicate");
        assert!(
            builder.find_predicate(&result, "gte", 0, Some(5)),
            "Should simplify to A >= 5"
        );
    }

    // Test: A > 5 AND A > 5 => A > 5
    {
        let pred_a_gt_5 = builder.gt(col_a.clone(), const_5.clone());
        let pred_a_gt_5_dup = builder.gt(col_a.clone(), const_5.clone());

        let result = run_optimizer(vec![pred_a_gt_5, pred_a_gt_5_dup])?;

        assert_eq!(result.len(), 1, "Should simplify to one predicate");
        assert!(
            builder.find_predicate(&result, "gt", 0, Some(5)),
            "Should keep A > 5 predicate"
        );
    }

    // Test: A >= 5 AND A >= 5 => A >= 5
    {
        let pred_a_gte_5 = builder.gte(col_a.clone(), const_5.clone());
        let pred_a_gte_5_dup = builder.gte(col_a.clone(), const_5.clone());

        let result = run_optimizer(vec![pred_a_gte_5, pred_a_gte_5_dup])?;

        assert_eq!(result.len(), 1, "Should simplify to one predicate");
        assert!(
            builder.find_predicate(&result, "gte", 0, Some(5)),
            "Should keep A >= 5 predicate"
        );
    }

    // Test: A > 5 AND A >= 5 => A > 5
    {
        let pred_a_gt_5 = builder.gt(col_a.clone(), const_5.clone());
        let pred_a_gte_5 = builder.gte(col_a.clone(), const_5.clone());

        let result = run_optimizer(vec![pred_a_gt_5, pred_a_gte_5])?;

        assert_eq!(result.len(), 1, "Should simplify to one predicate");
        assert!(
            builder.find_predicate(&result, "gt", 0, Some(5)),
            "Should simplify to A > 5"
        );
    }

    // Test: A > 3 AND A < 7 => 3 < A < 7 (range)
    {
        let pred_a_gt_3 = builder.gt(col_a.clone(), const_3.clone());
        let pred_a_lt_7 = builder.lt(col_a.clone(), const_7.clone());

        let result = run_optimizer(vec![pred_a_gt_3, pred_a_lt_7])?;

        assert_eq!(result.len(), 2, "Should keep both predicates for range");
    }

    // Test: A >= 3 AND A <= 7 => 3 <= A <= 7 (inclusive range)
    {
        let pred_a_gte_3 = builder.gte(col_a.clone(), const_3.clone());
        let pred_a_lte_7 = builder.lte(col_a.clone(), const_7.clone());

        let result = run_optimizer(vec![pred_a_gte_3, pred_a_lte_7])?;

        assert_eq!(
            result.len(),
            2,
            "Should keep both predicates for inclusive range"
        );
    }

    // Test: A > 5 AND A <= 5 => false (contradiction)
    {
        let pred_a_gt_5 = builder.gt(col_a.clone(), const_5.clone());
        let pred_a_lte_5 = builder.lte(col_a.clone(), const_5.clone());

        let result = run_optimizer(vec![pred_a_gt_5, pred_a_lte_5])?;

        assert!(
            builder.is_boolean_constant(&result, false),
            "Should detect contradiction and return false"
        );
    }

    // Test: A >= 5 AND A < 5 => false (contradiction)
    {
        let pred_a_gte_5 = builder.gte(col_a.clone(), const_5.clone());
        let pred_a_lt_5 = builder.lt(col_a.clone(), const_5.clone());

        let result = run_optimizer(vec![pred_a_gte_5, pred_a_lt_5])?;

        assert!(
            builder.is_boolean_constant(&result, false),
            "Should detect contradiction and return false"
        );
    }

    // Test: A > 7 AND A < 5 => false (contradiction)
    {
        let pred_a_gt_7 = builder.gt(col_a.clone(), const_7.clone());
        let pred_a_lt_5 = builder.lt(col_a.clone(), const_5.clone());

        let result = run_optimizer(vec![pred_a_gt_7, pred_a_lt_5])?;

        assert!(
            builder.is_boolean_constant(&result, false),
            "Should detect contradiction and return false"
        );
    }

    // Test: A >= 7 AND A <= 5 => false (contradiction)
    {
        let pred_a_gte_7 = builder.gte(col_a.clone(), const_7.clone());
        let pred_a_lte_5 = builder.lte(col_a.clone(), const_5.clone());

        let result = run_optimizer(vec![pred_a_gte_7, pred_a_lte_5])?;

        assert!(
            builder.is_boolean_constant(&result, false),
            "Should detect contradiction and return false"
        );
    }

    Ok(())
}

#[test]
fn test_special_transformations() -> anyhow::Result<()> {
    // Create a builder for expressions
    let mut builder = ExprBuilder::new();

    // Setup common columns and constants
    let col_a = builder.column("A", 0, "A", DataType::Number(NumberDataType::Int64), "", 0);
    let const_5 = builder.int(5);

    // Test: A >= 5 AND A <= 5 => A = 5
    {
        let pred_a_gte_5 = builder.gte(col_a.clone(), const_5.clone());
        let pred_a_lte_5 = builder.lte(col_a.clone(), const_5.clone());

        let result = run_optimizer(vec![pred_a_gte_5, pred_a_lte_5])?;

        assert_eq!(result.len(), 1, "Should simplify to one predicate");
        assert!(
            builder.find_predicate(&result, "eq", 0, Some(5)),
            "Should infer A = 5 predicate"
        );
    }

    // Test: A <= 5 AND A >= 5 => A = 5 (reverse order)
    {
        let pred_a_lte_5 = builder.lte(col_a.clone(), const_5.clone());
        let pred_a_gte_5 = builder.gte(col_a.clone(), const_5.clone());

        let result = run_optimizer(vec![pred_a_lte_5, pred_a_gte_5])?;

        assert_eq!(result.len(), 1, "Should simplify to one predicate");
        assert!(
            builder.find_predicate(&result, "eq", 0, Some(5)),
            "Should infer A = 5 predicate"
        );
    }

    // Test: A != 5 AND A <= 5 => A < 5
    {
        let pred_a_ne_5 = builder.neq(col_a.clone(), const_5.clone());
        let pred_a_lte_5 = builder.lte(col_a.clone(), const_5.clone());

        let result = run_optimizer(vec![pred_a_ne_5, pred_a_lte_5])?;

        assert_eq!(result.len(), 1, "Should simplify to one predicate");
        assert!(
            builder.find_predicate(&result, "lt", 0, Some(5)),
            "Should simplify to A < 5"
        );
    }

    // Test: A <= 5 AND A != 5 => A < 5 (reverse order)
    {
        let pred_a_lte_5 = builder.lte(col_a.clone(), const_5.clone());
        let pred_a_ne_5 = builder.neq(col_a.clone(), const_5.clone());

        let result = run_optimizer(vec![pred_a_lte_5, pred_a_ne_5])?;

        assert_eq!(result.len(), 1, "Should simplify to one predicate");
        assert!(
            builder.find_predicate(&result, "lt", 0, Some(5)),
            "Should simplify to A < 5"
        );
    }

    // Test: A != 5 AND A >= 5 => A > 5
    {
        let pred_a_ne_5 = builder.neq(col_a.clone(), const_5.clone());
        let pred_a_gte_5 = builder.gte(col_a.clone(), const_5.clone());

        let result = run_optimizer(vec![pred_a_ne_5, pred_a_gte_5])?;

        assert_eq!(result.len(), 1, "Should simplify to one predicate");
        assert!(
            builder.find_predicate(&result, "gt", 0, Some(5)),
            "Should simplify to A > 5"
        );
    }

    // Test: A >= 5 AND A != 5 => A > 5 (reverse order)
    {
        let pred_a_gte_5 = builder.gte(col_a.clone(), const_5.clone());
        let pred_a_ne_5 = builder.neq(col_a.clone(), const_5.clone());

        let result = run_optimizer(vec![pred_a_gte_5, pred_a_ne_5])?;

        assert_eq!(result.len(), 1, "Should simplify to one predicate");
        assert!(
            builder.find_predicate(&result, "gt", 0, Some(5)),
            "Should simplify to A > 5"
        );
    }

    // Test: A = 5 AND A >= 5 AND A <= 5 => A = 5 (redundant conditions)
    {
        let pred_a_eq_5 = builder.eq(col_a.clone(), const_5.clone());
        let pred_a_gte_5 = builder.gte(col_a.clone(), const_5.clone());
        let pred_a_lte_5 = builder.lte(col_a.clone(), const_5.clone());

        let result = run_optimizer(vec![pred_a_eq_5, pred_a_gte_5, pred_a_lte_5])?;

        assert!(
            builder.find_predicate(&result, "eq", 0, Some(5)),
            "Should simplify to A = 5 predicate"
        );
    }

    Ok(())
}

// ===== Test Cases for Contradiction Detection =====

#[test]
fn test_contradiction_detection() -> anyhow::Result<()> {
    // Create a builder for expressions
    let mut builder = ExprBuilder::new();

    // Setup common columns and constants
    let col_a = builder.column("A", 0, "A", DataType::Number(NumberDataType::Int64), "", 0);
    let col_b = builder.column("B", 1, "B", DataType::Number(NumberDataType::Int64), "", 0);
    let const_3 = builder.int(3);
    let const_10 = builder.int(10);

    // Additional columns for transitive equality tests
    let col_c = builder.column("C", 2, "C", DataType::Number(NumberDataType::Int64), "", 0);
    let col_d = builder.column("D", 3, "D", DataType::Number(NumberDataType::Int64), "", 0);
    let const_5 = builder.int(5);
    let const_7 = builder.int(7);

    // Test: A = 5 AND A = 7 => false (contradiction)
    {
        let pred_a_eq_5 = builder.eq(col_a.clone(), const_5.clone());
        let pred_a_eq_7 = builder.eq(col_a.clone(), const_7.clone());

        let result = run_optimizer(vec![pred_a_eq_5, pred_a_eq_7])?;

        assert!(
            builder.is_boolean_constant(&result, false),
            "Should detect contradiction and return false"
        );
    }

    // Test: A = 5 AND A != 5 => false (contradiction)
    {
        let pred_a_eq_5 = builder.eq(col_a.clone(), const_5.clone());
        let pred_a_ne_5 = builder.neq(col_a.clone(), const_5.clone());

        let result = run_optimizer(vec![pred_a_eq_5, pred_a_ne_5])?;

        assert!(
            builder.is_boolean_constant(&result, false),
            "Should detect contradiction and return false"
        );
    }

    // Test: A < 5 AND A >= 5 => false (contradiction)
    {
        let pred_a_lt_5 = builder.lt(col_a.clone(), const_5.clone());
        let pred_a_gte_5 = builder.gte(col_a.clone(), const_5.clone());

        let result = run_optimizer(vec![pred_a_lt_5, pred_a_gte_5])?;

        assert!(
            builder.is_boolean_constant(&result, false),
            "Should detect contradiction and return false"
        );
    }

    // Test: A <= 5 AND A > 5 => false (contradiction)
    {
        let pred_a_lte_5 = builder.lte(col_a.clone(), const_5.clone());
        let pred_a_gt_5 = builder.gt(col_a.clone(), const_5.clone());

        let result = run_optimizer(vec![pred_a_lte_5, pred_a_gt_5])?;

        assert!(
            builder.is_boolean_constant(&result, false),
            "Should detect contradiction and return false"
        );
    }

    // Test: A < 3 AND A > 5 => false (contradiction)
    {
        let pred_a_lt_3 = builder.lt(col_a.clone(), const_3.clone());
        let pred_a_gt_5 = builder.gt(col_a.clone(), const_5.clone());

        let result = run_optimizer(vec![pred_a_lt_3, pred_a_gt_5])?;

        assert!(
            builder.is_boolean_constant(&result, false),
            "Should detect contradiction and return false"
        );
    }

    // Test: A = B AND B = C => A = B AND B = C AND A = C (transitive equality)
    {
        let pred_a_eq_b = builder.eq(col_a.clone(), col_b.clone());
        let pred_b_eq_c = builder.eq(col_b.clone(), col_c.clone());

        let result = run_optimizer(vec![pred_a_eq_b, pred_b_eq_c])?;

        assert_eq!(result.len(), 3, "Should add transitive equality");
    }

    // Test: A = B AND B = C AND C = D => should infer A = D (transitive equality chain)
    {
        let pred_a_eq_b = builder.eq(col_a.clone(), col_b.clone());
        let pred_b_eq_c = builder.eq(col_b.clone(), col_c.clone());
        let pred_c_eq_d = builder.eq(col_c.clone(), col_d.clone());

        let result = run_optimizer(vec![pred_a_eq_b, pred_b_eq_c, pred_c_eq_d])?;

        // Should have at least 6 equalities (A=B, B=C, C=D, A=C, B=D, A=D)
        assert!(result.len() >= 6, "Should infer all transitive equalities");
    }

    // Test: A = 10 AND A = B => A = 10 AND B = 10 AND A = B
    {
        let pred_a_eq_10 = builder.eq(col_a.clone(), const_10.clone());
        let pred_a_eq_b = builder.eq(col_a.clone(), col_b.clone());

        let result = run_optimizer(vec![pred_a_eq_10, pred_a_eq_b])?;

        assert_eq!(result.len(), 3, "Should propagate constant to B");
        assert!(
            builder.find_predicate(&result, "eq", 1, Some(10)),
            "Should add B = 10"
        );
    }

    // Test: A = 10 AND A = B AND B = C => A = 10 AND B = 10 AND C = 10
    {
        let pred_a_eq_10 = builder.eq(col_a.clone(), const_10.clone());
        let pred_a_eq_b = builder.eq(col_a.clone(), col_b.clone());
        let pred_b_eq_c = builder.eq(col_b.clone(), col_c.clone());

        let result = run_optimizer(vec![pred_a_eq_10, pred_a_eq_b, pred_b_eq_c])?;

        assert!(
            builder.find_predicate(&result, "eq", 1, Some(10)),
            "Should add B = 10"
        );
        assert!(
            builder.find_predicate(&result, "eq", 2, Some(10)),
            "Should add C = 10"
        );
    }

    // Test: A > 1 AND A < 10 AND A = B => B > 1 AND B < 10 AND A = B
    {
        let const_1 = builder.int(1);
        let pred_a_gt_1 = builder.gt(col_a.clone(), const_1.clone());
        let pred_a_lt_10 = builder.lt(col_a.clone(), const_10.clone());
        let pred_a_eq_b = builder.eq(col_a.clone(), col_b.clone());

        let result = run_optimizer(vec![pred_a_gt_1, pred_a_lt_10, pred_a_eq_b])?;

        assert!(
            builder.find_predicate(&result, "gt", 1, Some(1)),
            "Should add B > 1"
        );
        assert!(
            builder.find_predicate(&result, "lt", 1, Some(10)),
            "Should add B < 10"
        );

        assert!(
            builder.find_predicate(&result, "lt", 1, Some(10)),
            "Should infer B < 10 predicate through equality"
        );
    }

    Ok(())
}

// ===== Test Cases for Different Data Types =====
#[test]
fn test_different_data_types() -> anyhow::Result<()> {
    // Create a builder for expressions
    let mut builder = ExprBuilder::new();

    // Test with different data types
    let col_int8 = builder.column(
        "int8",
        0,
        "int8",
        DataType::Number(NumberDataType::Int8),
        "",
        0,
    );
    let col_uint8 = builder.column(
        "uint8",
        1,
        "uint8",
        DataType::Number(NumberDataType::UInt8),
        "",
        0,
    );
    let col_float = builder.column(
        "float",
        2,
        "float",
        DataType::Number(NumberDataType::Float64),
        "",
        0,
    );
    let col_variant = builder.column("variant", 3, "variant", DataType::Variant, "", 0);
    let _col_a = builder.column("A", 3, "A", DataType::Number(NumberDataType::Int64), "", 0);

    // Test: int8 column with values at type boundaries
    {
        // i8::MIN = -128, i8::MAX = 127
        let const_min = builder.int(-128);
        let const_max = builder.int(127);

        // Test: int8 > -128 AND int8 < 127 => keep both predicates
        let pred_gt_min = builder.gt(col_int8.clone(), const_min.clone());
        let pred_lt_max = builder.lt(col_int8.clone(), const_max.clone());

        let result = run_optimizer(vec![pred_gt_min, pred_lt_max])?;

        assert_eq!(result.len(), 2, "Should keep both boundary predicates");
    }

    // Test: uint8 column with values at type boundaries
    {
        // u8::MIN = 0, u8::MAX = 255
        let const_min = builder.int(0);
        let const_max = builder.int(255);

        // Test: uint8 >= 0 AND uint8 <= 255 => no constraints (always true)
        let pred_gte_min = builder.gte(col_uint8.clone(), const_min.clone());
        let pred_lte_max = builder.lte(col_uint8.clone(), const_max.clone());

        let result = run_optimizer(vec![pred_gte_min, pred_lte_max])?;

        // The optimizer might simplify this in different ways, but it should not be a contradiction
        assert!(
            !builder.is_boolean_constant(&result, false),
            "Should not be a contradiction"
        );
    }

    // Test: float column with equality and range checks
    {
        let const_5_0 = builder.float(5.0);

        // Test: float = 5.0 AND float >= 5.0 AND float <= 5.0 => float = 5.0
        let pred_eq_5 = builder.eq(col_float.clone(), const_5_0.clone());
        let pred_gte_5 = builder.gte(col_float.clone(), const_5_0.clone());
        let pred_lte_5 = builder.lte(col_float.clone(), const_5_0.clone());

        let result = run_optimizer(vec![pred_eq_5, pred_gte_5, pred_lte_5])?;

        assert!(
            get_function_name(&result[0]) == Some("eq"),
            "Should infer float = 5.0 predicate"
        );
    }

    // Test: mixing integer and variant types
    {
        let const_5_int = builder.int(5);

        // Test: variant = int8 AND variant = 5
        let pred_variant_eq_int = builder.eq(col_variant.clone(), col_int8.clone());
        let pred_variant_eq_5 = builder.eq(col_variant.clone(), const_5_int.clone());

        let result = run_optimizer(vec![pred_variant_eq_int, pred_variant_eq_5])?;

        assert_eq!(result.len(), 2, "Shouldn't add transitive equality");
    }

    // Different data type not work yet, need fix.
    // Test: mixing integer and float types
    // {
    // let const_5_int = builder.int(5);
    // let const_5_float = builder.float(5.0);
    //
    // Test: A > 5 AND A > 5.0 => A > 5 (or A > 5.0, depending on type conversion)
    // let pred_gt_5_int = builder.gt(_col_a.clone(), const_5_int.clone());
    // let pred_gt_5_float = builder.gt(_col_a.clone(), const_5_float.clone());
    //
    // let result = run_optimizer(vec![pred_gt_5_int, pred_gt_5_float])?;
    //
    // assert_eq!(result.len(), 1, "Should simplify to one predicate");
    // assert!(
    // get_function_name(&result[0]) == Some("gt"),
    // "Function should be gt (greater than)"
    // );
    // }
    //
    // Test: mixing integer and float types with different values
    // {
    // let const_5_int = create_int_constant(5);
    // let const_7_float = create_float_constant(7.0);
    //
    // Test: A > 5 AND A > 7.0 => A > 7.0
    // let pred_gt_5_int = create_comparison(col_a.clone(), const_5_int.clone(), ComparisonOp::GT)?;
    // let pred_gt_7_float = create_comparison(col_a.clone(), const_7_float.clone(), ComparisonOp::GT)?;
    //
    // let result = run_optimizer(vec![pred_gt_5_int, pred_gt_7_float])?;
    //
    // assert_eq!(result.len(), 1, "Should simplify to one predicate");
    // assert!(
    // get_function_name(&result[0]) == Some("gt"),
    // "Function should be gt (greater than)"
    // );
    //
    // The value should be the larger one (7.0)
    // if let ScalarExpr::FunctionCall(func) = &result[0] {
    // if let ScalarExpr::ConstantExpr(constant) = &func.arguments[1] {
    // if let Scalar::Number(NumberScalar::Float64(value)) = &constant.value {
    // assert_eq!(value.0, 7.0, "Value should be 7.0");
    // } else if let Scalar::Number(NumberScalar::Int64(value)) = &constant.value {
    // assert_eq!(*value, 7, "Value should be 7");
    // } else {
    // panic!("Unexpected constant type");
    // }
    // } else {
    // panic!("Expected constant expression");
    // }
    // } else {
    // panic!("Expected function call");
    // }
    // }
    //
    // Test: mixing integer and float types with contradictions
    // {
    // let const_5_int = create_int_constant(5);
    // let const_3_float = create_float_constant(3.0);
    //
    // Test: A < 3.0 AND A >= 5 => false (contradiction)
    // let pred_lt_3_float = create_comparison(col_a.clone(), const_3_float.clone(), ComparisonOp::LT)?;
    // let pred_gte_5_int = create_comparison(col_a.clone(), const_5_int.clone(), ComparisonOp::GTE)?;
    //
    // let result = run_optimizer(vec![pred_lt_3_float, pred_gte_5_int])?;
    //
    // assert!(
    // is_boolean_constant(&result, false),
    // "Should detect contradiction and return false"
    // );
    // }

    Ok(())
}

#[test]
fn test_predicate_merging_and_duplication() -> anyhow::Result<()> {
    // Create a builder for expressions
    let mut builder = ExprBuilder::new();

    // Setup common columns and constants
    let col_a = builder.column("A", 0, "A", DataType::Number(NumberDataType::Int64), "", 0);
    let col_b = builder.column("B", 1, "B", DataType::Number(NumberDataType::Int64), "", 0);
    let const_3 = builder.int(3);
    let const_5 = builder.int(5);
    let const_7 = builder.int(7);
    let const_10 = builder.int(10);

    // Case 1: NotEqual with NotEqual (different values) - MergeResult::All
    // Derivation: A != 3 AND A != 7 => A != 3 AND A != 7
    // Explanation: Two inequalities with different values, both should be preserved without duplication
    {
        let pred_a_ne_3 = builder.neq(col_a.clone(), const_3.clone());
        let pred_a_ne_7 = builder.neq(col_a.clone(), const_7.clone());

        let result = run_optimizer(vec![pred_a_ne_3, pred_a_ne_7])?;

        // Check that we have exactly two predicates
        assert_eq!(
            result.len(),
            2,
            "Case 1: Should have exactly two predicates"
        );

        // Count occurrences of each predicate to ensure no duplicates
        let ne_3_count = builder.count_predicates(&result, "noteq", 0, Some(3));
        let ne_7_count = builder.count_predicates(&result, "noteq", 0, Some(7));

        assert_eq!(
            ne_3_count, 1,
            "Case 1: Should have exactly one A != 3 predicate"
        );
        assert_eq!(
            ne_7_count, 1,
            "Case 1: Should have exactly one A != 7 predicate"
        );
    }

    // Case 2: NotEqual with NotEqual (same value) - MergeResult::Left
    // Derivation: A != 3 AND A != 3 => A != 3
    // Explanation: Two identical inequalities, should be deduplicated
    {
        let pred_a_ne_3 = builder.neq(col_a.clone(), const_3.clone());
        let pred_a_ne_3_dup = builder.neq(col_a.clone(), const_3.clone());

        let result = run_optimizer(vec![pred_a_ne_3, pred_a_ne_3_dup])?;

        // Check that we have exactly one predicate (deduplication)
        assert_eq!(
            result.len(),
            1,
            "Case 2: Should have exactly one predicate after deduplication"
        );

        // Verify it's the correct predicate
        assert!(
            builder.find_predicate(&result, "noteq", 0, Some(3)),
            "Case 2: Should keep A != 3 predicate"
        );
    }

    // Case 3: LT with GT (X > Y) - MergeResult::All (range)
    // Derivation: A < 10 AND A > 3 => 3 < A < 10
    // Explanation: This forms a range, both predicates should be preserved to represent this range
    {
        let pred_a_lt_10 = builder.lt(col_a.clone(), const_10.clone());
        let pred_a_gt_3 = builder.gt(col_a.clone(), const_3.clone());

        let result = run_optimizer(vec![pred_a_lt_10, pred_a_gt_3])?;

        // Check that we have exactly two predicates (range: 3 < A < 10)
        assert_eq!(
            result.len(),
            2,
            "Case 3: Should have exactly two predicates for range"
        );

        // Count occurrences of each predicate
        let lt_10_count = builder.count_predicates(&result, "lt", 0, Some(10));
        let gt_3_count = builder.count_predicates(&result, "gt", 0, Some(3));

        assert_eq!(
            lt_10_count, 1,
            "Case 3: Should have exactly one A < 10 predicate"
        );
        assert_eq!(
            gt_3_count, 1,
            "Case 3: Should have exactly one A > 3 predicate"
        );
    }

    // Case 4: Multiple predicates on different columns - should all be preserved
    // Derivation: A != 3 AND A < 10 AND B > 5 AND B != 7 => keep all predicates
    // Explanation: Predicates involve different columns, all should be preserved
    {
        let pred_a_ne_3 = builder.neq(col_a.clone(), const_3.clone());
        let pred_a_lt_10 = builder.lt(col_a.clone(), const_10.clone());
        let pred_b_gt_5 = builder.gt(col_b.clone(), const_5.clone());
        let pred_b_ne_7 = builder.neq(col_b.clone(), const_7.clone());

        let result = run_optimizer(vec![pred_a_ne_3, pred_a_lt_10, pred_b_gt_5, pred_b_ne_7])?;

        // Check that we have the right number of predicates
        // Note: The optimizer might simplify some predicates, so we check for specific predicates
        assert!(
            result.len() >= 4,
            "Case 4: Should have at least 4 predicates"
        );

        // Verify each predicate appears exactly once
        let ne_3_count = builder.count_predicates(&result, "noteq", 0, Some(3));
        let lt_10_count = builder.count_predicates(&result, "lt", 0, Some(10));
        let gt_5_count = builder.count_predicates(&result, "gt", 1, Some(5));
        let ne_7_count = builder.count_predicates(&result, "noteq", 1, Some(7));

        assert_eq!(
            ne_3_count, 1,
            "Case 4: Should have exactly one A != 3 predicate"
        );
        assert_eq!(
            lt_10_count, 1,
            "Case 4: Should have exactly one A < 10 predicate"
        );
        assert_eq!(
            gt_5_count, 1,
            "Case 4: Should have exactly one B > 5 predicate"
        );
        assert_eq!(
            ne_7_count, 1,
            "Case 4: Should have exactly one B != 7 predicate"
        );
    }

    // Case 5: Edge case - NotEqual with LTE (X = Y) - should convert to LT
    // Derivation: A != 5 AND A <= 5 => A < 5
    // Explanation: If A is not equal to 5 and A is less than or equal to 5, then A must be less than 5
    {
        let pred_a_ne_5 = builder.neq(col_a.clone(), const_5.clone());
        let pred_a_lte_5 = builder.lte(col_a.clone(), const_5.clone());

        let result = run_optimizer(vec![pred_a_ne_5, pred_a_lte_5])?;

        // Should simplify to A < 5
        assert_eq!(result.len(), 1, "Case 5: Should simplify to one predicate");

        // Verify it's A < 5, not A <= 5
        let lt_5_count = builder.count_predicates(&result, "lt", 0, Some(5));
        let lte_5_count = builder.count_predicates(&result, "lte", 0, Some(5));

        assert_eq!(
            lt_5_count, 1,
            "Case 5: Should have exactly one A < 5 predicate"
        );
        assert_eq!(lte_5_count, 0, "Case 5: Should not have A <= 5 predicate");
    }

    // Case 6: Edge case - GTE with LTE (X = Y) - should convert to Equal
    // Derivation: A >= 5 AND A <= 5 => A = 5
    // Explanation: If A is greater than or equal to 5 and A is less than or equal to 5, then A must equal 5
    {
        let pred_a_gte_5 = builder.gte(col_a.clone(), const_5.clone());
        let pred_a_lte_5 = builder.lte(col_a.clone(), const_5.clone());

        let result = run_optimizer(vec![pred_a_gte_5, pred_a_lte_5])?;

        // Should simplify to A = 5
        assert_eq!(result.len(), 1, "Case 6: Should simplify to one predicate");

        // Verify it's A = 5, not A >= 5 or A <= 5
        let eq_5_count = builder.count_predicates(&result, "eq", 0, Some(5));
        let gte_5_count = builder.count_predicates(&result, "gte", 0, Some(5));
        let lte_5_count = builder.count_predicates(&result, "lte", 0, Some(5));

        assert_eq!(
            eq_5_count, 1,
            "Case 6: Should have exactly one A = 5 predicate"
        );
        assert_eq!(gte_5_count, 0, "Case 6: Should not have A >= 5 predicate");
        assert_eq!(lte_5_count, 0, "Case 6: Should not have A <= 5 predicate");
    }

    // Case 7: Complex case - multiple predicates that should be merged and simplified
    // Derivation: A > 3 AND A < 10 AND A != 5 AND A != 7 => 3 < A < 10 AND A != 5 AND A != 7
    // Explanation: These predicates together define a range with specific values excluded
    {
        let pred_a_gt_3 = builder.gt(col_a.clone(), const_3.clone());
        let pred_a_lt_10 = builder.lt(col_a.clone(), const_10.clone());
        let pred_a_ne_5 = builder.neq(col_a.clone(), const_5.clone());
        let pred_a_ne_7 = builder.neq(col_a.clone(), const_7.clone());

        let result = run_optimizer(vec![pred_a_gt_3, pred_a_lt_10, pred_a_ne_5, pred_a_ne_7])?;

        // Should keep all predicates (3 < A < 10, A != 5, A != 7)
        assert_eq!(result.len(), 4, "Case 7: Should have 4 predicates");

        // Verify each predicate appears exactly once
        let gt_3_count = builder.count_predicates(&result, "gt", 0, Some(3));
        let lt_10_count = builder.count_predicates(&result, "lt", 0, Some(10));
        let ne_5_count = builder.count_predicates(&result, "noteq", 0, Some(5));
        let ne_7_count = builder.count_predicates(&result, "noteq", 0, Some(7));

        assert_eq!(
            gt_3_count, 1,
            "Case 7: Should have exactly one A > 3 predicate"
        );
        assert_eq!(
            lt_10_count, 1,
            "Case 7: Should have exactly one A < 10 predicate"
        );
        assert_eq!(
            ne_5_count, 1,
            "Case 7: Should have exactly one A != 5 predicate"
        );
        assert_eq!(
            ne_7_count, 1,
            "Case 7: Should have exactly one A != 7 predicate"
        );
    }

    // Case 8: NotEqual with GTE (X = Y) - should convert to GT
    // Derivation: A != 5 AND A >= 5 => A > 5
    // Explanation: If A is not equal to 5 and A is greater than or equal to 5, then A must be greater than 5
    {
        let pred_a_ne_5 = builder.neq(col_a.clone(), const_5.clone());
        let pred_a_gte_5 = builder.gte(col_a.clone(), const_5.clone());

        let result = run_optimizer(vec![pred_a_ne_5, pred_a_gte_5])?;

        // Should simplify to A > 5
        assert_eq!(result.len(), 1, "Case 8: Should simplify to one predicate");

        // Verify it's A > 5, not A >= 5
        let gt_5_count = builder.count_predicates(&result, "gt", 0, Some(5));
        let gte_5_count = builder.count_predicates(&result, "gte", 0, Some(5));

        assert_eq!(
            gt_5_count, 1,
            "Case 8: Should have exactly one A > 5 predicate"
        );
        assert_eq!(gte_5_count, 0, "Case 8: Should not have A >= 5 predicate");
    }

    Ok(())
}
