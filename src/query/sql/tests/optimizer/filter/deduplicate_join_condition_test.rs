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

use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::optimizer::optimizers::operator::DeduplicateJoinConditionOptimizer;
use databend_common_sql::planner::binder::ColumnBinding;
use databend_common_sql::planner::binder::Visibility;
use databend_common_sql::planner::plans::BoundColumnRef;
use databend_common_sql::planner::plans::Join;
use databend_common_sql::planner::plans::JoinEquiCondition;
use databend_common_sql::planner::plans::JoinType;
use databend_common_sql::planner::plans::RelOperator;
use databend_common_sql::planner::plans::ScalarExpr;
use databend_common_sql::IndexType;

// ===== Helper Functions =====

/// Creates a column reference with the given index, name, data type, table name and table index
fn create_column_ref1(
    index: IndexType,
    name: &str,
    data_type: DataType,
    table_name: Option<&str>,
    table_index: Option<IndexType>,
) -> ScalarExpr {
    let column = ColumnBinding {
        index,
        column_name: name.to_string(),
        data_type: Box::new(data_type),
        database_name: None,
        table_name: table_name.map(|s| s.to_string()),
        column_position: None,
        table_index,
        visibility: Visibility::Visible,
        virtual_expr: None,
    };
    ScalarExpr::BoundColumnRef(BoundColumnRef { column, span: None })
}

/// Creates a simple SExpr for a table scan
fn create_table_scan(table_index: usize, _name: &str) -> SExpr {
    let scan = databend_common_sql::plans::Scan {
        table_index,
        ..Default::default()
    };
    SExpr::create_leaf(Arc::new(RelOperator::Scan(scan)))
}

/// Creates a join SExpr with the specified conditions
fn create_join(
    left: SExpr,
    right: SExpr,
    equi_conditions: Vec<JoinEquiCondition>,
    join_type: JoinType,
) -> SExpr {
    let join = Join {
        join_type,
        equi_conditions,
        ..Default::default()
    };
    SExpr::create_binary(
        Arc::new(RelOperator::Join(join)),
        Arc::new(left),
        Arc::new(right),
    )
}

/// Creates a join condition between two columns
fn create_join_condition(
    left: ScalarExpr,
    right: ScalarExpr,
    is_null_equal: bool,
) -> JoinEquiCondition {
    JoinEquiCondition::new(left, right, is_null_equal)
}

/// Runs the DeduplicateJoinConditionOptimizer on the given SExpr
fn run_optimizer(s_expr: SExpr) -> Result<SExpr> {
    let optimizer = DeduplicateJoinConditionOptimizer::new();
    optimizer.optimize(&s_expr)
}

/// Converts an SExpr to a readable string representation using a simple indented format
fn sexpr_to_string(s_expr: &SExpr) -> String {
    fn format_join_conditions(s_expr: &SExpr) -> String {
        if let RelOperator::Join(join) = s_expr.plan() {
            let conditions: Vec<String> = join
                .equi_conditions
                .iter()
                .map(|cond| {
                    let left = if let ScalarExpr::BoundColumnRef(left) = &cond.left {
                        // Only use the explicitly provided table_name
                        if let Some(table_name) = &left.column.table_name {
                            format!("{}.{}", table_name, left.column.column_name)
                        } else {
                            // If no table_name is provided, use a placeholder
                            format!("unknown.{}", left.column.column_name)
                        }
                    } else {
                        "?".to_string()
                    };

                    let right = if let ScalarExpr::BoundColumnRef(right) = &cond.right {
                        // Only use the explicitly provided table_name
                        if let Some(table_name) = &right.column.table_name {
                            format!("{}.{}", table_name, right.column.column_name)
                        } else {
                            // If no table_name is provided, use a placeholder
                            format!("unknown.{}", right.column.column_name)
                        }
                    } else {
                        "?".to_string()
                    };

                    if cond.is_null_equal {
                        format!("{} IS NOT DISTINCT FROM {}", left, right)
                    } else {
                        format!("{} = {}", left, right)
                    }
                })
                .collect();

            if conditions.is_empty() {
                "[]".to_string()
            } else {
                format!("[{}]", conditions.join(", "))
            }
        } else {
            "[]".to_string()
        }
    }

    // Recursive function to build the tree string with indentation
    fn build_tree(s_expr: &SExpr, depth: usize) -> String {
        let indent = "  ".repeat(depth);
        let mut result = String::new();

        if let RelOperator::Join(_) = s_expr.plan() {
            // Add the join node with conditions
            result.push_str(&format!(
                "{indent}Join {}\n",
                format_join_conditions(s_expr)
            ));

            // Process children
            let children: Vec<&SExpr> = s_expr.children().collect();
            for child in children {
                result.push_str(&build_tree(child, depth + 1));
            }
        } else if let RelOperator::Scan(scan) = s_expr.plan() {
            // Leaf node (table scan)
            result.push_str(&format!("{indent}Table t{}\n", scan.table_index));
        } else {
            // Other types of nodes
            result.push_str(&format!("{indent}Unknown node\n"));
        }

        result
    }

    // Build the tree structure
    build_tree(s_expr, 0)
}

/// Helper function to normalize a string while preserving structure
/// This removes leading/trailing whitespace from each line and normalizes line endings
fn normalize_string_preserving_structure(s: &str) -> String {
    s.lines()
        .map(|line| line.trim())
        .filter(|line| !line.is_empty())
        .collect::<Vec<_>>()
        .join("\n")
}

/// Helper function to compare trees before and after optimization
fn compare_trees(
    before: &SExpr,
    after: &SExpr,
    before_patterns: &[&str],
    after_patterns: &[&str],
) -> Result<()> {
    let before_str = sexpr_to_string(before);
    let after_str = sexpr_to_string(after);

    // Verify the before tree matches expected patterns
    let mut before_matched = false;
    let normalized_before = normalize_string_preserving_structure(&before_str);

    for &pattern in before_patterns.iter() {
        let normalized_pattern = normalize_string_preserving_structure(pattern);

        // Use normalized strings for comparison
        if normalized_before == normalized_pattern {
            before_matched = true;
            break;
        }
    }

    if !before_matched && !before_patterns.is_empty() {
        return Err(ErrorCode::from_string(format!(
            "Input tree does not match expected pattern. Please update the before_patterns in the test.\nExpected one of:\n{}\nBut got:\n{}",
            before_patterns.join("\n"),
            before_str
        )));
    }

    // Verify the after tree matches expected patterns
    let normalized_after = normalize_string_preserving_structure(&after_str);
    let mut after_matched = false;

    for &pattern in after_patterns.iter() {
        let normalized_pattern = normalize_string_preserving_structure(pattern);

        // Use normalized strings for comparison
        if normalized_after == normalized_pattern {
            after_matched = true;
            break;
        }
    }

    if !after_matched {
        return Err(ErrorCode::from_string(format!(
            "After optimization tree does not match expected pattern.\nExpected one of:\n{}\nBut got:\n{}", 
            after_patterns.join("\n"), 
            after_str
        )));
    }

    Ok(())
}

// ===== Test Cases =====

// Test case for basic deduplication
// For example: SELECT * FROM t1, t2, t3 WHERE t1.id = t2.id AND t1.id = t3.id AND t2.id = t3.id
//
// Before optimization:
//    Join [t1.id = t3.id, t2.id = t3.id]
//    /  \
//   t3   \
//       Join [t1.id = t2.id]
//       /  \
//      t1  t2
//
// After optimization:
//    Join [t1.id = t3.id]  (or [t2.id = t3.id], one of them is removed)
//    /  \
//   t3   \
//       Join [t1.id = t2.id]
//       /  \
//      t1  t2
//
// Optimization: Removes redundant join condition (t2.id = t3.id or t1.id = t3.id) since one can be
// inferred from the transitive relationship t1.id = t2.id AND t1.id = t3.id (or t1.id = t2.id AND t2.id = t3.id)
#[test]
fn test_basic_deduplication() -> Result<()> {
    // Create column references for t1.id, t2.id, and t3.id
    let t1_id = create_column_ref1(
        0,
        "id",
        DataType::Number(NumberDataType::Int64),
        Some("t1"),
        Some(0),
    );
    let t2_id = create_column_ref1(
        1,
        "id",
        DataType::Number(NumberDataType::Int64),
        Some("t2"),
        Some(1),
    );
    let t3_id = create_column_ref1(
        2,
        "id",
        DataType::Number(NumberDataType::Int64),
        Some("t3"),
        Some(2),
    );

    // Create table scans
    let t1 = create_table_scan(0, "t1");
    let t2 = create_table_scan(1, "t2");
    let t3 = create_table_scan(2, "t3");

    // Create join conditions
    let cond_t1_t2 = create_join_condition(t1_id.clone(), t2_id.clone(), false);
    let cond_t1_t3 = create_join_condition(t1_id.clone(), t3_id.clone(), false);
    let cond_t2_t3 = create_join_condition(t2_id.clone(), t3_id.clone(), false);

    // Create the join tree: (t3 JOIN (t1 JOIN t2))
    let join_t1_t2 = create_join(t1, t2, vec![cond_t1_t2], JoinType::Inner);
    let join_tree = create_join(
        t3,
        join_t1_t2,
        vec![cond_t1_t3.clone(), cond_t2_t3.clone()],
        JoinType::Inner,
    );

    // Define expected before optimization patterns
    let before_patterns = [r#"
Join [t1.id = t3.id, t2.id = t3.id]
  Table t2
  Join [t1.id = t2.id]
    Table t0
    Table t1
"#];

    // Define expected after optimization patterns
    let after_patterns = [
        // Pattern 1: t1-t3 condition remains
        r#"
Join [t1.id = t3.id]
  Table t2
  Join [t1.id = t2.id]
    Table t0
    Table t1
"#,
        // Pattern 2: t2-t3 condition remains
        r#"
Join [t2.id = t3.id]
  Table t2
  Join [t1.id = t2.id]
    Table t0
    Table t1
"#,
    ];

    // Run the optimizer
    let optimized = run_optimizer(join_tree.clone())?;

    // Compare trees before and after optimization
    compare_trees(&join_tree, &optimized, &before_patterns, &after_patterns)?;

    Ok(())
}

// Test case for different data types in join conditions
// For example: SELECT * FROM t1, t2, t3 WHERE t1.id = t2.id AND t2.id = t3.id AND t1.id = t3.id
// where t1.id is INT32, t2.id is INT64, and t3.id is FLOAT64
// This tests if the optimizer correctly handles join conditions with different data types
#[test]
fn test_different_data_types() -> Result<()> {
    // Create column references with different data types
    let t1_id = create_column_ref1(
        0,
        "id",
        DataType::Number(NumberDataType::Int32),
        Some("t1"),
        Some(0),
    );
    let t2_id = create_column_ref1(
        1,
        "id",
        DataType::Number(NumberDataType::Int64),
        Some("t2"),
        Some(1),
    );
    let t3_id = create_column_ref1(
        2,
        "id",
        DataType::Number(NumberDataType::Float64),
        Some("t3"),
        Some(2),
    );

    // Create table scans
    let t1 = create_table_scan(0, "t1");
    let t2 = create_table_scan(1, "t2");
    let t3 = create_table_scan(2, "t3");

    // Create join conditions between tables with different data types
    let cond_t1_t2 = create_join_condition(t1_id.clone(), t2_id.clone(), false);
    let cond_t2_t3 = create_join_condition(t2_id.clone(), t3_id.clone(), false);
    let cond_t1_t3 = create_join_condition(t1_id.clone(), t3_id.clone(), false);

    // Create the join tree: ((t1 JOIN t2) JOIN t3)
    let join_t1_t2 = create_join(t1, t2, vec![cond_t1_t2], JoinType::Inner);
    let join_tree = create_join(
        join_t1_t2,
        t3,
        vec![cond_t2_t3.clone(), cond_t1_t3.clone()],
        JoinType::Inner,
    );

    // Define expected before optimization patterns
    let before_patterns = [r#"
    Join [t2.id = t3.id, t1.id = t3.id]
      Join [t1.id = t2.id]
        Table t0
        Table t1
      Table t2
    "#];

    // Define expected after optimization patterns
    let after_patterns = [
        // Pattern 1: t1-t3 condition is removed
        r#"
    Join [t2.id = t3.id]
      Join [t1.id = t2.id]
        Table t0
        Table t1
      Table t2
    "#,
        // Pattern 2: t2-t3 condition is removed
        r#"
    Join [t1.id = t3.id]
      Join [t1.id = t2.id]
        Table t0
        Table t1
      Table t2
    "#,
    ];

    // Run the optimizer
    let optimized = run_optimizer(join_tree.clone())?;

    // Compare trees before and after optimization
    compare_trees(&join_tree, &optimized, &before_patterns, &after_patterns)?;

    Ok(())
}

// Test case for non-redundant join conditions
// For example: SELECT * FROM t1, t2 WHERE t1.id = t2.id AND t1.name = t2.name
//
// Before optimization:
//    Join [t1.id = t2.id, t1.name = t2.name]
//    /  \
//   t1  t2
//
// After optimization (no change):
//    Join [t1.id = t2.id, t1.name = t2.name]
//    /  \
//   t1  t2
//
// Optimization: No change since the conditions are on different columns and not redundant
#[test]
fn test_no_redundant_conditions() -> Result<()> {
    // Create column references for different columns
    let t1_id = create_column_ref1(
        0,
        "id",
        DataType::Number(NumberDataType::Int64),
        Some("t1"),
        Some(0),
    );
    let t2_id = create_column_ref1(
        1,
        "id",
        DataType::Number(NumberDataType::Int64),
        Some("t2"),
        Some(1),
    );
    let t1_name = create_column_ref1(2, "name", DataType::String, Some("t1"), Some(0));
    let t2_name = create_column_ref1(3, "name", DataType::String, Some("t2"), Some(1));

    // Create table scans
    let t1 = create_table_scan(0, "t1");
    let t2 = create_table_scan(1, "t2");

    // Create join conditions on different columns (no redundancy)
    let cond_id = create_join_condition(t1_id.clone(), t2_id.clone(), false);
    let cond_name = create_join_condition(t1_name.clone(), t2_name.clone(), false);

    // Create the join
    let join = create_join(t1, t2, vec![cond_id, cond_name], JoinType::Inner);

    // Define expected before optimization patterns
    let before_patterns = [
        // The tree with both non-redundant conditions
        r#"
Join [t1.id = t2.id, t1.name = t2.name]
  Table t0
  Table t1
"#,
    ];

    // Define expected after optimization patterns
    let after_patterns = [
        // The tree should remain unchanged with both conditions
        r#"
Join [t1.id = t2.id, t1.name = t2.name]
  Table t0
  Table t1
"#,
    ];

    // Run the optimizer
    let optimized = run_optimizer(join.clone())?;

    // Compare trees before and after optimization
    compare_trees(&join, &optimized, &before_patterns, &after_patterns)?;

    Ok(())
}

// Test case for complex join tree with multiple redundant conditions
// For example: SELECT * FROM t1, t2, t3, t4 WHERE t1.id = t2.id AND t2.id = t3.id AND t3.id = t4.id
//              AND t1.id = t3.id AND t2.id = t4.id AND t1.id = t4.id
//
// Before optimization:
//                Join [t3.id = t4.id, t2.id = t4.id, t1.id = t4.id]
//                /  \
//               /    \
//              /      \
//    Join [t2.id = t3.id, t1.id = t3.id]   t4
//    /  \
//   /    \
// Join [t1.id = t2.id]  t3
// /  \
// t1  t2
//
// After optimization:
//                Join [t3.id = t4.id]
//                /  \
//               /    \
//              /      \
//    Join [t2.id = t3.id]   t4
//    /  \
//   /    \
// Join [t1.id = t2.id]  t3
// /  \
// t1  t2
//
// Optimization: Removes 5 redundant join conditions, keeping only the minimum spanning tree
// of join conditions (t1.id = t2.id, t2.id = t3.id, t3.id = t4.id)
#[test]
fn test_complex_transitive_conditions() -> Result<()> {
    // Create column references for multiple tables
    let t1_id = create_column_ref1(
        0,
        "id",
        DataType::Number(NumberDataType::Int64),
        Some("t1"),
        Some(0),
    );
    let t2_id = create_column_ref1(
        1,
        "id",
        DataType::Number(NumberDataType::Int64),
        Some("t2"),
        Some(1),
    );
    let t3_id = create_column_ref1(
        2,
        "id",
        DataType::Number(NumberDataType::Int64),
        Some("t3"),
        Some(2),
    );
    let t4_id = create_column_ref1(
        3,
        "id",
        DataType::Number(NumberDataType::Int64),
        Some("t4"),
        Some(3),
    );

    // Create table scans
    let t1 = create_table_scan(0, "t1");
    let t2 = create_table_scan(1, "t2");
    let t3 = create_table_scan(2, "t3");
    let t4 = create_table_scan(3, "t4");

    // Create join conditions
    let cond_t1_t2 = create_join_condition(t1_id.clone(), t2_id.clone(), false);
    let cond_t2_t3 = create_join_condition(t2_id.clone(), t3_id.clone(), false);
    let cond_t3_t4 = create_join_condition(t3_id.clone(), t4_id.clone(), false);
    let cond_t1_t3 = create_join_condition(t1_id.clone(), t3_id.clone(), false); // Redundant: t1-t2-t3
    let cond_t2_t4 = create_join_condition(t2_id.clone(), t4_id.clone(), false); // Redundant: t2-t3-t4
    let cond_t1_t4 = create_join_condition(t1_id.clone(), t4_id.clone(), false); // Redundant: t1-t2-t3-t4

    // Create a complex join tree: (((t1 JOIN t2) JOIN t3) JOIN t4)
    let join_t1_t2 = create_join(t1, t2, vec![cond_t1_t2], JoinType::Inner);
    let join_t1_t2_t3 = create_join(
        join_t1_t2,
        t3,
        vec![cond_t2_t3, cond_t1_t3],
        JoinType::Inner,
    );
    let join_tree = create_join(
        join_t1_t2_t3,
        t4,
        vec![cond_t3_t4, cond_t2_t4, cond_t1_t4],
        JoinType::Inner,
    );

    // Define expected before optimization patterns
    let before_patterns = [
        // Before optimization, we have all 6 conditions
        r#"
Join [t3.id = t4.id, t2.id = t4.id, t1.id = t4.id]
  Join [t2.id = t3.id, t1.id = t3.id]
    Join [t1.id = t2.id]
      Table t0
      Table t1
    Table t2
  Table t3
"#,
    ];

    // Define expected after optimization patterns
    let after_patterns = [
        // After optimization, we should have only 3 non-redundant conditions
        r#"
Join [t3.id = t4.id]
  Join [t2.id = t3.id]
    Join [t1.id = t2.id]
      Table t0
      Table t1
    Table t2
  Table t3
"#,
    ];

    // Run the optimizer
    let optimized = run_optimizer(join_tree.clone())?;

    // Compare trees before and after optimization
    compare_trees(&join_tree, &optimized, &before_patterns, &after_patterns)?;

    Ok(())
}

// Test case for join conditions with is_null_equal=true
// For example: SELECT * FROM t1, t2, t3 WHERE t1.id IS NOT DISTINCT FROM t2.id
//              AND t2.id IS NOT DISTINCT FROM t3.id AND t1.id IS NOT DISTINCT FROM t3.id
//
// Before optimization:
//    Join [t2.id IS NOT DISTINCT FROM t3.id, t1.id IS NOT DISTINCT FROM t3.id]
//    /  \
//   /    \
// Join [t1.id IS NOT DISTINCT FROM t2.id]  t3
// /  \
// t1  t2
//
// After optimization:
//    Join [t2.id IS NOT DISTINCT FROM t3.id]
//    /  \
//   /    \
// Join [t1.id IS NOT DISTINCT FROM t2.id]  t3
// /  \
// t1  t2
//
// Optimization: Removes redundant IS NOT DISTINCT FROM condition (t1.id IS NOT DISTINCT FROM t3.id)
// since it can be inferred from the transitive relationship
#[test]
fn test_null_equal_conditions() -> Result<()> {
    // Create column references
    let t1_id = create_column_ref1(
        0,
        "id",
        DataType::Number(NumberDataType::Int64),
        Some("t1"),
        Some(0),
    );
    let t2_id = create_column_ref1(
        1,
        "id",
        DataType::Number(NumberDataType::Int64),
        Some("t2"),
        Some(1),
    );
    let t3_id = create_column_ref1(
        2,
        "id",
        DataType::Number(NumberDataType::Int64),
        Some("t3"),
        Some(2),
    );

    // Create table scans
    let t1 = create_table_scan(0, "t1");
    let t2 = create_table_scan(1, "t2");
    let t3 = create_table_scan(2, "t3");

    // Create join conditions with is_null_equal=true
    let cond_t1_t2 = create_join_condition(t1_id.clone(), t2_id.clone(), true);
    let cond_t2_t3 = create_join_condition(t2_id.clone(), t3_id.clone(), true);
    let cond_t1_t3 = create_join_condition(t1_id.clone(), t3_id.clone(), true); // Redundant

    // Create the join tree: (t1 JOIN t2) JOIN t3
    let join_t1_t2 = create_join(t1, t2, vec![cond_t1_t2], JoinType::Inner);
    let join_tree = create_join(
        join_t1_t2,
        t3,
        vec![cond_t2_t3, cond_t1_t3],
        JoinType::Inner,
    );

    // Define expected before optimization patterns
    let before_patterns = [
        // Before optimization, we have both conditions
        r#"
Join [t2.id IS NOT DISTINCT FROM t3.id, t1.id IS NOT DISTINCT FROM t3.id]
  Join [t1.id IS NOT DISTINCT FROM t2.id]
    Table t0
    Table t1
  Table t2
"#,
    ];

    // Define expected after optimization patterns
    let after_patterns = [
        // After optimization, we should have only the non-redundant condition
        r#"
Join [t2.id IS NOT DISTINCT FROM t3.id]
  Join [t1.id IS NOT DISTINCT FROM t2.id]
    Table t0
    Table t1
  Table t2
"#,
    ];

    // Run the optimizer
    let optimized = run_optimizer(join_tree.clone())?;

    // Compare trees before and after optimization
    compare_trees(&join_tree, &optimized, &before_patterns, &after_patterns)?;

    Ok(())
}

// Test case for mixed join conditions with is_null_equal values
// For example: SELECT * FROM t1, t2, t3 WHERE t1.id IS NOT DISTINCT FROM t2.id
//              AND t2.id = t3.id AND t1.id IS NOT DISTINCT FROM t3.id
//
// Before optimization:
//    Join [t2.id = t3.id, t1.id IS NOT DISTINCT FROM t3.id]
//    /  \
//   /    \
// Join [t1.id IS NOT DISTINCT FROM t2.id]  t3
// /  \
// t1  t2
//
// After optimization:
//    Join [t2.id = t3.id]
//    /  \
//   /    \
// Join [t1.id IS NOT DISTINCT FROM t2.id]  t3
// /  \
// t1  t2
//
// Optimization: Removes redundant condition with mixed IS NOT DISTINCT FROM and equality operators
#[test]
fn test_mixed_null_equal_conditions() -> Result<()> {
    // Create column references
    let t1_id = create_column_ref1(
        0,
        "id",
        DataType::Number(NumberDataType::Int64),
        Some("t1"),
        Some(0),
    );
    let t2_id = create_column_ref1(
        1,
        "id",
        DataType::Number(NumberDataType::Int64),
        Some("t2"),
        Some(1),
    );
    let t3_id = create_column_ref1(
        2,
        "id",
        DataType::Number(NumberDataType::Int64),
        Some("t3"),
        Some(2),
    );

    // Create table scans
    let t1 = create_table_scan(0, "t1");
    let t2 = create_table_scan(1, "t2");
    let t3 = create_table_scan(2, "t3");

    // Create join conditions with mixed is_null_equal values
    let cond_t1_t2 = create_join_condition(t1_id.clone(), t2_id.clone(), true);
    let cond_t2_t3 = create_join_condition(t2_id.clone(), t3_id.clone(), false);
    let cond_t1_t3 = create_join_condition(t1_id.clone(), t3_id.clone(), true); // Redundant but with different is_null_equal

    // Create the join tree: (t1 JOIN t2) JOIN t3
    let join_t1_t2 = create_join(t1, t2, vec![cond_t1_t2], JoinType::Inner);
    let join_tree = create_join(
        join_t1_t2,
        t3,
        vec![cond_t2_t3, cond_t1_t3],
        JoinType::Inner,
    );

    // Define expected before optimization patterns
    let before_patterns = [
        // Before optimization, we have both conditions
        r#"
Join [t2.id = t3.id, t1.id IS NOT DISTINCT FROM t3.id]
  Join [t1.id IS NOT DISTINCT FROM t2.id]
    Table t0
    Table t1
  Table t2
"#,
    ];

    // Define expected after optimization patterns
    let after_patterns = [
        // After optimization, we should have only the non-redundant condition
        r#"
Join [t2.id = t3.id]
  Join [t1.id IS NOT DISTINCT FROM t2.id]
    Table t0
    Table t1
  Table t2
"#,
    ];

    // Run the optimizer
    let optimized = run_optimizer(join_tree.clone())?;

    // Compare trees before and after optimization
    compare_trees(&join_tree, &optimized, &before_patterns, &after_patterns)?;

    Ok(())
}

// Test case for non-inner join types (LEFT, RIGHT, FULL)
// For example: SELECT * FROM t1 LEFT JOIN t2 ON t1.id = t2.id LEFT JOIN t3
//              ON t2.id = t3.id AND t1.id = t3.id
//
// Before optimization:
//    Left Join [t2.id = t3.id, t1.id = t3.id]
//    /  \
//   /    \
// Left Join [t1.id = t2.id]  t3
// /  \
// t1  t2
//
// After optimization (no change for non-inner joins):
//    Left Join [t2.id = t3.id, t1.id = t3.id]
//    /  \
//   /    \
// Left Join [t1.id = t2.id]  t3
// /  \
// t1  t2
//
// Optimization: No change for non-inner joins since removing conditions could change semantics
#[test]
fn test_non_inner_join_types() -> Result<()> {
    // Create column references
    let t1_id = create_column_ref1(
        0,
        "id",
        DataType::Number(NumberDataType::Int64),
        Some("t1"),
        Some(0),
    );
    let t2_id = create_column_ref1(
        1,
        "id",
        DataType::Number(NumberDataType::Int64),
        Some("t2"),
        Some(1),
    );
    let t3_id = create_column_ref1(
        2,
        "id",
        DataType::Number(NumberDataType::Int64),
        Some("t3"),
        Some(2),
    );

    // Create table scans
    let t1 = create_table_scan(0, "t1");
    let t2 = create_table_scan(1, "t2");
    let t3 = create_table_scan(2, "t3");

    // Create join conditions
    let cond_t1_t2 = create_join_condition(t1_id.clone(), t2_id.clone(), false);
    let cond_t2_t3 = create_join_condition(t2_id.clone(), t3_id.clone(), false);
    let cond_t1_t3 = create_join_condition(t1_id.clone(), t3_id.clone(), false); // Redundant

    // Create a join tree with non-inner joins
    let join_t1_t2 = create_join(t1, t2, vec![cond_t1_t2], JoinType::Left);
    let join_tree = create_join(join_t1_t2, t3, vec![cond_t2_t3, cond_t1_t3], JoinType::Left);

    // Define expected before optimization patterns
    let before_patterns = [
        // Before optimization, we have both conditions
        r#"
Join [t2.id = t3.id, t1.id = t3.id]
  Join [t1.id = t2.id]
    Table t0
    Table t1
  Table t2
"#,
    ];

    // Define expected after optimization patterns
    let after_patterns = [
        // After optimization, we should still have both conditions for non-inner joins
        r#"
Join [t2.id = t3.id, t1.id = t3.id]
  Join [t1.id = t2.id]
    Table t0
    Table t1
  Table t2
"#,
    ];

    // Run the optimizer
    let optimized = run_optimizer(join_tree.clone())?;

    // Compare trees before and after optimization
    compare_trees(&join_tree, &optimized, &before_patterns, &after_patterns)?;

    Ok(())
}

// Test case for multiple join conditions between the same tables
// Before optimization:
//    Join [t1.id = t2.id, t1.name = t2.name, t1.id = t2.id]
//    /  \
//   t1  t2
//
// After optimization:
//    Join [t1.id = t2.id, t1.name = t2.name]
//    /  \
//   t1  t2
//
// Optimization: Removes duplicate join condition (t1.id = t2.id appears twice),
// but keeps the non-duplicate condition (t1.name = t2.name).
#[test]
fn test_multiple_conditions_same_tables() -> Result<()> {
    // Create column references for different columns
    let t1_id = create_column_ref1(
        0,
        "id",
        DataType::Number(NumberDataType::Int32),
        Some("t1"),
        Some(0),
    );
    let t2_id = create_column_ref1(
        0,
        "id",
        DataType::Number(NumberDataType::Int32),
        Some("t2"),
        Some(1),
    );
    let t1_name = create_column_ref1(
        1,
        "name",
        DataType::Number(NumberDataType::Int32),
        Some("t1"),
        Some(0),
    );
    let t2_name = create_column_ref1(
        1,
        "name",
        DataType::Number(NumberDataType::Int32),
        Some("t2"),
        Some(1),
    );

    // Create table scans
    let t1 = create_table_scan(0, "t1");
    let t2 = create_table_scan(1, "t2");

    // Create join conditions - note that we have a duplicate condition (t1.id = t2.id appears twice)
    let cond_t1_t2_id = create_join_condition(t1_id.clone(), t2_id.clone(), false);
    let cond_t1_t2_name = create_join_condition(t1_name.clone(), t2_name.clone(), false);
    let cond_t1_t2_id_duplicate = create_join_condition(t1_id.clone(), t2_id.clone(), false);

    // Create the join tree with multiple conditions including a duplicate
    let join_tree = create_join(
        t1,
        t2,
        vec![cond_t1_t2_id, cond_t1_t2_name, cond_t1_t2_id_duplicate],
        JoinType::Inner,
    );

    // Define expected before optimization patterns
    let before_patterns = [r#"
    Join [t1.id = t2.id, t1.name = t2.name, t1.id = t2.id]
      Table t0
      Table t1
    "#];

    // Define expected after optimization patterns
    let after_patterns = [r#"
    Join [t1.id = t2.id, t1.name = t2.name]
      Table t0
      Table t1
    "#];

    // Run the optimizer
    let optimized = run_optimizer(join_tree.clone())?;

    // Compare trees before and after optimization
    compare_trees(&join_tree, &optimized, &before_patterns, &after_patterns)?;

    Ok(())
}

// Test case for empty join conditions
// For example: SELECT * FROM t1 INNER JOIN t2
//
// Before optimization:
//    Join []
//    /  \
//   t1  t2
//
// After optimization (no change):
//    Join []
//    /  \
//   t1  t2
//
// Optimization: No change since there are no conditions to deduplicate
#[test]
fn test_empty_conditions() -> Result<()> {
    // Create table scans
    let t1 = create_table_scan(0, "t1");
    let t2 = create_table_scan(1, "t2");

    // Create a join with no conditions
    let join = create_join(t1, t2, vec![], JoinType::Inner);

    // Define expected before optimization patterns
    let before_patterns = [
        // Before optimization, join has no conditions
        r#"
Join []
  Table t0
  Table t1
"#,
    ];

    // Define expected after optimization patterns
    let after_patterns = [
        // After optimization, join still has no conditions
        r#"
Join []
  Table t0
  Table t1
"#,
    ];

    // Run the optimizer
    let optimized = run_optimizer(join.clone())?;

    // Compare trees before and after optimization
    compare_trees(&join, &optimized, &before_patterns, &after_patterns)?;

    Ok(())
}

// Test case for duplicate identical join conditions
// For example: SELECT * FROM t1, t2 WHERE t1.id = t2.id AND t1.id = t2.id
//
// Before optimization:
//    Join [t1.id = t2.id, t1.id = t2.id]
//    /  \
//   t1  t2
//
// After optimization:
//    Join [t1.id = t2.id]
//    /  \
//   t1  t2
//
// Optimization: Removes duplicate identical join condition
#[test]
fn test_duplicate_identical_conditions() -> Result<()> {
    // Create column references
    let t1_id = create_column_ref1(
        0,
        "id",
        DataType::Number(NumberDataType::Int64),
        Some("t1"),
        Some(0),
    );
    let t2_id = create_column_ref1(
        1,
        "id",
        DataType::Number(NumberDataType::Int64),
        Some("t2"),
        Some(1),
    );

    // Create table scans
    let t1 = create_table_scan(0, "t1");
    let t2 = create_table_scan(1, "t2");

    // Create identical join conditions
    let cond1 = create_join_condition(t1_id.clone(), t2_id.clone(), false);
    let cond2 = create_join_condition(t1_id.clone(), t2_id.clone(), false); // Duplicate

    // Create a join with duplicate conditions
    let join = create_join(t1, t2, vec![cond1, cond2], JoinType::Inner);

    // Define expected before optimization patterns
    let before_patterns = [
        // Before optimization, join has duplicate conditions
        r#"
Join [t1.id = t2.id, t1.id = t2.id]
  Table t0
  Table t1
"#,
    ];

    // Define expected after optimization patterns
    let after_patterns = [
        // After optimization, duplicate conditions are removed
        r#"
Join [t1.id = t2.id]
  Table t0
  Table t1
"#,
    ];

    // Run the optimizer
    let optimized = run_optimizer(join.clone())?;

    // Compare trees before and after optimization
    compare_trees(&join, &optimized, &before_patterns, &after_patterns)?;

    Ok(())
}

// Test case for commutative and circular join conditions
// For example: SELECT * FROM t1, t2, t3 WHERE t1.id = t2.id AND t2.id = t3.id AND t3.id = t1.id
// The last condition is redundant but written in reverse order
//
// Before optimization:
//    Join [t2.id = t3.id, t3.id = t1.id]
//    /  \
//   /    \
// Join [t1.id = t2.id]  t3
// /  \
// t1  t2
//
// After optimization (the optimizer correctly recognizes the redundancy):
//    Join [t2.id = t3.id]
//    /  \
//   /    \
// Join [t1.id = t2.id]  t3
// /  \
// t1  t2
//
// Optimization: Removes redundant condition (t3.id = t1.id) that creates a circular dependency
// since it can be inferred from the transitive relationship
#[test]
fn test_commutative_and_circular_conditions() -> Result<()> {
    // Create column references
    let t1_id = create_column_ref1(
        0,
        "id",
        DataType::Number(NumberDataType::Int64),
        Some("t1"),
        Some(0),
    );
    let t2_id = create_column_ref1(
        1,
        "id",
        DataType::Number(NumberDataType::Int64),
        Some("t2"),
        Some(1),
    );
    let t3_id = create_column_ref1(
        2,
        "id",
        DataType::Number(NumberDataType::Int64),
        Some("t3"),
        Some(2),
    );

    // Create table scans
    let t1 = create_table_scan(0, "t1");
    let t2 = create_table_scan(1, "t2");
    let t3 = create_table_scan(2, "t3");

    // Create join conditions with the last one in reverse order
    let cond_t1_t2 = create_join_condition(t1_id.clone(), t2_id.clone(), false);
    let cond_t2_t3 = create_join_condition(t2_id.clone(), t3_id.clone(), false);
    let cond_t3_t1 = create_join_condition(t3_id.clone(), t1_id.clone(), false); // Same as t1.id = t3.id but reversed

    // Create a join tree
    let join_t1_t2 = create_join(t1, t2, vec![cond_t1_t2], JoinType::Inner);
    let join_tree = create_join(
        join_t1_t2,
        t3,
        vec![cond_t2_t3, cond_t3_t1],
        JoinType::Inner,
    );

    // Define expected before optimization patterns
    let before_patterns = [r#"
Join [t2.id = t3.id, t3.id = t1.id]
  Join [t1.id = t2.id]
    Table t0
    Table t1
  Table t2
"#];

    // Define expected after optimization patterns
    // The optimizer correctly recognizes commutative conditions and removes redundant ones
    let after_patterns = [r#"
Join [t2.id = t3.id]
  Join [t1.id = t2.id]
    Table t0
    Table t1
  Table t2
"#];

    // Run the optimizer
    let optimized = run_optimizer(join_tree.clone())?;

    // Compare trees before and after optimization
    compare_trees(&join_tree, &optimized, &before_patterns, &after_patterns)?;

    Ok(())
}

// Test case for deep nested join trees
// This tests the robustness of the Union-Find implementation with deep nesting
//
// Before optimization:
//                        Join [t4.id = t5.id, t1.id = t5.id]
//                        /  \
//                       /    \
//                      /      \
//            Join [t3.id = t4.id]   t5
//            /  \
//           /    \
//          /      \
//  Join [t2.id = t3.id]   t4
//  /  \
// /    \
// Join [t1.id = t2.id]  t3
// /  \
// t1  t2
//
// After optimization:
//                        Join [t4.id = t5.id]
//                        /  \
//                       /    \
//                      /      \
//            Join [t3.id = t4.id]   t5
//            /  \
//           /    \
//          /      \
//  Join [t2.id = t3.id]   t4
//  /  \
// /    \
// Join [t1.id = t2.id]  t3
// /  \
// t1  t2
//
// Optimization: Removes redundant condition (t1.id = t5.id) in a deeply nested join tree
// since it can be inferred from the chain of transitive relationships
#[test]
fn test_deep_nested_join_tree() -> Result<()> {
    // Create column references for 5 tables
    let t1_id = create_column_ref1(
        0,
        "id",
        DataType::Number(NumberDataType::Int64),
        Some("t1"),
        Some(0),
    );
    let t2_id = create_column_ref1(
        1,
        "id",
        DataType::Number(NumberDataType::Int64),
        Some("t2"),
        Some(1),
    );
    let t3_id = create_column_ref1(
        2,
        "id",
        DataType::Number(NumberDataType::Int64),
        Some("t3"),
        Some(2),
    );
    let t4_id = create_column_ref1(
        3,
        "id",
        DataType::Number(NumberDataType::Int64),
        Some("t4"),
        Some(3),
    );
    let t5_id = create_column_ref1(
        4,
        "id",
        DataType::Number(NumberDataType::Int64),
        Some("t5"),
        Some(4),
    );

    // Create table scans
    let t1 = create_table_scan(0, "t1");
    let t2 = create_table_scan(1, "t2");
    let t3 = create_table_scan(2, "t3");
    let t4 = create_table_scan(3, "t4");
    let t5 = create_table_scan(4, "t5");

    // Create join conditions
    let cond_t1_t2 = create_join_condition(t1_id.clone(), t2_id.clone(), false);
    let cond_t2_t3 = create_join_condition(t2_id.clone(), t3_id.clone(), false);
    let cond_t3_t4 = create_join_condition(t3_id.clone(), t4_id.clone(), false);
    let cond_t4_t5 = create_join_condition(t4_id.clone(), t5_id.clone(), false);
    let cond_t1_t5 = create_join_condition(t1_id.clone(), t5_id.clone(), false); // Redundant due to transitivity

    // Create a deeply nested join tree: ((((t1 JOIN t2) JOIN t3) JOIN t4) JOIN t5)
    let join_t1_t2 = create_join(t1, t2, vec![cond_t1_t2], JoinType::Inner);
    let join_t1_t2_t3 = create_join(join_t1_t2, t3, vec![cond_t2_t3], JoinType::Inner);
    let join_t1_t2_t3_t4 = create_join(join_t1_t2_t3, t4, vec![cond_t3_t4], JoinType::Inner);
    let join_tree = create_join(
        join_t1_t2_t3_t4,
        t5,
        vec![cond_t4_t5, cond_t1_t5],
        JoinType::Inner,
    );

    // Define expected before optimization patterns
    let before_patterns = [r#"
Join [t4.id = t5.id, t1.id = t5.id]
  Join [t3.id = t4.id]
    Join [t2.id = t3.id]
      Join [t1.id = t2.id]
        Table t0
        Table t1
      Table t2
    Table t3
  Table t4
"#];

    // Define expected after optimization patterns
    let after_patterns = [r#"
Join [t4.id = t5.id]
  Join [t3.id = t4.id]
    Join [t2.id = t3.id]
      Join [t1.id = t2.id]
        Table t0
        Table t1
      Table t2
    Table t3
  Table t4
"#];

    // Run the optimizer
    let optimized = run_optimizer(join_tree.clone())?;

    // Compare trees before and after optimization
    compare_trees(&join_tree, &optimized, &before_patterns, &after_patterns)?;

    Ok(())
}

// Test case for mixed join types with redundant conditions
// For example: SELECT * FROM t1 INNER JOIN t2 ON t1.id = t2.id LEFT JOIN t3
//              ON t2.id = t3.id AND t1.id = t3.id
//
// Before optimization:
//    Left Join [t2.id = t3.id, t1.id = t3.id]
//    /  \
//   /    \
// Inner Join [t1.id = t2.id]  t3
// /  \
// t1  t2
//
// After optimization:
//    Left Join [t2.id = t3.id, t1.id = t3.id]  (no change for outer join)
//    /  \
//   /    \
// Inner Join [t1.id = t2.id]  t3
// /  \
// t1  t2
//
// Optimization: No change in the LEFT JOIN conditions since removing conditions from
// non-inner joins could change semantics, but inner join conditions are optimized
#[test]
fn test_mixed_join_types() -> Result<()> {
    // Create column references
    let t1_id = create_column_ref1(
        0,
        "id",
        DataType::Number(NumberDataType::Int64),
        Some("t1"),
        Some(0),
    );
    let t2_id = create_column_ref1(
        1,
        "id",
        DataType::Number(NumberDataType::Int64),
        Some("t2"),
        Some(1),
    );
    let t3_id = create_column_ref1(
        2,
        "id",
        DataType::Number(NumberDataType::Int64),
        Some("t3"),
        Some(2),
    );

    // Create table scans
    let t1 = create_table_scan(0, "t1");
    let t2 = create_table_scan(1, "t2");
    let t3 = create_table_scan(2, "t3");

    // Create join conditions
    let cond_t1_t2 = create_join_condition(t1_id.clone(), t2_id.clone(), false);
    let cond_t2_t3 = create_join_condition(t2_id.clone(), t3_id.clone(), false);
    let cond_t1_t3 = create_join_condition(t1_id.clone(), t3_id.clone(), false); // Redundant

    // Create a mixed join tree: (t1 INNER JOIN t2) LEFT JOIN t3
    let join_t1_t2 = create_join(t1, t2, vec![cond_t1_t2], JoinType::Inner);
    let join_tree = create_join(join_t1_t2, t3, vec![cond_t2_t3, cond_t1_t3], JoinType::Left);

    // Define expected before optimization patterns
    let before_patterns = [
        // Before optimization, we have both conditions in the LEFT JOIN
        r#"
Join [t2.id = t3.id, t1.id = t3.id]
  Join [t1.id = t2.id]
    Table t0
    Table t1
  Table t2
"#,
    ];

    // Define expected after optimization patterns
    let after_patterns = [
        // After optimization, conditions in LEFT JOIN should be preserved
        r#"
Join [t2.id = t3.id, t1.id = t3.id]
  Join [t1.id = t2.id]
    Table t0
    Table t1
  Table t2
"#,
    ];

    // Run the optimizer
    let optimized = run_optimizer(join_tree.clone())?;

    // Compare trees before and after optimization
    compare_trees(&join_tree, &optimized, &before_patterns, &after_patterns)?;

    Ok(())
}
