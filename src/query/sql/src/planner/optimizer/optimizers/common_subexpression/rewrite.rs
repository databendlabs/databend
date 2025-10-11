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

use crate::optimizer::ir::SExpr;
use crate::plans::RelOperator;
use crate::plans::Sequence;

/// Replace a subtree at the specified path in the SExpr tree.
///
/// # Arguments
/// * `root` - The root SExpr to perform replacement on
/// * `path` - A slice of child indices specifying the path to the replacement position
/// * `replacement` - The new SExpr to replace the subtree at the specified position
///
/// # Returns
/// A new SExpr with the replacement performed, or an error if the path is invalid
///
/// # Example
/// If path is [0, 1], this will replace the second child (index 1) of the first child (index 0) of root.
pub fn replace_at_path(root: &SExpr, path: &[usize], replacement: Arc<SExpr>) -> Result<SExpr> {
    if path.is_empty() {
        // Replace the root itself
        return Ok((*replacement).clone());
    }

    let first_index = path[0];
    if first_index >= root.children.len() {
        return Err(ErrorCode::Internal(format!(
            "Invalid path in replace_at_path: path: {:?}, root: {:?}",
            path, root
        )));
    }

    // Recursively replace in the subtree
    let remaining_path = &path[1..];
    let old_child = &root.children[first_index];
    let new_child = Arc::new(replace_at_path(old_child, remaining_path, replacement)?);

    // Create new children with the replaced child
    let mut new_children = root.children.clone();
    new_children[first_index] = new_child;

    // Return a new SExpr with updated children
    Ok(root.replace_children(new_children))
}

pub fn wrap_with_sequence(materialized_cte: SExpr, s_expr: SExpr) -> SExpr {
    let sequence = Sequence;
    SExpr::create_binary(
        Arc::new(RelOperator::Sequence(sequence)),
        Arc::new(materialized_cte),
        Arc::new(s_expr),
    )
}

pub fn rewrite_sexpr(
    s_expr: &SExpr,
    replacements: Vec<SExprReplacement>,
    materialized_ctes: Vec<SExpr>,
) -> Result<SExpr> {
    let mut result = s_expr.clone();

    for replacement in replacements {
        result = replace_at_path(&result, &replacement.path, replacement.new_expr)?;
    }

    for cte_expr in materialized_ctes {
        result = wrap_with_sequence(cte_expr, result);
    }

    Ok(result)
}

/// Represents a single SExpr replacement operation
#[derive(Clone, Debug)]
pub struct SExprReplacement {
    /// Path to the location where replacement should occur
    pub path: Vec<usize>,
    /// The new expression to replace with
    pub new_expr: Arc<SExpr>,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use super::replace_at_path;
    use crate::optimizer::ir::SExpr;
    use crate::plans::RelOperator;
    use crate::plans::Scan;

    fn create_scan_expr(table_index: u32) -> SExpr {
        let scan = Scan {
            table_index: table_index as usize,
            ..Default::default()
        };
        SExpr::create_leaf(Arc::new(RelOperator::Scan(scan)))
    }

    fn create_join_expr(left: Arc<SExpr>, right: Arc<SExpr>) -> SExpr {
        use crate::plans::Join;
        use crate::plans::JoinType;

        let join = Join {
            equi_conditions: vec![],
            non_equi_conditions: vec![],
            join_type: JoinType::Cross,
            marker_index: None,
            from_correlated_subquery: false,
            need_hold_hash_table: false,
            is_lateral: false,
            single_to_inner: None,
            build_side_cache_info: None,
        };
        SExpr::create_binary(Arc::new(RelOperator::Join(join)), left, right)
    }

    #[test]
    fn test_replace_at_root() {
        let original = create_scan_expr(1);
        let replacement = Arc::new(create_scan_expr(2));

        let result = replace_at_path(&original, &[], replacement).unwrap();

        if let RelOperator::Scan(scan) = result.plan.as_ref() {
            assert_eq!(scan.table_index, 2);
        } else {
            panic!("Expected Scan operator");
        }
    }

    #[test]
    fn test_replace_first_child() {
        let left = Arc::new(create_scan_expr(1));
        let right = Arc::new(create_scan_expr(2));
        let original = create_join_expr(left, right);

        let replacement = Arc::new(create_scan_expr(3));
        let result = replace_at_path(&original, &[0], replacement).unwrap();

        // Check that the left child was replaced
        let new_left = result.child(0).unwrap();
        if let RelOperator::Scan(scan) = new_left.plan.as_ref() {
            assert_eq!(scan.table_index, 3);
        } else {
            panic!("Expected Scan operator");
        }

        // Check that the right child is unchanged
        let new_right = result.child(1).unwrap();
        if let RelOperator::Scan(scan) = new_right.plan.as_ref() {
            assert_eq!(scan.table_index, 2);
        } else {
            panic!("Expected Scan operator");
        }
    }

    #[test]
    fn test_replace_nested_path() {
        // Create a nested structure: Join(Join(Scan1, Scan2), Scan3)
        let scan1 = Arc::new(create_scan_expr(1));
        let scan2 = Arc::new(create_scan_expr(2));
        let inner_join = Arc::new(create_join_expr(scan1, scan2));
        let scan3 = Arc::new(create_scan_expr(3));
        let outer_join = create_join_expr(inner_join, scan3);

        // Replace the right child of the left child (path [0, 1])
        let replacement = Arc::new(create_scan_expr(4));
        let result = replace_at_path(&outer_join, &[0, 1], replacement).unwrap();

        // Navigate to the replaced position
        let left_child = result.child(0).unwrap();
        let replaced_child = left_child.child(1).unwrap();

        if let RelOperator::Scan(scan) = replaced_child.plan.as_ref() {
            assert_eq!(scan.table_index, 4);
        } else {
            panic!("Expected Scan operator");
        }

        // Check that other nodes are unchanged
        let left_left_child = left_child.child(0).unwrap();
        if let RelOperator::Scan(scan) = left_left_child.plan.as_ref() {
            assert_eq!(scan.table_index, 1);
        } else {
            panic!("Expected Scan operator");
        }
    }

    #[test]
    fn test_invalid_path_out_of_bounds() {
        let original = create_scan_expr(1);
        let replacement = Arc::new(create_scan_expr(2));

        let result = replace_at_path(&original, &[0], replacement);

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.to_string().contains("out of bounds"));
    }

    #[test]
    fn test_invalid_path_deep() {
        let left = Arc::new(create_scan_expr(1));
        let right = Arc::new(create_scan_expr(2));
        let original = create_join_expr(left, right);

        let replacement = Arc::new(create_scan_expr(3));
        let result = replace_at_path(&original, &[0, 0], replacement);

        assert!(result.is_err());
        let error = result.unwrap_err();
        assert!(error.to_string().contains("out of bounds"));
    }
}
