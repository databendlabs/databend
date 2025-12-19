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

#![allow(clippy::collapsible_if)]

use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_sql::optimizer::ir::AsyncSExprVisitor;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::optimizer::ir::SExprVisitor;
use databend_common_sql::optimizer::ir::VisitAction;
use databend_common_sql::plans::AggregateMode;
use databend_common_sql::plans::JoinType;
use databend_common_sql::plans::Limit;
use databend_common_sql::plans::Operator;
use databend_common_sql::plans::RelOperator;

use crate::sql::planner::optimizer::test_utils::ExprBuilder;

// Create a complex expression tree for testing
// The expression tree corresponds to the following SQL query:
// SELECT t1.id, SUM(t2.value) as total
// FROM table1 t1
// JOIN table2 t2 ON t1.id = t2.id
// WHERE t1.age > 18
// GROUP BY t1.id
// HAVING SUM(t2.value) > 100
// LIMIT 10;
//
// Expression tree structure:
// Limit(10)
//   └─ Filter(TRUE)  -- Simplified HAVING clause
//       └─ Aggregate([t1.id], [ScalarExpr])
//           └─ Filter(TRUE)  -- Simplified WHERE clause
//               └─ Join(INNER, t1.id = t2.id)
//                   ├─ DummyTableScan(table1)
//                   └─ DummyTableScan(table2)
fn create_complex_expr() -> SExpr {
    let mut builder = ExprBuilder::new();

    // Create two base table scans
    let scan1 = builder.table_scan(0, "table1");
    let scan2 = builder.table_scan(1, "table2");

    // Create join condition: t1.id = t2.id
    let t1_id = builder.column(
        "t1_id",
        0,
        "id",
        DataType::Number(NumberDataType::Int32),
        "t1",
        0,
    );
    let t2_id = builder.column(
        "t2_id",
        1,
        "id",
        DataType::Number(NumberDataType::Int32),
        "t2",
        1,
    );

    // Create join condition
    let join_condition = builder.join_condition(t1_id, t2_id, false);

    // Create JOIN node
    let joined = builder.join(scan1, scan2, vec![join_condition], JoinType::Inner);

    // Add WHERE filter condition (simplified as boolean constant)
    let where_condition = builder.bool(true);
    let filtered_join = builder.filter(joined, vec![where_condition]);

    // Add aggregation operation: GROUP BY t1.id
    let group_expr = builder.column(
        "t1_id_group",
        0,
        "id",
        DataType::Number(NumberDataType::Int32),
        "t1",
        0,
    );
    let group_item = builder.scalar_item(0, group_expr);

    // Create aggregate function item (simplified as constant expression)
    let agg_scalar = builder.bool(true);
    let agg_item = builder.scalar_item(1, agg_scalar);

    let aggregated = builder.aggregate(
        filtered_join,
        vec![group_item],
        vec![agg_item],
        AggregateMode::Initial,
    );

    // Add HAVING filter condition (simplified as boolean constant)
    let having_condition = builder.bool(true);
    let filtered_agg = builder.filter(aggregated, vec![having_condition]);

    // Add LIMIT 10
    builder.limit(filtered_agg, 10, 0)
}

// Test node traversal order and types
#[test]
fn test_node_traversal_order() {
    // Create a visitor that records the types of nodes visited
    struct NodeTracker {
        node_types: Vec<String>,
    }

    impl SExprVisitor for NodeTracker {
        fn visit(&mut self, expr: &SExpr) -> Result<VisitAction> {
            // Record the node type with debug format
            self.node_types
                .push(format!("{:?}", expr.plan.as_ref().rel_op()));
            Ok(VisitAction::Continue)
        }

        fn post_visit(&mut self, _expr: &SExpr) -> Result<VisitAction> {
            Ok(VisitAction::Continue)
        }
    }

    let expr = create_complex_expr();
    let mut tracker = NodeTracker {
        node_types: Vec::new(),
    };
    let result = expr.accept(&mut tracker);
    assert!(result.is_ok());

    // Verify that all 7 nodes were visited in the correct order
    assert_eq!(tracker.node_types.len(), 7);

    // Verify node types and traversal order (top-down, depth-first)
    let expected_types = [
        "Limit",
        "Filter",
        "Aggregate",
        "Filter",
        "Join",
        "Scan",
        "Scan",
    ];

    assert_eq!(tracker.node_types.len(), expected_types.len());
    for (i, expected) in expected_types.iter().enumerate() {
        assert_eq!(tracker.node_types[i], *expected);
    }
}

// Test synchronous visitor pattern
#[test]
fn test_sync_visitor() {
    // Create a simple visitor that counts the number of nodes in the expression tree
    struct NodeCounter {
        count: usize,
        node_types: Vec<String>,
    }

    impl SExprVisitor for NodeCounter {
        fn visit(&mut self, expr: &SExpr) -> Result<VisitAction> {
            self.count += 1;
            // Record node type
            self.node_types
                .push(format!("{:?}", expr.plan.as_ref().rel_op()));
            Ok(VisitAction::Continue)
        }

        fn post_visit(&mut self, _expr: &SExpr) -> Result<VisitAction> {
            Ok(VisitAction::Continue)
        }
    }

    let expr = create_complex_expr();
    let mut counter = NodeCounter {
        count: 0,
        node_types: Vec::new(),
    };
    let result = expr.accept(&mut counter);
    assert!(result.is_ok());

    // Our expression tree should have 7 nodes
    assert_eq!(counter.count, 7);

    // Verify node types and traversal order (top-down, depth-first)
    let expected_types = [
        "Limit",
        "Filter",
        "Aggregate",
        "Filter",
        "Join",
        "Scan",
        "Scan",
    ];

    assert_eq!(counter.node_types.len(), expected_types.len());
    for (i, expected) in expected_types.iter().enumerate() {
        assert_eq!(counter.node_types[i], *expected);
    }
}

// Test node replacement functionality
#[test]
fn test_replace_node() {
    // Create a simple visitor that doubles the limit value of all Limit nodes
    struct LimitDoubler {
        visit_count: usize,
        node_types: Vec<String>,
    }

    impl SExprVisitor for LimitDoubler {
        fn visit(&mut self, expr: &SExpr) -> Result<VisitAction> {
            self.visit_count += 1;
            // Record node type
            self.node_types
                .push(format!("{:?}", expr.plan.as_ref().rel_op()));
            let op = expr.plan();
            if let RelOperator::Limit(limit) = op {
                if let Some(limit_value) = limit.limit {
                    let new_limit = Limit {
                        limit: Some(limit_value * 2),
                        offset: limit.offset,
                        before_exchange: limit.before_exchange,
                        lazy_columns: limit.lazy_columns.clone(),
                    };
                    let replacement = SExpr::create_unary(
                        Arc::new(RelOperator::Limit(new_limit)),
                        Arc::new(expr.child(0).unwrap().clone()),
                    );
                    return Ok(VisitAction::Replace(replacement));
                }
            }
            Ok(VisitAction::Continue)
        }

        fn post_visit(&mut self, _expr: &SExpr) -> Result<VisitAction> {
            Ok(VisitAction::Continue)
        }
    }

    let expr = create_complex_expr();
    let mut transformer = LimitDoubler {
        visit_count: 0,
        node_types: Vec::new(),
    };
    let result = expr.accept(&mut transformer);
    assert!(result.is_ok());
    let transformed = result.unwrap().unwrap();

    // Verify that the root node was visited
    // When using Replace action, the traversal stops after replacement
    assert_eq!(transformer.visit_count, 1);

    // Verify node types and order
    assert_eq!(transformer.node_types.len(), 1);

    // Check if the Limit has been doubled
    let op = transformed.plan();
    if let RelOperator::Limit(limit) = op {
        assert_eq!(limit.limit, Some(20));
    } else {
        panic!("Root should be a Limit node");
    }
}

// Test asynchronous visitor pattern
#[tokio::test]
async fn test_async_visitor() {
    // Create a simple asynchronous visitor that counts the number of nodes in the expression tree
    struct AsyncNodeCounter {
        count: usize,
        node_types: Vec<String>,
    }

    #[async_trait::async_trait]
    impl AsyncSExprVisitor for AsyncNodeCounter {
        async fn visit(&mut self, expr: &SExpr) -> Result<VisitAction> {
            self.count += 1;
            // Record node type
            self.node_types
                .push(format!("{:?}", expr.plan.as_ref().rel_op()));
            Ok(VisitAction::Continue)
        }

        async fn post_visit(&mut self, _expr: &SExpr) -> Result<VisitAction> {
            Ok(VisitAction::Continue)
        }
    }

    let expr = create_complex_expr();
    let mut counter = AsyncNodeCounter {
        count: 0,
        node_types: Vec::new(),
    };
    let result = expr.accept_async(&mut counter).await;
    assert!(result.is_ok());

    // Our expression tree should have 7 nodes
    assert_eq!(counter.count, 7);

    // Verify node types and traversal order (top-down, depth-first)
    let expected_types = [
        "Limit",
        "Filter",
        "Aggregate",
        "Filter",
        "Join",
        "Scan",
        "Scan",
    ];

    assert_eq!(counter.node_types.len(), expected_types.len());

    for (i, expected) in expected_types.iter().enumerate() {
        assert_eq!(counter.node_types[i], *expected);
    }
}

// Test asynchronous node replacement functionality
#[tokio::test]
async fn test_async_replace_node() {
    // Create a simple asynchronous visitor that doubles the limit value of all Limit nodes
    struct AsyncLimitDoubler {
        visit_count: usize,
        node_types: Vec<String>,
    }

    #[async_trait::async_trait]
    impl AsyncSExprVisitor for AsyncLimitDoubler {
        async fn visit(&mut self, expr: &SExpr) -> Result<VisitAction> {
            self.visit_count += 1;
            // Record node type
            self.node_types
                .push(format!("{:?}", expr.plan.as_ref().rel_op()));
            let op = expr.plan();
            if let RelOperator::Limit(limit) = op {
                if let Some(limit_value) = limit.limit {
                    let new_limit = Limit {
                        limit: Some(limit_value * 2),
                        offset: limit.offset,
                        before_exchange: limit.before_exchange,
                        lazy_columns: limit.lazy_columns.clone(),
                    };
                    let replacement = SExpr::create_unary(
                        Arc::new(RelOperator::Limit(new_limit)),
                        Arc::new(expr.child(0).unwrap().clone()),
                    );
                    return Ok(VisitAction::Replace(replacement));
                }
            }
            Ok(VisitAction::Continue)
        }

        async fn post_visit(&mut self, _expr: &SExpr) -> Result<VisitAction> {
            Ok(VisitAction::Continue)
        }
    }

    let expr = create_complex_expr();
    let mut transformer = AsyncLimitDoubler {
        visit_count: 0,
        node_types: Vec::new(),
    };
    let result = expr.accept_async(&mut transformer).await;
    assert!(result.is_ok());
    let transformed = result.unwrap().unwrap();

    // Verify that the root node was visited
    // When using Replace action, the traversal stops after replacement
    assert_eq!(transformer.visit_count, 1);

    // Verify node types and order
    assert_eq!(transformer.node_types.len(), 1);

    // Check if the Limit has been doubled
    let op = transformed.plan();
    if let RelOperator::Limit(limit) = op {
        assert_eq!(limit.limit, Some(20));
    } else {
        panic!("Root should be a Limit node");
    }
}
