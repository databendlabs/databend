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

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_base::base::OrderedFloat;
use databend_common_catalog::catalog::CatalogManager;
use databend_common_exception::Result;
use databend_common_expression::Scalar;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberScalar;
use databend_common_sql::ColumnBinding;
use databend_common_sql::ColumnSet;
use databend_common_sql::IndexType;
use databend_common_sql::NameResolutionContext;
use databend_common_sql::Planner;
use databend_common_sql::Visibility;
use databend_common_sql::optimizer::ir::SExpr;
use databend_common_sql::planner::Binder;
use databend_common_sql::planner::Metadata;
use databend_common_sql::planner::plans::Filter;
use databend_common_sql::plans::Aggregate;
use databend_common_sql::plans::AggregateMode;
use databend_common_sql::plans::BoundColumnRef;
use databend_common_sql::plans::ComparisonOp;
use databend_common_sql::plans::ConstantExpr;
use databend_common_sql::plans::FunctionCall;
use databend_common_sql::plans::Join;
use databend_common_sql::plans::JoinEquiCondition;
use databend_common_sql::plans::JoinType;
use databend_common_sql::plans::Limit;
use databend_common_sql::plans::Plan;
use databend_common_sql::plans::RelOperator;
use databend_common_sql::plans::ScalarExpr;
use databend_common_sql::plans::ScalarItem;
use databend_common_sql::plans::Scan;
use databend_common_storages_fuse::TableContext;
use databend_query::interpreters::InterpreterFactory;
use databend_query::sessions::QueryContext;

// TPC-DS Test Utilities

/// Plan SQL query to get a Plan object
pub async fn plan_sql(ctx: &Arc<QueryContext>, sql: &str) -> Result<Plan> {
    let mut planner = Planner::new(ctx.clone());
    let (plan, _) = planner.plan_sql(sql).await?;

    Ok(plan)
}

/// Execute SQL statement
pub async fn execute_sql(ctx: &Arc<QueryContext>, sql: &str) -> Result<()> {
    let plan = plan_sql(ctx, sql).await?;
    let it = InterpreterFactory::get(ctx.clone(), &plan).await?;
    let _ = it.execute(ctx.clone()).await?;
    Ok(())
}

/// Get raw plan from SQL
pub async fn raw_plan(ctx: &Arc<QueryContext>, sql: &str) -> Result<Plan> {
    let settings = ctx.get_settings();
    let planner = Planner::new(ctx.clone());
    let extras = planner.parse_sql(sql)?;

    let metadata = Arc::new(parking_lot::RwLock::new(Metadata::default()));
    let name_resolution_ctx = NameResolutionContext::try_from(settings.as_ref())?;

    let binder = Binder::new(
        ctx.clone(),
        CatalogManager::instance(),
        name_resolution_ctx,
        metadata.clone(),
    );

    binder.bind(&extras.statement).await
}

// ===== Helper Functions =====

/// Creates a column reference with the given index, name, data type, table name and table index
pub fn create_table_bound_column_ref(
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
        is_srf: false,
    };
    ScalarExpr::BoundColumnRef(BoundColumnRef { column, span: None })
}

/// Creates a column reference with the given name
pub fn create_column_function_call(name: &str) -> ScalarExpr {
    ScalarExpr::FunctionCall(FunctionCall {
        span: None,
        func_name: "column".to_string(),
        params: vec![],
        arguments: vec![create_constant_string(name)],
    })
}

/// Creates an integer constant expression
pub fn create_int_constant(value: i64) -> ScalarExpr {
    ScalarExpr::ConstantExpr(ConstantExpr {
        value: Scalar::Number(NumberScalar::Int64(value)),
        span: None,
    })
}

/// Creates a float constant expression
pub fn create_float_constant(value: f64) -> ScalarExpr {
    ScalarExpr::ConstantExpr(ConstantExpr {
        value: Scalar::Number(NumberScalar::Float64(OrderedFloat(value))),
        span: None,
    })
}

/// Creates a constant string value
pub fn create_constant_string(value: &str) -> ScalarExpr {
    ScalarExpr::ConstantExpr(ConstantExpr {
        span: None,
        value: Scalar::String(value.to_string()),
    })
}

// Creates a constant integer value
// Removed unused function create_constant_int

/// Creates a constant boolean value
pub fn create_constant_bool(value: bool) -> ScalarExpr {
    ScalarExpr::ConstantExpr(ConstantExpr {
        span: None,
        value: Scalar::Boolean(value),
    })
}

/// Extracts the function name from a scalar expression
pub fn get_function_name(expr: &ScalarExpr) -> Option<&str> {
    if let ScalarExpr::FunctionCall(func) = expr {
        Some(&func.func_name)
    } else {
        None
    }
}

/// Extracts the column index from a scalar expression
pub fn get_column_index(expr: &ScalarExpr) -> Option<IndexType> {
    if let ScalarExpr::BoundColumnRef(col_ref) = expr {
        Some(col_ref.column.index)
    } else {
        None
    }
}

/// Extracts an integer value from a scalar expression
pub fn get_int_value(expr: &ScalarExpr) -> Option<i64> {
    if let ScalarExpr::ConstantExpr(constant) = expr {
        if let Scalar::Number(NumberScalar::Int64(value)) = constant.value {
            Some(value)
        } else {
            None
        }
    } else {
        None
    }
}

/// Extracts a boolean value from a scalar expression
pub fn get_bool_value(expr: &ScalarExpr) -> Option<bool> {
    if let ScalarExpr::ConstantExpr(constant) = expr {
        if let Scalar::Boolean(value) = constant.value {
            Some(value)
        } else {
            None
        }
    } else {
        None
    }
}

/// Finds a predicate with the given function name and column index
#[allow(dead_code)]
pub fn find_predicate(
    predicates: &[ScalarExpr],
    func_name: &str,
    col_index: IndexType,
    value: Option<i64>,
) -> bool {
    for pred in predicates {
        if let ScalarExpr::FunctionCall(func) = pred {
            if func.func_name != func_name {
                continue;
            }

            let left_index = get_column_index(&func.arguments[0]);
            if left_index != Some(col_index) {
                continue;
            }

            if let Some(expected_value) = value {
                let right_value = get_int_value(&func.arguments[1]);
                if right_value != Some(expected_value) {
                    continue;
                }
            }

            return true;
        }
    }
    false
}

/// Finds an equality predicate between two columns
#[allow(dead_code)]
fn find_equality_predicate(
    predicates: &[ScalarExpr],
    left_col_index: IndexType,
    right_col_index: IndexType,
) -> bool {
    for pred in predicates {
        if let ScalarExpr::FunctionCall(func) = pred {
            if func.func_name != "eq" {
                continue;
            }

            let left_index = get_column_index(&func.arguments[0]);
            let right_index = get_column_index(&func.arguments[1]);

            if (left_index == Some(left_col_index) && right_index == Some(right_col_index))
                || (left_index == Some(right_col_index) && right_index == Some(left_col_index))
            {
                return true;
            }
        }
    }
    false
}

/// Helper struct for building test expressions with a fluent API
pub struct ExprBuilder {
    // Cache for column references
    columns: HashMap<String, ScalarExpr>,
}

impl ExprBuilder {
    /// Create a new ExprBuilder
    pub fn new() -> Self {
        Self {
            columns: HashMap::new(),
        }
    }

    /// Checks if the result is a single boolean constant with the given value
    #[allow(dead_code)]
    pub fn is_boolean_constant(&self, result: &[ScalarExpr], value: bool) -> bool {
        if result.len() != 1 {
            return false;
        }

        get_bool_value(&result[0]) == Some(value)
    }

    /// Helper function to count predicates of a specific type
    #[allow(dead_code)]
    pub fn count_predicates(
        &self,
        predicates: &[ScalarExpr],
        func_name: &str,
        col_index: IndexType,
        value: Option<i64>,
    ) -> usize {
        predicates
            .iter()
            .filter(|expr| {
                if let ScalarExpr::FunctionCall(func) = expr {
                    func.func_name == func_name
                        && get_column_index(&func.arguments[0]) == Some(col_index)
                        && (value.is_none() || get_int_value(&func.arguments[1]) == value)
                } else {
                    false
                }
            })
            .count()
    }

    /// Find a predicate with the given function name, column index, and optional value
    pub fn find_predicate(
        &self,
        predicates: &[ScalarExpr],
        func_name: &str,
        col_index: IndexType,
        value: Option<i64>,
    ) -> bool {
        for pred in predicates {
            if let ScalarExpr::FunctionCall(func) = pred {
                if func.func_name != func_name {
                    continue;
                }

                let left_index = get_column_index(&func.arguments[0]);
                if left_index != Some(col_index) {
                    continue;
                }

                if let Some(expected_value) = value {
                    let right_value = get_int_value(&func.arguments[1]);
                    if right_value != Some(expected_value) {
                        continue;
                    }
                }

                return true;
            }
        }
        false
    }

    /// Create or retrieve a column reference with table context
    pub fn column(
        &mut self,
        key: &str,
        index: IndexType,
        name: &str,
        data_type: DataType,
        table_name: &str,
        table_index: IndexType,
    ) -> ScalarExpr {
        self.columns
            .entry(key.to_string())
            .or_insert_with(|| {
                create_table_bound_column_ref(
                    index,
                    name,
                    data_type,
                    Some(table_name),
                    Some(table_index),
                )
            })
            .clone()
    }

    /// Create a simple column reference without table context
    #[allow(dead_code)]
    pub fn simple_column(
        &mut self,
        key: &str,
        index: IndexType,
        name: &str,
        data_type: DataType,
    ) -> ScalarExpr {
        self.columns
            .entry(key.to_string())
            .or_insert_with(|| create_table_bound_column_ref(index, name, data_type, None, None))
            .clone()
    }

    /// Create a column by name using function call
    pub fn column_by_name(&self, name: &str) -> ScalarExpr {
        create_column_function_call(name)
    }

    /// Create an integer constant
    pub fn int(&self, value: i64) -> ScalarExpr {
        create_int_constant(value)
    }

    /// Create a float constant
    pub fn float(&self, value: f64) -> ScalarExpr {
        create_float_constant(value)
    }

    /// Create a string constant
    #[allow(dead_code)]
    pub fn string(&self, value: &str) -> ScalarExpr {
        create_constant_string(value)
    }

    /// Create a boolean constant
    pub fn bool(&self, value: bool) -> ScalarExpr {
        create_constant_bool(value)
    }

    /// Create a null constant
    pub fn null(&self) -> ScalarExpr {
        ScalarExpr::ConstantExpr(ConstantExpr {
            span: None,
            value: Scalar::Null,
        })
    }

    /// Create an equality comparison
    pub fn eq(&self, left: ScalarExpr, right: ScalarExpr) -> ScalarExpr {
        self.comparison(left, right, ComparisonOp::Equal)
    }

    /// Create a greater than comparison
    pub fn gt(&self, left: ScalarExpr, right: ScalarExpr) -> ScalarExpr {
        self.comparison(left, right, ComparisonOp::GT)
    }

    /// Create a less than comparison
    pub fn lt(&self, left: ScalarExpr, right: ScalarExpr) -> ScalarExpr {
        self.comparison(left, right, ComparisonOp::LT)
    }

    /// Create a greater than or equal comparison
    pub fn gte(&self, left: ScalarExpr, right: ScalarExpr) -> ScalarExpr {
        self.comparison(left, right, ComparisonOp::GTE)
    }

    /// Create a less than or equal comparison
    pub fn lte(&self, left: ScalarExpr, right: ScalarExpr) -> ScalarExpr {
        self.comparison(left, right, ComparisonOp::LTE)
    }

    /// Create a not equal comparison
    pub fn neq(&self, left: ScalarExpr, right: ScalarExpr) -> ScalarExpr {
        self.comparison(left, right, ComparisonOp::NotEqual)
    }

    /// Create an AND expression
    pub fn and(&self, left: ScalarExpr, right: ScalarExpr) -> ScalarExpr {
        ScalarExpr::FunctionCall(FunctionCall {
            span: None,
            func_name: "and".to_string(),
            params: vec![],
            arguments: vec![left, right],
        })
    }

    /// Create an OR expression
    pub fn or(&self, left: ScalarExpr, right: ScalarExpr) -> ScalarExpr {
        ScalarExpr::FunctionCall(FunctionCall {
            span: None,
            func_name: "or".to_string(),
            params: vec![],
            arguments: vec![left, right],
        })
    }

    /// Create a comparison expression with the given operator
    pub fn comparison(&self, left: ScalarExpr, right: ScalarExpr, op: ComparisonOp) -> ScalarExpr {
        let func_name = op.to_func_name().to_string();
        ScalarExpr::FunctionCall(FunctionCall {
            span: None,
            func_name,
            arguments: vec![left, right],
            params: vec![],
        })
    }

    /// Create an IF expression
    pub fn if_expr(
        &self,
        condition: ScalarExpr,
        then_expr: ScalarExpr,
        else_expr: ScalarExpr,
    ) -> ScalarExpr {
        ScalarExpr::FunctionCall(FunctionCall {
            span: None,
            func_name: "if".to_string(),
            arguments: vec![condition, then_expr, else_expr],
            params: vec![],
        })
    }

    /// Create a join
    pub fn join(
        &self,
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

    /// Create a join condition between two columns
    pub fn join_condition(
        &self,
        left: ScalarExpr,
        right: ScalarExpr,
        is_null_equal: bool,
    ) -> JoinEquiCondition {
        JoinEquiCondition::new(left, right, is_null_equal)
    }

    /// Create a filter
    pub fn filter(&self, input: SExpr, predicates: Vec<ScalarExpr>) -> SExpr {
        SExpr::create_unary(
            Arc::new(RelOperator::Filter(Filter { predicates })),
            Arc::new(input),
        )
    }

    /// Create a table scan
    pub fn table_scan(&self, table_index: usize, _name: &str) -> SExpr {
        let scan = Scan {
            table_index,
            ..Default::default()
        };
        SExpr::create_leaf(Arc::new(RelOperator::Scan(scan)))
    }

    /// Create a table scan with column indices
    pub fn table_scan_with_columns(
        &self,
        table_index: usize,
        _name: &str,
        columns: ColumnSet,
    ) -> SExpr {
        let scan = Scan {
            table_index,
            columns,
            ..Default::default()
        };
        SExpr::create_leaf(Arc::new(RelOperator::Scan(scan)))
    }

    /// Create an aggregate
    pub fn aggregate(
        &self,
        input: SExpr,
        group_items: Vec<ScalarItem>,
        aggregate_functions: Vec<ScalarItem>,
        mode: AggregateMode,
    ) -> SExpr {
        let agg = Aggregate {
            aggregate_functions,
            group_items,
            grouping_sets: None,
            mode,
            from_distinct: false,
            rank_limit: None,
        };
        SExpr::create_unary(Arc::new(RelOperator::Aggregate(agg)), Arc::new(input))
    }

    /// Create a limit
    pub fn limit(&self, input: SExpr, limit: usize, offset: usize) -> SExpr {
        let limit_op = Limit {
            limit: Some(limit),
            offset,
            before_exchange: false,
            lazy_columns: Default::default(),
        };
        SExpr::create_unary(Arc::new(RelOperator::Limit(limit_op)), Arc::new(input))
    }

    /// Create a scalar item
    pub fn scalar_item(&self, index: IndexType, scalar: ScalarExpr) -> ScalarItem {
        ScalarItem { index, scalar }
    }
}
