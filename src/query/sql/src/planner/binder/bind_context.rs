// Copyright 2022 Datafuse Labs.
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

use std::collections::BTreeMap;
use std::hash::Hash;

use common_ast::ast::Query;
use common_ast::ast::TableAlias;
use common_catalog::plan::InternalColumn;
use common_exception::ErrorCode;
use common_exception::Result;
use common_exception::Span;
use common_expression::types::DataType;
use common_expression::ColumnId;
use common_expression::DataField;
use common_expression::DataSchemaRef;
use common_expression::DataSchemaRefExt;
use dashmap::DashMap;

use super::AggregateInfo;
use super::INTERNAL_COLUMN_FACTORY;
use crate::binder::window::WindowInfo;
use crate::normalize_identifier;
use crate::optimizer::SExpr;
use crate::plans::ScalarExpr;
use crate::ColumnSet;
use crate::IndexType;
use crate::MetadataRef;
use crate::NameResolutionContext;

/// Context of current expression, this is used to check if
/// the expression is valid in current context.
#[derive(Debug, Clone, Default)]
pub enum ExprContext {
    SelectClause,
    WhereClause,
    HavingClause,
    OrderByClause,
    LimitClause,

    InSetReturningFunction,
    InAggregateFunction,

    #[default]
    Unknown,
}

#[derive(Clone, Debug, Eq, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum Visibility {
    // Default for a column
    Visible,
    // Inner column of struct
    InVisible,
    // Consider the sql: `select * from t join t1 using(a)`.
    // The result should only contain one `a` column.
    // So we need make `t.a` or `t1.a` invisible in unqualified
    UnqualifiedWildcardInVisible,
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct ColumnBinding {
    /// Database name of this `ColumnBinding` in current context
    pub database_name: Option<String>,
    /// Table name of this `ColumnBinding` in current context
    pub table_name: Option<String>,
    /// Table index of this `ColumnBinding` in current context
    pub table_index: Option<IndexType>,
    /// Column name of this `ColumnBinding` in current context
    pub column_name: String,
    /// Column index of ColumnBinding
    pub index: IndexType,

    pub data_type: Box<DataType>,

    pub visibility: Visibility,
}

impl PartialEq for ColumnBinding {
    fn eq(&self, other: &Self) -> bool {
        self.index == other.index
    }
}

impl Eq for ColumnBinding {}

impl Hash for ColumnBinding {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.index.hash(state);
    }
}

#[derive(Clone, Debug, serde::Deserialize, serde::Serialize)]
pub struct InternalColumnBinding {
    /// Database name of this `InternalColumnBinding` in current context
    pub database_name: Option<String>,
    /// Table name of this `InternalColumnBinding` in current context
    pub table_name: Option<String>,
    /// Column index of InternalColumnBinding
    pub index: IndexType,

    pub internal_column: InternalColumn,
}

impl PartialEq for InternalColumnBinding {
    fn eq(&self, other: &Self) -> bool {
        self.index == other.index
    }
}

impl Eq for InternalColumnBinding {}

impl Hash for InternalColumnBinding {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.index.hash(state);
    }
}

#[derive(Debug, Clone)]
pub enum NameResolutionResult {
    Column(ColumnBinding),
    InternalColumn(InternalColumnBinding),
    Alias { alias: String, scalar: ScalarExpr },
}

/// `BindContext` stores all the free variables in a query and tracks the context of binding procedure.
#[derive(Clone, Debug)]
pub struct BindContext {
    pub parent: Option<Box<BindContext>>,

    pub columns: Vec<ColumnBinding>,

    // map internal column id to (table_index, column_index)
    pub bound_internal_columns: BTreeMap<ColumnId, (IndexType, IndexType)>,

    pub aggregate_info: AggregateInfo,

    pub windows: WindowInfo,

    /// True if there is aggregation in current context, which means
    /// non-grouping columns cannot be referenced outside aggregation
    /// functions, otherwise a grouping error will be raised.
    pub in_grouping: bool,

    pub ctes_map: Box<DashMap<String, CteInfo>>,

    /// If current binding table is a view, record its database and name.
    ///
    /// It's used to check if the view has a loop dependency.
    pub view_info: Option<(String, String)>,

    /// Set-returning functions in current context.
    /// The key is the `Expr::to_string` of the function.
    pub srfs: DashMap<String, ScalarExpr>,

    pub expr_context: ExprContext,
}

#[derive(Clone, Debug)]
pub struct CteInfo {
    pub columns_alias: Vec<String>,
    pub query: Query,
}

impl BindContext {
    pub fn new() -> Self {
        Self {
            parent: None,
            columns: Vec::new(),
            bound_internal_columns: BTreeMap::new(),
            aggregate_info: AggregateInfo::default(),
            windows: WindowInfo::default(),
            in_grouping: false,
            ctes_map: Box::new(DashMap::new()),
            view_info: None,
            srfs: DashMap::new(),
            expr_context: ExprContext::default(),
        }
    }

    pub fn with_parent(parent: Box<BindContext>) -> Self {
        BindContext {
            parent: Some(parent.clone()),
            columns: vec![],
            bound_internal_columns: BTreeMap::new(),
            aggregate_info: Default::default(),
            windows: Default::default(),
            in_grouping: false,
            ctes_map: parent.ctes_map.clone(),
            view_info: None,
            srfs: DashMap::new(),
            expr_context: ExprContext::default(),
        }
    }

    /// Create a new BindContext with self's parent as its parent
    pub fn replace(&self) -> Self {
        let mut bind_context = BindContext::new();
        bind_context.parent = self.parent.clone();
        bind_context.ctes_map = self.ctes_map.clone();
        bind_context
    }

    /// Generate a new BindContext and take current BindContext as its parent.
    pub fn push(self) -> Self {
        Self::with_parent(Box::new(self))
    }

    /// Returns all column bindings in current scope.
    pub fn all_column_bindings(&self) -> &[ColumnBinding] {
        &self.columns
    }

    pub fn add_column_binding(&mut self, column_binding: ColumnBinding) {
        self.columns.push(column_binding);
    }

    /// Apply table alias like `SELECT * FROM t AS t1(a, b, c)`.
    /// This method will rename column bindings according to table alias.
    pub fn apply_table_alias(
        &mut self,
        alias: &TableAlias,
        name_resolution_ctx: &NameResolutionContext,
    ) -> Result<()> {
        for column in self.columns.iter_mut() {
            column.database_name = None;
            column.table_name = Some(normalize_identifier(&alias.name, name_resolution_ctx).name);
        }

        if alias.columns.len() > self.columns.len() {
            return Err(ErrorCode::SemanticError(format!(
                "table has {} columns available but {} columns specified",
                self.columns.len(),
                alias.columns.len()
            )));
        }
        for (index, column_name) in alias
            .columns
            .iter()
            .map(|ident| normalize_identifier(ident, name_resolution_ctx).name)
            .enumerate()
        {
            self.columns[index].column_name = column_name;
        }
        Ok(())
    }

    /// Try to find a column binding with given table name and column name.
    /// This method will return error if the given names are ambiguous or invalid.
    pub fn resolve_name(
        &self,
        database: Option<&str>,
        table: Option<&str>,
        column: &str,
        span: Span,
        available_aliases: &[(String, ScalarExpr)],
    ) -> Result<NameResolutionResult> {
        let mut result = vec![];

        let mut bind_context: &BindContext = self;
        // Lookup parent context to resolve outer reference.
        loop {
            // TODO(leiysky): use `Identifier` for alias instead of raw string
            for (alias, scalar) in available_aliases {
                if database.is_none() && table.is_none() && column == alias {
                    result.push(NameResolutionResult::Alias {
                        alias: alias.clone(),
                        scalar: scalar.clone(),
                    });
                }
            }

            // We will lookup alias first. If there are matched aliases, we will skip
            // looking up `BindContext` to avoid ambiguity.
            if !result.is_empty() {
                break;
            }

            for column_binding in bind_context.columns.iter() {
                if Self::match_column_binding(database, table, column, column_binding) {
                    result.push(NameResolutionResult::Column(column_binding.clone()));
                }
            }
            if !result.is_empty() {
                break;
            }

            // look up internal column
            if let Some(internal_column) = INTERNAL_COLUMN_FACTORY.get_internal_column(column) {
                let column_binding = InternalColumnBinding {
                    database_name: database.map(|n| n.to_owned()),
                    table_name: table.map(|n| n.to_owned()),
                    index: bind_context.columns.len(),
                    internal_column,
                };
                result.push(NameResolutionResult::InternalColumn(column_binding));
                break;
            }

            if let Some(ref parent) = bind_context.parent {
                bind_context = parent;
            } else {
                break;
            }
        }

        if result.is_empty() {
            Err(ErrorCode::SemanticError(format!("column {column} doesn't exist")).set_span(span))
        } else if result.len() > 1 {
            Err(ErrorCode::SemanticError(format!(
                "column {column} reference is ambiguous, got {result:?}"
            ))
            .set_span(span))
        } else {
            Ok(result.remove(0))
        }
    }

    pub fn match_column_binding(
        database: Option<&str>,
        table: Option<&str>,
        column: &str,
        column_binding: &ColumnBinding,
    ) -> bool {
        match (
            (database, column_binding.database_name.as_ref()),
            (table, column_binding.table_name.as_ref()),
        ) {
            // No qualified table name specified
            ((None, _), (None, None)) | ((None, _), (None, Some(_)))
                if column == column_binding.column_name =>
            {
                column_binding.visibility != Visibility::UnqualifiedWildcardInVisible
            }

            // Qualified column reference without database name
            ((None, _), (Some(table), Some(table_name)))
                if table == table_name && column == column_binding.column_name =>
            {
                true
            }

            // Qualified column reference with database name
            ((Some(db), Some(db_name)), (Some(table), Some(table_name)))
                if db == db_name && table == table_name && column == column_binding.column_name =>
            {
                true
            }
            _ => false,
        }
    }

    /// Get result columns of current context in order.
    /// For example, a query `SELECT b, a AS b FROM t` has `[(index_of(b), "b"), index_of(a), "b"]` as
    /// its result columns.
    ///
    /// This method is used to retrieve the physical representation of result set of
    /// a query.
    pub fn result_columns(&self) -> Vec<(IndexType, String)> {
        self.columns
            .iter()
            .map(|col| (col.index, col.column_name.clone()))
            .collect()
    }

    /// Return data scheme.
    pub fn output_schema(&self) -> DataSchemaRef {
        let fields = self
            .columns
            .iter()
            .map(|column_binding| {
                DataField::new(
                    &column_binding.column_name,
                    *column_binding.data_type.clone(),
                )
            })
            .collect();
        DataSchemaRefExt::create(fields)
    }

    fn get_internal_column_table_index(
        column_binding: &InternalColumnBinding,
        metadata: MetadataRef,
    ) -> (IndexType, Option<String>, Option<String>) {
        let metadata = metadata.read();
        let (database_name, table_name) =
            match (&column_binding.database_name, &column_binding.table_name) {
                (Some(database_name), Some(table_name)) => {
                    (Some(database_name.clone()), Some(table_name.clone()))
                }
                (None, Some(table_name)) => (None, Some(table_name.clone())),
                (database_name, None) => {
                    // If table_name is None, assert that metadata.tables has only one table
                    debug_assert!(metadata.tables().len() == 1);
                    return (metadata.table(0).index(), database_name.clone(), None);
                }
            };

        (
            metadata
                .get_table_index(
                    database_name.as_deref(),
                    table_name.as_ref().unwrap().as_str(),
                )
                .unwrap(),
            database_name,
            table_name,
        )
    }

    // Add internal column binding into `BindContext`
    // Convert `InternalColumnBinding` to `ColumnBinding`
    pub fn add_internal_column_binding(
        &mut self,
        column_binding: &InternalColumnBinding,
        metadata: MetadataRef,
    ) {
        let column_id = column_binding.internal_column.column_id();
        if let std::collections::btree_map::Entry::Vacant(e) =
            self.bound_internal_columns.entry(column_id)
        {
            // New added internal column MUST at the end of `columns` array.
            debug_assert_eq!(column_binding.index, self.columns.len());

            let (table_index, database_name, table_name) =
                BindContext::get_internal_column_table_index(column_binding, metadata.clone());

            let mut metadata = metadata.write();
            metadata.add_internal_column(table_index, column_binding.internal_column.clone());
            self.columns.push(ColumnBinding {
                database_name,
                table_name,
                table_index: Some(table_index),
                column_name: column_binding.internal_column.column_name().clone(),
                index: column_binding.index,
                data_type: Box::new(column_binding.internal_column.data_type()),
                visibility: Visibility::Visible,
            });

            e.insert((table_index, column_binding.index));
        }
    }

    pub fn add_internal_column_into_expr(&self, s_expr: SExpr) -> SExpr {
        let bound_internal_columns = &self.bound_internal_columns;
        let mut s_expr = s_expr;
        for (table_index, column_index) in bound_internal_columns.values() {
            s_expr = SExpr::add_internal_column_index(&s_expr, *table_index, *column_index);
        }
        s_expr
    }

    pub fn column_set(&self) -> ColumnSet {
        self.columns.iter().map(|c| c.index).collect()
    }

    pub fn set_expr_context(&mut self, expr_context: ExprContext) {
        self.expr_context = expr_context;
    }
}

impl Default for BindContext {
    fn default() -> Self {
        BindContext::new()
    }
}
