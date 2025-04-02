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

use std::collections::btree_map;
use std::collections::BTreeMap;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::collections::HashSet;
use std::hash::Hash;

use dashmap::DashMap;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::Query;
use databend_common_ast::ast::TableAlias;
use databend_common_ast::ast::WindowSpec;
use databend_common_ast::Span;
use databend_common_catalog::plan::InternalColumn;
use databend_common_catalog::plan::InvertedIndexInfo;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::DataSchemaRefExt;
use databend_common_expression::TableDataType;
use enum_as_inner::EnumAsInner;
use indexmap::IndexMap;
use itertools::Itertools;

use super::AggregateInfo;
use super::INTERNAL_COLUMN_FACTORY;
use crate::binder::column_binding::ColumnBinding;
use crate::binder::project_set::SetReturningInfo;
use crate::binder::window::WindowInfo;
use crate::binder::ColumnBindingBuilder;
use crate::normalize_identifier;
use crate::plans::ScalarExpr;
use crate::ColumnSet;
use crate::IndexType;
use crate::MetadataRef;
use crate::NameResolutionContext;

/// Context of current expression, this is used to check if
/// the expression is valid in current context.
#[derive(Debug, Clone, Default, EnumAsInner)]
pub enum ExprContext {
    SelectClause,
    WhereClause,
    GroupClaue,
    HavingClause,
    QualifyClause,
    OrderByClause,
    LimitClause,

    InSetReturningFunction,
    InAggregateFunction,
    InLambdaFunction,
    InAsyncFunction,

    #[default]
    Unknown,
}

impl ExprContext {
    pub fn prefer_resolve_alias(&self) -> bool {
        !matches!(self, ExprContext::SelectClause | ExprContext::WhereClause)
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Hash, serde::Deserialize, serde::Serialize)]
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

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct InternalColumnBinding {
    /// Database name of this `InternalColumnBinding` in current context
    pub database_name: Option<String>,
    /// Table name of this `InternalColumnBinding` in current context
    pub table_name: Option<String>,

    pub internal_column: InternalColumn,
}

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum NameResolutionResult {
    Column(ColumnBinding),
    InternalColumn(InternalColumnBinding),
    Alias { alias: String, scalar: ScalarExpr },
}

#[derive(Default, Clone, PartialEq, Eq, Debug)]
pub struct VirtualColumnContext {
    /// Whether allow rewrite as virtual column and pushdown.
    pub allow_pushdown: bool,
    /// The table indics of the virtual column has been readded,
    /// used to avoid repeated reading
    pub table_indices: HashSet<IndexType>,
    /// Mapping: (table index) -> (derived virtual column indices)
    /// This is used to add virtual column indices to Scan plan
    pub virtual_column_indices: HashMap<IndexType, Vec<IndexType>>,
    /// Mapping: (table index) -> (virtual column names and data types)
    /// This is used to check whether the virtual column has be created
    pub virtual_column_names: HashMap<IndexType, HashMap<String, (TableDataType, ColumnId)>>,
    /// virtual column alias names
    pub virtual_columns: Vec<ColumnBinding>,
}

impl VirtualColumnContext {
    fn with_parent(parent: &VirtualColumnContext) -> VirtualColumnContext {
        VirtualColumnContext {
            allow_pushdown: parent.allow_pushdown,
            table_indices: HashSet::new(),
            virtual_column_indices: HashMap::new(),
            virtual_column_names: HashMap::new(),
            virtual_columns: Vec::new(),
        }
    }

    pub(crate) fn merge(&mut self, other: &VirtualColumnContext) {
        self.allow_pushdown = self.allow_pushdown || other.allow_pushdown;
        self.table_indices.extend(other.table_indices.clone());
        self.virtual_column_indices
            .extend(other.virtual_column_indices.clone());
        self.virtual_column_names
            .extend(other.virtual_column_names.clone());
        self.virtual_columns.extend(other.virtual_columns.clone());
    }
}

/// `BindContext` stores all the free variables in a query and tracks the context of binding procedure.
#[derive(Clone, Debug)]
pub struct BindContext {
    pub parent: Option<Box<BindContext>>,

    pub columns: Vec<ColumnBinding>,

    // map internal column: (table_index, column_id) -> column_index
    pub bound_internal_columns: BTreeMap<(IndexType, ColumnId), IndexType>,

    pub aggregate_info: AggregateInfo,

    pub windows: WindowInfo,

    /// Set-returning functions info in current context.
    pub srf_info: SetReturningInfo,

    pub cte_context: CteContext,

    /// True if there is aggregation in current context, which means
    /// non-grouping columns cannot be referenced outside aggregation
    /// functions, otherwise a grouping error will be raised.
    pub in_grouping: bool,

    /// If current binding table is a view, record its database and name.
    ///
    /// It's used to check if the view has a loop dependency.
    pub view_info: Option<(String, String)>,

    /// True if there is async function in current context, need rewrite.
    pub have_async_func: bool,
    /// True if there is udf script in current context, need rewrite.
    pub have_udf_script: bool,
    /// True if there is udf server in current context, need rewrite.
    pub have_udf_server: bool,

    pub inverted_index_map: Box<IndexMap<IndexType, InvertedIndexInfo>>,

    pub virtual_column_context: VirtualColumnContext,

    pub expr_context: ExprContext,

    /// If true, the query is planning for aggregate index.
    /// It's used to avoid infinite loop.
    pub planning_agg_index: bool,

    pub window_definitions: DashMap<String, WindowSpec>,
}

#[derive(Clone, Debug, Default)]
pub struct CteContext {
    /// If the `BindContext` is created from a CTE, record the cte name
    pub cte_name: Option<String>,
    pub cte_map: Box<IndexMap<String, CteInfo>>,
}

impl CteContext {
    // Merge two `CteContext` into one.
    pub fn merge(&mut self, other: CteContext) {
        let mut merged_cte_map = IndexMap::new();
        for (left_key, left_value) in self.cte_map.iter() {
            if let Some(right_value) = other.cte_map.get(left_key) {
                let mut merged_value = left_value.clone();
                if left_value.columns.is_empty() {
                    merged_value.columns = right_value.columns.clone()
                }
                merged_cte_map.insert(left_key.clone(), merged_value);
            }
        }
        self.cte_map = Box::new(merged_cte_map);
    }

    // Set cte context to current `BindContext`.
    pub fn set_cte_context(&mut self, cte_context: CteContext) {
        self.cte_map = cte_context.cte_map;
    }

    // Set cte context to current `BindContext`.
    pub fn set_cte_context_and_name(&mut self, cte_context: CteContext) {
        self.cte_map = cte_context.cte_map;
        self.cte_name = cte_context.cte_name;
    }
}

#[derive(Clone, Debug)]
pub struct CteInfo {
    pub columns_alias: Vec<String>,
    pub query: Query,
    pub materialized: bool,
    pub recursive: bool,
    pub cte_idx: IndexType,
    // If cte is materialized, save its columns
    pub columns: Vec<ColumnBinding>,
}

impl BindContext {
    pub fn new() -> Self {
        Self {
            parent: None,
            columns: Vec::new(),
            bound_internal_columns: BTreeMap::new(),
            aggregate_info: AggregateInfo::default(),
            windows: WindowInfo::default(),
            srf_info: SetReturningInfo::default(),
            cte_context: CteContext::default(),
            in_grouping: false,
            view_info: None,
            have_async_func: false,
            have_udf_script: false,
            have_udf_server: false,
            inverted_index_map: Box::default(),
            virtual_column_context: VirtualColumnContext::default(),
            expr_context: ExprContext::default(),
            planning_agg_index: false,
            window_definitions: DashMap::new(),
        }
    }

    pub fn depth(&self) -> usize {
        if let Some(ref p) = self.parent {
            return p.depth() + 1;
        }
        1
    }

    pub fn with_opt_parent(parent: Option<&BindContext>) -> Result<Self> {
        if let Some(p) = parent {
            Self::with_parent(p.clone())
        } else {
            Self::with_parent(Self::new())
        }
    }

    pub fn with_parent(parent: BindContext) -> Result<Self> {
        const MAX_DEPTH: usize = 4096;
        if parent.depth() >= MAX_DEPTH {
            return Err(ErrorCode::Internal(
                "Query binder exceeds the maximum iterations",
            ));
        }

        Ok(BindContext {
            parent: Some(Box::new(parent.clone())),
            columns: vec![],
            bound_internal_columns: BTreeMap::new(),
            aggregate_info: Default::default(),
            windows: Default::default(),
            srf_info: Default::default(),
            cte_context: parent.cte_context.clone(),
            in_grouping: false,
            view_info: None,
            have_async_func: false,
            have_udf_script: false,
            have_udf_server: false,
            inverted_index_map: Box::default(),
            virtual_column_context: VirtualColumnContext::with_parent(
                &parent.virtual_column_context,
            ),
            expr_context: ExprContext::default(),
            planning_agg_index: false,
            window_definitions: DashMap::new(),
        })
    }

    /// Create a new BindContext with self's parent as its parent
    pub fn replace(&self) -> Self {
        let mut bind_context = BindContext::new();
        bind_context.parent = self.parent.clone();
        bind_context.cte_context = self.cte_context.clone();
        bind_context
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
        apply_alias_for_columns(&mut self.columns, alias, name_resolution_ctx)
    }

    /// Try to find a column binding with given table name and column name.
    /// This method will return error if the given names are ambiguous or invalid.
    pub fn resolve_name(
        &self,
        database: Option<&str>,
        table: Option<&str>,
        column: &Identifier,
        available_aliases: &[(String, ScalarExpr)],
        name_resolution_ctx: &NameResolutionContext,
    ) -> Result<NameResolutionResult> {
        let name = &column.name;

        if name_resolution_ctx.deny_column_reference {
            let err = if column.is_quoted() {
                ErrorCode::SemanticError(format!(
                    "invalid identifier {name}, do you mean '{name}'?"
                ))
            } else {
                ErrorCode::SemanticError(format!("invalid identifier {name}"))
            };
            return Err(err.set_span(column.span));
        }

        let mut result = vec![];
        // Lookup parent context to resolve outer reference.
        let mut alias_match_count = 0;
        if self.expr_context.prefer_resolve_alias() {
            for (alias, scalar) in available_aliases {
                if database.is_none() && table.is_none() && name == alias {
                    result.push(NameResolutionResult::Alias {
                        alias: alias.clone(),
                        scalar: scalar.clone(),
                    });

                    alias_match_count += 1;
                }
            }

            if alias_match_count == 0 {
                self.search_bound_columns_recursively(database, table, name, &mut result);
            }
        } else {
            self.search_bound_columns_recursively(database, table, name, &mut result);

            if result.is_empty() {
                for (alias, scalar) in available_aliases {
                    if database.is_none() && table.is_none() && name == alias {
                        result.push(NameResolutionResult::Alias {
                            alias: alias.clone(),
                            scalar: scalar.clone(),
                        });
                        alias_match_count += 1;
                    }
                }
            }
        }

        if result.len() > 1 && !result.iter().all_equal() {
            return Err(ErrorCode::SemanticError(format!(
                "column {name} reference or alias is ambiguous, please use another alias name",
            ))
            .set_span(column.span));
        }

        if result.is_empty() {
            let err = if column.is_quoted() {
                ErrorCode::SemanticError(format!(
                    "column {name} doesn't exist, do you mean '{name}'?"
                ))
            } else {
                ErrorCode::SemanticError(format!("column {name} doesn't exist"))
            };
            Err(err.set_span(column.span))
        } else {
            Ok(result.remove(0))
        }
    }

    pub fn search_column_position(
        &self,
        span: Span,
        database: Option<&str>,
        table: Option<&str>,
        column: usize,
    ) -> Result<NameResolutionResult> {
        let mut result = vec![];

        for column_binding in self.columns.iter() {
            if let Some(position) = column_binding.column_position {
                if column == position
                    && Self::match_column_binding_by_position(database, table, column_binding)
                {
                    result.push(NameResolutionResult::Column(column_binding.clone()));
                }
            }
        }

        if result.is_empty() {
            Err(
                ErrorCode::SemanticError(format!("column position {column} doesn't exist"))
                    .set_span(span),
            )
        } else {
            Ok(result.remove(0))
        }
    }

    // Search bound column recursively from the parent context.
    // If current context found results, it'll stop searching.
    pub fn search_bound_columns_recursively(
        &self,
        database: Option<&str>,
        table: Option<&str>,
        column: &str,
        result: &mut Vec<NameResolutionResult>,
    ) {
        let mut bind_context: &BindContext = self;
        loop {
            for column_binding in bind_context.columns.iter() {
                if Self::match_column_binding(database, table, column, column_binding) {
                    result.push(NameResolutionResult::Column(column_binding.clone()));
                }
            }

            if !result.is_empty() {
                return;
            }

            // look up internal column
            if let Some(internal_column) = INTERNAL_COLUMN_FACTORY.get_internal_column(column) {
                let column_binding = InternalColumnBinding {
                    database_name: database.map(|n| n.to_owned()),
                    table_name: table.map(|n| n.to_owned()),
                    internal_column,
                };
                result.push(NameResolutionResult::InternalColumn(column_binding));
            }

            if !result.is_empty() {
                return;
            }

            // look up virtual column alias names
            for column_binding in bind_context.virtual_column_context.virtual_columns.iter() {
                if Self::match_column_binding(database, table, column, column_binding) {
                    result.push(NameResolutionResult::Column(column_binding.clone()));
                }
            }

            if !result.is_empty() {
                return;
            }

            if let Some(ref parent) = bind_context.parent {
                bind_context = parent;
            } else {
                break;
            }
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

    pub fn match_column_binding_by_position(
        database: Option<&str>,
        table: Option<&str>,
        column_binding: &ColumnBinding,
    ) -> bool {
        match (
            (database, column_binding.database_name.as_ref()),
            (table, column_binding.table_name.as_ref()),
        ) {
            // No qualified table name specified
            ((None, _), (None, None)) | ((None, _), (None, Some(_))) => {
                column_binding.visibility != Visibility::UnqualifiedWildcardInVisible
            }
            // Qualified column reference without database name
            ((None, _), (Some(table), Some(table_name))) => table == table_name,
            // Qualified column reference with database name
            ((Some(db), Some(db_name)), (Some(table), Some(table_name))) => {
                db == db_name && table == table_name
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
        &self,
        column_binding: &InternalColumnBinding,
    ) -> Result<IndexType> {
        if let Some(table_name) = &column_binding.table_name {
            for column in &self.columns {
                if column_binding.database_name.is_some()
                    && column.database_name != column_binding.database_name
                {
                    continue;
                }
                if column.table_name != column_binding.table_name {
                    continue;
                }
                if let Some(table_index) = column.table_index {
                    return Ok(table_index);
                }
            }
            Err(ErrorCode::SemanticError(format!(
                "Table `{table_name}` is not found in the bind context"
            )))
        } else {
            let mut table_indices = BTreeSet::new();
            for column in &self.columns {
                if column.table_name.is_none() {
                    continue;
                }
                if let Some(table_index) = column.table_index {
                    table_indices.insert(table_index);
                }
            }
            if table_indices.is_empty() {
                return Err(ErrorCode::SemanticError(format!(
                    "The table of the internal column `{}` is not found",
                    column_binding.internal_column.column_name()
                )));
            }
            if table_indices.len() > 1 {
                return Err(ErrorCode::SemanticError(format!(
                    "The table of the internal column `{}` is ambiguous",
                    column_binding.internal_column.column_name()
                )));
            }
            Ok(*table_indices.first().unwrap())
        }
    }

    // Add internal column binding into `BindContext` and convert `InternalColumnBinding` to `ColumnBinding`.
    pub fn add_internal_column_binding(
        &mut self,
        column_binding: &InternalColumnBinding,
        metadata: MetadataRef,
        table_index: Option<IndexType>,
        visible: bool,
    ) -> Result<ColumnBinding> {
        let column_id = column_binding.internal_column.column_id();
        let table_index = if let Some(table_index) = table_index {
            table_index
        } else {
            self.get_internal_column_table_index(column_binding)?
        };

        let (column_index, is_new) =
            match self.bound_internal_columns.entry((table_index, column_id)) {
                btree_map::Entry::Vacant(e) => {
                    let mut metadata = metadata.write();
                    let column_index = metadata
                        .add_internal_column(table_index, column_binding.internal_column.clone());
                    e.insert(column_index);
                    (column_index, true)
                }
                btree_map::Entry::Occupied(e) => {
                    let column_index = e.get();
                    (*column_index, false)
                }
            };

        let metadata = metadata.read();
        let table = metadata.table(table_index);
        if !table.table().supported_internal_column(column_id) {
            return Err(ErrorCode::SemanticError(format!(
                "Unsupported internal column '{}' in table '{}'.",
                column_binding.internal_column.column_name(),
                table.table().name()
            )));
        }

        let column = metadata.column(column_index);
        let column_binding = ColumnBindingBuilder::new(
            column.name(),
            column_index,
            Box::new(column.data_type()),
            if visible {
                Visibility::Visible
            } else {
                Visibility::InVisible
            },
        )
        .database_name(Some(table.database().to_string()))
        .table_name(Some(table.name().to_string()))
        .table_index(Some(table_index))
        .build();

        if is_new {
            debug_assert!(!self.columns.iter().any(|c| c == &column_binding));
            self.columns.push(column_binding.clone());
        }

        Ok(column_binding)
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

pub fn apply_alias_for_columns(
    columns: &mut [ColumnBinding],
    alias: &TableAlias,
    name_resolution_ctx: &NameResolutionContext,
) -> Result<()> {
    for column in columns.iter_mut() {
        column.database_name = None;
        column.table_name = Some(normalize_identifier(&alias.name, name_resolution_ctx).name);
    }

    if alias.columns.len() > columns.len() {
        return Err(ErrorCode::SemanticError(format!(
            "table has {} columns available but {} columns specified",
            columns.len(),
            alias.columns.len()
        ))
        .set_span(alias.name.span));
    }
    for (index, column_name) in alias
        .columns
        .iter()
        .map(|ident| normalize_identifier(ident, name_resolution_ctx).name)
        .enumerate()
    {
        columns[index].column_name = column_name;
    }
    Ok(())
}
