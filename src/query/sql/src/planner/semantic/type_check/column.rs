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

use databend_common_ast::Span;
use databend_common_ast::ast::ColumnID;
use databend_common_ast::ast::ColumnRef;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::Identifier;
use databend_common_ast::parser::parse_expr;
use databend_common_ast::parser::tokenize_sql;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use derive_visitor::DriveMut;
use derive_visitor::VisitorMut;

use super::CoreExpr;
use super::CoreExprArena;
use super::CoreExprId;
use super::TypeCheckAdapter;
use super::TypeChecker;
use crate::binder::NameResolutionResult;
use crate::planner::semantic::normalize_identifier;
use crate::plans::BoundColumnRef;
use crate::plans::ScalarExpr;

impl<'a> CoreExprArena<'a> {
    pub(super) fn column_ref(&mut self, span: Span, column: &'a ColumnRef) -> CoreExprId {
        self.alloc(CoreExpr::ColumnRef { span, column })
    }
}

impl<'a, A> TypeChecker<'a, A>
where A: TypeCheckAdapter
{
    pub(super) fn resolve_column_ref(
        &mut self,
        span: Span,
        column_ref: &ColumnRef,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let ColumnRef {
            database,
            table,
            column: ident,
        } = column_ref;
        let database = database
            .as_ref()
            .map(|ident| normalize_identifier(ident, self.name_resolution_ctx).name);
        let table = table
            .as_ref()
            .map(|ident| normalize_identifier(ident, self.name_resolution_ctx).name);
        let result = match ident {
            ColumnID::Name(ident) => {
                let column = normalize_identifier(ident, self.name_resolution_ctx);
                self.bind_context.resolve_name(
                    database.as_deref(),
                    table.as_deref(),
                    &column,
                    self.aliases,
                    self.name_resolution_ctx,
                )?
            }
            ColumnID::Position(pos) => self.bind_context.search_column_position(
                pos.span,
                database.as_deref(),
                table.as_deref(),
                pos.pos,
            )?,
        };

        let (scalar, data_type) = match result {
            NameResolutionResult::Column(column) => {
                if let Some(virtual_expr) = column.virtual_expr {
                    let sql_tokens = tokenize_sql(virtual_expr.as_str())?;
                    let expr = parse_expr(&sql_tokens, self.dialect)?;
                    return self.resolve(&expr);
                } else {
                    // Fast path: Check if table has any masking policies at all before doing expensive async work
                    // BUT: skip masking policy application if we're already resolving a masking policy expression
                    // to prevent infinite recursion (e.g., policy references the masked column itself)
                    let has_masking_policy = !self.in_masking_policy
                        // Does this column reference a table with masking policy?
                        && column
                            .table_index
                            .and_then(|table_index| {
                                // IMPORTANT: Extract all needed data before releasing the lock
                                // to avoid holding the lock during fallback resolution
                                let (table_entry_opt, db_name, tbl_name) = {
                                    let metadata = self.metadata.read();
                                    let entry = metadata.tables().get(table_index);
                                    (
                                        entry.is_some(),
                                        column.database_name.clone(),
                                        column.table_name.clone(),
                                    )
                                }; // metadata lock is released here

                                // Now handle the fallback case without holding the lock
                                let final_table_index = if table_entry_opt {
                                    Some(table_index)
                                } else {
                                    // table_index invalid - try fallback by name
                                    // This can happen in complex queries (e.g., REPLACE INTO with source columns)
                                    // where metadata context differs between binding phases
                                    if let (Some(db), Some(tbl)) =
                                        (db_name.as_ref(), tbl_name.as_ref())
                                    {
                                        // Re-acquire lock for lookup
                                        let metadata = self.metadata.read();
                                        metadata.get_table_index(Some(db), tbl)
                                    } else {
                                        None
                                    }
                                };

                                // Re-acquire lock to get table info
                                final_table_index.and_then(|idx| {
                                    let metadata = self.metadata.read();
                                    let table_entry = metadata.tables().get(idx)?;
                                    let table_ref = table_entry.table();
                                    let table_info = table_ref.get_table_info();
                                    let table_schema = table_ref.schema();

                                    if table_info.meta.column_mask_policy_columns_ids.is_empty() {
                                        return None;
                                    }
                                    table_schema
                                        .fields()
                                        .iter()
                                        .find(|f| f.name == column.column_name)
                                        .and_then(|field| {
                                            table_info
                                                .meta
                                                .column_mask_policy_columns_ids
                                                .contains_key(&field.column_id)
                                                .then_some(())
                                        })
                                })
                            })
                            .is_some();

                    if has_masking_policy {
                        // Only load the policy definition after table metadata proves this
                        // column has an attached masking policy.
                        let mask_expr = self.get_masking_policy_expr_for_column(
                            &column,
                            database.as_deref(),
                            table.as_deref(),
                        )?;

                        if let Some(mask_expr) = mask_expr {
                            // Set flag to prevent recursive masking policy application
                            let old_in_masking_policy = self.in_masking_policy;
                            self.in_masking_policy = true;

                            // Recursively resolve the masking policy expression
                            let result = self.resolve(&mask_expr);

                            // Restore flag
                            self.in_masking_policy = old_in_masking_policy;

                            return result;
                        }
                    }

                    let data_type = *column.data_type.clone();
                    (BoundColumnRef { span, column }.into(), data_type)
                }
            }
            NameResolutionResult::InternalColumn(column) => {
                // add internal column binding into `BindContext`
                let column = self.bind_context.add_internal_column_binding(
                    &column,
                    self.metadata.clone(),
                    None,
                    true,
                )?;
                let data_type = *column.data_type.clone();
                (BoundColumnRef { span, column }.into(), data_type)
            }
            NameResolutionResult::Alias { scalar, .. } => (scalar.clone(), scalar.data_type()?),
        };

        Ok(Box::new((scalar, data_type)))
    }

    /// Get masking policy expression for a column reference
    /// This is the ONLY place where masking policy is applied - unifying all paths (SELECT/WHERE/HAVING)
    fn get_masking_policy_expr_for_column(
        &self,
        column_binding: &crate::ColumnBinding,
        database: Option<&str>,
        table: Option<&str>,
    ) -> Result<Option<Expr>> {
        use databend_common_ast::ast;

        // Check if this column has a masking policy
        if let Some(table_index) = column_binding.table_index {
            // Extract all needed data before loading the policy definition to avoid
            // holding the metadata lock across cache or metastore access.
            let policy_data = {
                let metadata = self.metadata.read();
                let table_entry = metadata.table(table_index);
                let table_ref = table_entry.table();
                let table_info_ref = table_ref.get_table_info();
                let table_schema = table_ref.schema();

                // Find the field by name to get column_id
                if let Some(field) = table_schema
                    .fields()
                    .iter()
                    .find(|f| f.name == column_binding.column_name)
                {
                    table_info_ref
                        .meta
                        .column_mask_policy_columns_ids
                        .get(&field.column_id)
                        .map(|policy_info| {
                            // Extract data needed after the metadata lock is released.
                            (
                                policy_info.policy_id,
                                policy_info.columns_ids.clone(),
                                table_schema,
                            )
                        })
                } else {
                    None
                }
            }; // metadata lock is released here

            if let Some((policy_id, using_columns, table_schema)) = policy_data {
                let Some(cached) = self.adapter.resolve_data_mask_policy(policy_id).map_err(
                    |err| {
                        ErrorCode::UnknownMaskPolicy(format!(
                            "Failed to load masking policy (id: {}) for column '{}': {}. Query denied to prevent potential data leakage. Please verify the policy still exists and meta service is available",
                            policy_id, column_binding.column_name, err
                        ))
                    },
                )?
                else {
                    return Ok(None);
                };

                let args = &cached.args;

                // Create arguments based on USING clause
                let arguments: Result<Vec<Expr>> = args
                    .iter()
                    .enumerate()
                    .map(|(param_idx, _)| {
                        let column_id = using_columns.get(param_idx).ok_or_else(|| {
                            ErrorCode::Internal(format!(
                                "Masking policy metadata is corrupted: policy requires {} parameters, \
                                 but only {} columns are configured in USING clause. \
                                 Please drop and recreate the masking policy attachment.",
                                args.len(),
                                using_columns.len()
                            ))
                        })?;

                        let field_name = table_schema
                            .fields()
                            .iter()
                            .find(|f| f.column_id == *column_id)
                            .map(|f| f.name.clone())
                            .unwrap_or_else(|| format!("column_{}", column_id));

                        Ok(Expr::ColumnRef {
                            span: None,
                            column: ast::ColumnRef {
                                database: database.map(|d| Identifier::from_name(None, d.to_string())),
                                table: table.map(|t| Identifier::from_name(None, t.to_string())),
                                column: ast::ColumnID::Name(Identifier::from_name(
                                    None, field_name,
                                )),
                            },
                        })
                    })
                    .collect();
                let arguments = arguments?;

                // Create parameter mapping
                // Since parameter names are normalized to lowercase at policy creation time (see data_mask.rs),
                // we use them directly as keys.
                let args_map: HashMap<_, _> = args
                    .iter()
                    .map(|(param_name, _)| param_name.as_str())
                    .zip(arguments.iter().cloned())
                    .collect();

                // Replace parameters in the expression
                let expr = Self::clone_expr_with_replacement(&cached.expr, |nest_expr| {
                    if let Expr::ColumnRef { column, .. } = nest_expr {
                        // Parameter names are already lowercase in args_map (normalized at creation).
                        // Lookup also needs to be lowercase for consistent matching.
                        if let Some(arg) =
                            args_map.get(column.column.name().to_lowercase().as_str())
                        {
                            return Ok(Some(arg.clone()));
                        }
                    }
                    Ok(None)
                })?;

                return Ok(Some(expr));
            }
        }
        Ok(None)
    }
}

impl<'a, A> TypeChecker<'a, A> {
    pub fn clone_expr_with_replacement<F>(original_expr: &Expr, replacement_fn: F) -> Result<Expr>
    where F: Fn(&Expr) -> Result<Option<Expr>> {
        #[derive(VisitorMut)]
        #[visitor(Expr(enter))]
        struct ReplacerVisitor<F: Fn(&Expr) -> Result<Option<Expr>>>(F);

        impl<F: Fn(&Expr) -> Result<Option<Expr>>> ReplacerVisitor<F> {
            fn enter_expr(&mut self, expr: &mut Expr) {
                let replacement_opt = (self.0)(expr);
                if let Ok(Some(replacement)) = replacement_opt {
                    *expr = replacement;
                }
            }
        }
        let mut visitor = ReplacerVisitor(replacement_fn);
        let mut expr = original_expr.clone();
        expr.drive_mut(&mut visitor);
        Ok(expr)
    }
}
