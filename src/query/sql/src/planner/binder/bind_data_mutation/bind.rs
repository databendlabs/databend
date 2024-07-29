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
use std::collections::HashSet;
use std::sync::Arc;

use databend_common_ast::ast::Expr;
use databend_common_ast::ast::MatchOperation;
use databend_common_ast::ast::MatchedClause;
use databend_common_ast::ast::UnmatchedClause;
use databend_common_ast::ParseError;
use databend_common_catalog::lock::LockTableOption;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::FieldIndex;
use databend_common_expression::Scalar;
use databend_common_expression::TableSchema;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::ROW_VERSION_COL_NAME;
use indexmap::IndexMap;

use crate::binder::bind_data_mutation::data_mutation_input::DataMutationInput;
use crate::binder::bind_data_mutation::data_mutation_input::DataMutationInputBindResult;
use crate::binder::util::TableIdentifier;
use crate::binder::wrap_cast;
use crate::binder::Binder;
use crate::normalize_identifier;
use crate::optimizer::SExpr;
use crate::plans::BoundColumnRef;
use crate::plans::ConstantExpr;
use crate::plans::FunctionCall;
use crate::plans::MatchedEvaluator;
use crate::plans::Plan;
use crate::plans::RelOperator;
use crate::plans::UnmatchedEvaluator;
use crate::BindContext;
use crate::ColumnBinding;
use crate::ColumnEntry;
use crate::ScalarBinder;
use crate::ScalarExpr;
use crate::UdfRewriter;

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum DataMutationType {
    MatchedOnly,
    FullOperation,
    InsertOnly,
}

pub struct DataMutation {
    pub target_table_identifier: TableIdentifier,
    pub input: DataMutationInput,
    pub mutation_type: DataMutationType,
    pub matched_clauses: Vec<MatchedClause>,
    pub unmatched_clauses: Vec<UnmatchedClause>,
}

impl DataMutation {
    pub fn check_semantic(&self) -> Result<()> {
        Self::check_multi_match_clauses_semantic(&self.matched_clauses)?;
        Self::check_multi_unmatch_clauses_semantic(&self.unmatched_clauses)
    }

    pub fn check_multi_match_clauses_semantic(clauses: &[MatchedClause]) -> Result<()> {
        // Check match clauses.
        if clauses.len() > 1 {
            for (idx, clause) in clauses.iter().enumerate() {
                if clause.selection.is_none() && idx < clauses.len() - 1 {
                    return Err(ParseError(None,
                        "when there are multi matched clauses, we must have a condition for every one except the last one".to_string(),
                    ).into());
                }
            }
        }
        Ok(())
    }

    pub fn check_multi_unmatch_clauses_semantic(clauses: &[UnmatchedClause]) -> Result<()> {
        // Check unmatch clauses.
        if clauses.len() > 1 {
            for (idx, clause) in clauses.iter().enumerate() {
                if clause.selection.is_none() && idx < clauses.len() - 1 {
                    return Err(ParseError(None,
                        "when there are multi unmatched clauses, we must have a condition for every one except the last one".to_string(),
                    ).into());
                }
            }
        }
        Ok(())
    }
}

impl Binder {
    pub async fn bind_data_mutation(
        &mut self,
        bind_context: &mut BindContext,
        data_mutation: DataMutation,
    ) -> Result<Plan> {
        data_mutation.check_semantic()?;
        let DataMutation {
            target_table_identifier,
            input,
            mutation_type,
            matched_clauses,
            unmatched_clauses,
        } = data_mutation;

        let (catalog_name, database_name, table_name, table_name_alias) = (
            target_table_identifier.catalog_name(),
            target_table_identifier.database_name(),
            target_table_identifier.table_name(),
            target_table_identifier.table_name_alias(),
        );

        // Add table lock before execution.
        let lock_guard = if mutation_type != DataMutationType::InsertOnly {
            self.ctx
                .clone()
                .acquire_table_lock(
                    &catalog_name,
                    &database_name,
                    &table_name,
                    &LockTableOption::LockWithRetry,
                )
                .await
                .map_err(|err| target_table_identifier.not_found_suggest_error(err))?
        } else {
            None
        };

        let table = self
            .ctx
            .get_table(&catalog_name, &database_name, &table_name)
            .await?;

        let table_schema = table.schema();

        let input = input
            .bind(
                self,
                bind_context,
                table.clone(),
                &target_table_identifier,
                table_schema.clone(),
            )
            .await?;

        let DataMutationInputBindResult {
            input,
            input_type,
            mut required_columns,
            mut bind_context,
            update_or_insert_columns_star,
            target_table_index,
            target_row_id_index,
            mutation_source,
            predicate_index,
            truncate_table,
            mutation_filter,
        } = input;

        let target_table_name = if let Some(table_name_alias) = &table_name_alias {
            table_name_alias.clone()
        } else {
            table_name.clone()
        };

        let target_column_entries = self
            .metadata
            .read()
            .columns_by_table_index(target_table_index);

        let mut matched_evaluators = Vec::with_capacity(matched_clauses.len());
        let mut unmatched_evaluators = Vec::with_capacity(unmatched_clauses.len());
        let mut field_index_map = HashMap::<usize, String>::new();
        let update_row_version = if self.has_update(&matched_clauses) {
            // If exists update clause and change tracking enabled, we need to update row_version,
            // we need database name and table name in update_row_version, if the table alias is
            // not None, after the binding phase, the bound columns will have a database of None,
            // so we adjust it accordingly.
            let database_name = if table_name_alias.is_some() {
                None
            } else {
                Some(database_name.clone())
            };
            let update_row_version = if table.change_tracking_enabled() {
                Some(Self::update_row_version(
                    table.schema_with_stream(),
                    &bind_context.columns,
                    database_name.as_deref(),
                    Some(&target_table_name),
                )?)
            } else {
                None
            };

            // If exists update clause, we need to read all columns of target table.
            for (idx, field) in table.schema_with_stream().fields().iter().enumerate() {
                let column_index = Self::find_column_index(&target_column_entries, field.name())?;
                field_index_map.insert(idx, column_index.to_string());
            }
            update_row_version
        } else {
            None
        };

        if table.change_tracking_enabled() && mutation_type != DataMutationType::InsertOnly {
            for stream_column in table.stream_columns() {
                let column_index =
                    Self::find_column_index(&target_column_entries, stream_column.column_name())?;
                required_columns.insert(column_index);
            }
        }

        let name_resolution_ctx = self.name_resolution_ctx.clone();
        let mut scalar_binder = ScalarBinder::new(
            &mut bind_context,
            self.ctx.clone(),
            &name_resolution_ctx,
            self.metadata.clone(),
            &[],
            HashMap::new(),
            Box::new(IndexMap::new()),
        );

        // Bind matched clause columns and add update fields and exprs
        for clause in &matched_clauses {
            matched_evaluators.push(
                self.bind_matched_clause(
                    &mut scalar_binder,
                    clause,
                    table_schema.clone(),
                    update_or_insert_columns_star.clone(),
                    update_row_version.clone(),
                    &target_table_name,
                )
                .await?,
            );
        }

        // Bind not matched clause columns and add insert exprs
        for clause in &unmatched_clauses {
            unmatched_evaluators.push(
                self.bind_unmatched_clause(
                    &mut scalar_binder,
                    clause,
                    table_schema.clone(),
                    update_or_insert_columns_star.clone(),
                )
                .await?,
            );
        }

        let data_mutation = crate::plans::DataMutation {
            catalog_name,
            database_name,
            table_name,
            table_name_alias,
            bind_context: Box::new(bind_context),
            metadata: self.metadata.clone(),
            input_type,
            required_columns: Box::new(required_columns),
            matched_evaluators,
            unmatched_evaluators,
            target_table_index,
            field_index_map,
            mutation_type: mutation_type.clone(),
            distributed: false,
            change_join_order: false,
            mutation_source,
            predicate_index,
            truncate_table,
            mutation_filter,
            row_id_index: target_row_id_index,
            can_try_update_column_only: self.can_try_update_column_only(&matched_clauses),
            lock_guard,
        };

        if mutation_type == DataMutationType::InsertOnly && !insert_only(&data_mutation) {
            return Err(ErrorCode::SemanticError(
                "For unmatched clause, then condition and exprs can only have source fields",
            ));
        }

        let schema = data_mutation.schema();
        let mut s_expr = SExpr::create_unary(
            Arc::new(RelOperator::DataMutation(data_mutation)),
            Arc::new(input),
        );

        // rewrite udf for interpreter udf
        let mut udf_rewriter = UdfRewriter::new(self.metadata.clone(), true);
        s_expr = udf_rewriter.rewrite(&s_expr)?;

        // rewrite udf for server udf
        let mut udf_rewriter = UdfRewriter::new(self.metadata.clone(), false);
        s_expr = udf_rewriter.rewrite(&s_expr)?;

        Ok(Plan::DataMutation {
            s_expr: Box::new(s_expr),
            schema,
            metadata: self.metadata.clone(),
        })
    }

    fn can_try_update_column_only(&self, matched_clauses: &[MatchedClause]) -> bool {
        if matched_clauses.len() == 1 {
            let matched_clause = &matched_clauses[0];
            if matched_clause.selection.is_none() {
                if let MatchOperation::Update {
                    update_list,
                    is_star,
                } = &matched_clause.operation
                {
                    let mut is_column_only = true;
                    for update_expr in update_list {
                        is_column_only =
                            is_column_only && matches!(update_expr.expr, Expr::ColumnRef { .. });
                    }
                    return is_column_only || *is_star;
                }
            }
        }
        false
    }

    async fn bind_matched_clause<'a>(
        &mut self,
        scalar_binder: &mut ScalarBinder<'a>,
        clause: &MatchedClause,
        schema: TableSchemaRef,
        update_or_insert_columns_star: Option<HashMap<FieldIndex, ScalarExpr>>,
        update_row_version: Option<(FieldIndex, ScalarExpr)>,
        target_name: &str,
    ) -> Result<MatchedEvaluator> {
        let condition = if let Some(expr) = &clause.selection {
            let (scalar_expr, _) = scalar_binder.bind(expr)?;
            if !self.check_allowed_scalar_expr(&scalar_expr)? {
                return Err(ErrorCode::SemanticError(
                    "matched clause's condition can't contain subquery|window|aggregate|udf functions|async functions"
                        .to_string(),
                )
                .set_span(scalar_expr.span()));
            }
            Some(scalar_expr)
        } else {
            None
        };

        let mut update = if let MatchOperation::Update {
            update_list,
            is_star,
        } = &clause.operation
        {
            if *is_star {
                update_or_insert_columns_star
            } else {
                let mut update_columns = HashMap::with_capacity(update_list.len());
                for update_expr in update_list {
                    let (scalar_expr, _) = scalar_binder.bind(&update_expr.expr)?;
                    if !self.check_allowed_scalar_expr(&scalar_expr)? {
                        return Err(ErrorCode::SemanticError(
                            "update clause's can't contain subquery|window|aggregate|udf functions"
                                .to_string(),
                        )
                        .set_span(scalar_expr.span()));
                    }
                    let col_name =
                        normalize_identifier(&update_expr.name, &self.name_resolution_ctx).name;
                    if let Some(tbl_identify) = &update_expr.table {
                        let update_table_name =
                            normalize_identifier(tbl_identify, &self.name_resolution_ctx).name;
                        if update_table_name != target_name {
                            return Err(ErrorCode::BadArguments(format!(
                                "Update Identify's `{}` should be `{}`",
                                update_table_name, target_name
                            )));
                        }
                    }

                    let index = schema.index_of(&col_name)?;

                    if update_columns.contains_key(&index) {
                        return Err(ErrorCode::BadArguments(format!(
                            "Multiple assignments in the single statement to column `{}`",
                            col_name
                        )));
                    }

                    let field = schema.field(index);
                    if field.computed_expr().is_some() {
                        return Err(ErrorCode::BadArguments(format!(
                            "The value specified for computed column '{}' is not allowed",
                            field.name()
                        )));
                    }

                    update_columns.insert(index, scalar_expr.clone());
                }

                Some(update_columns)
            }
        } else {
            // delete
            None
        };

        // update row_version
        if let Some((index, row_version)) = update_row_version {
            update.as_mut().map(|v| v.insert(index, row_version));
        }
        Ok(MatchedEvaluator { condition, update })
    }

    async fn bind_unmatched_clause<'a>(
        &mut self,
        scalar_binder: &mut ScalarBinder<'a>,
        clause: &UnmatchedClause,
        table_schema: TableSchemaRef,
        update_or_insert_columns_star: Option<HashMap<FieldIndex, ScalarExpr>>,
    ) -> Result<UnmatchedEvaluator> {
        let condition = if let Some(expr) = &clause.selection {
            let (scalar_expr, _) = scalar_binder.bind(expr)?;
            if !self.check_allowed_scalar_expr(&scalar_expr)? {
                return Err(ErrorCode::SemanticError(
                    "unmatched clause's condition can't contain subquery|window|aggregate|udf functions"
                        .to_string(),
                )
                .set_span(scalar_expr.span()));
            }
            Some(scalar_expr)
        } else {
            None
        };
        if clause.insert_operation.is_star {
            let default_schema = table_schema.remove_computed_fields();
            let mut values = Vec::with_capacity(default_schema.num_fields());
            let update_or_insert_columns_star = update_or_insert_columns_star.unwrap();
            for idx in 0..default_schema.num_fields() {
                let scalar = update_or_insert_columns_star.get(&idx).unwrap().clone();
                // cast expr
                values.push(wrap_cast(
                    &scalar,
                    &DataType::from(default_schema.field(idx).data_type()),
                ));
            }

            Ok(UnmatchedEvaluator {
                source_schema: Arc::new(Arc::new(default_schema).into()),
                condition,
                values,
            })
        } else {
            if clause.insert_operation.values.is_empty() {
                return Err(ErrorCode::SemanticError(
                    "Values lists must have at least one row".to_string(),
                ));
            }

            let mut values = Vec::with_capacity(clause.insert_operation.values.len());
            // we need to get source schema, and use it for filling columns.
            let source_schema = if let Some(fields) = clause.insert_operation.columns.clone() {
                self.schema_project(&table_schema, &fields)?
            } else {
                table_schema.clone()
            };
            if clause.insert_operation.values.len() != source_schema.num_fields() {
                return Err(ErrorCode::SemanticError(
                    "insert columns and values are not matched".to_string(),
                ));
            }
            for (idx, expr) in clause.insert_operation.values.iter().enumerate() {
                let (mut scalar_expr, _) = scalar_binder.bind(expr)?;
                if !self.check_allowed_scalar_expr(&scalar_expr)? {
                    return Err(ErrorCode::SemanticError(
                        "insert clause's can't contain subquery|window|aggregate|udf functions"
                            .to_string(),
                    )
                    .set_span(scalar_expr.span()));
                }
                // type cast
                scalar_expr = wrap_cast(
                    &scalar_expr,
                    &DataType::from(source_schema.field(idx).data_type()),
                );

                values.push(scalar_expr.clone());
            }

            Ok(UnmatchedEvaluator {
                source_schema: Arc::new(source_schema.into()),
                condition,
                values,
            })
        }
    }

    pub fn find_column_index(column_entries: &Vec<ColumnEntry>, col_name: &str) -> Result<usize> {
        for column_entry in column_entries {
            if col_name == column_entry.name() {
                return Ok(column_entry.index());
            }
        }
        Err(ErrorCode::BadArguments(format!(
            "not found col name: {}",
            col_name
        )))
    }

    fn has_update(&self, matched_clauses: &Vec<MatchedClause>) -> bool {
        for clause in matched_clauses {
            if let MatchOperation::Update { .. } = clause.operation {
                return true;
            }
        }
        false
    }

    pub fn update_row_version(
        schema: Arc<TableSchema>,
        columns: &[ColumnBinding],
        database_name: Option<&str>,
        table_name: Option<&str>,
    ) -> Result<(FieldIndex, ScalarExpr)> {
        let col_name = ROW_VERSION_COL_NAME;
        let index = schema.index_of(col_name)?;
        let mut row_version = None;
        for column_binding in columns.iter() {
            if BindContext::match_column_binding(
                database_name,
                table_name,
                col_name,
                column_binding,
            ) {
                row_version = Some(ScalarExpr::BoundColumnRef(BoundColumnRef {
                    span: None,
                    column: column_binding.clone(),
                }));
                break;
            }
        }
        let col = row_version.ok_or_else(|| ErrorCode::Internal("row_version It's a bug"))?;
        let scalar = ScalarExpr::FunctionCall(FunctionCall {
            span: None,
            func_name: "plus".to_string(),
            params: vec![],
            arguments: vec![
                col,
                ConstantExpr {
                    span: None,
                    value: Scalar::Number(NumberScalar::UInt64(1)),
                }
                .into(),
            ],
        });
        Ok((index, scalar))
    }
}

fn insert_only(merge_plan: &crate::plans::DataMutation) -> bool {
    let metadata = merge_plan.metadata.read();
    let target_table_columns: HashSet<usize> = metadata
        .columns_by_table_index(merge_plan.target_table_index)
        .iter()
        .map(|column| column.index())
        .collect();

    for evaluator in &merge_plan.unmatched_evaluators {
        if evaluator.condition.is_some() {
            let condition = evaluator.condition.as_ref().unwrap();
            for column in condition.used_columns() {
                if target_table_columns.contains(&column) {
                    return false;
                }
            }
        }

        for value in &evaluator.values {
            for column in value.used_columns() {
                if target_table_columns.contains(&column) {
                    return false;
                }
            }
        }
    }
    true
}
