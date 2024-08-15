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
use std::fmt;
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

use crate::binder::bind_mutation::mutation_expression::MutationExpression;
use crate::binder::bind_mutation::mutation_expression::MutationExpressionBindResult;
use crate::binder::util::TableIdentifier;
use crate::binder::wrap_cast;
use crate::binder::Binder;
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

#[derive(Default, Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum MutationType {
    #[default]
    Merge,
    Update,
    Delete,
}

impl fmt::Display for MutationType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            MutationType::Merge => write!(f, "MERGE"),
            MutationType::Update => write!(f, "UPDATE"),
            MutationType::Delete => write!(f, "DELETE"),
        }
    }
}

// Mutation strategies:
// (1) Direct: if the mutation filter is a simple expression, we use MutationSource to execute the mutation directly.
// (2) MatchedOnly: INNER JOIN.
// (3) NotMatchedOnly: LEFT/RIGHT OUTER JOIN.
// (4) MixedMatched: LEFT/RIGHT OUTER JOIN.
#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum MutationStrategy {
    Direct,
    MatchedOnly,
    NotMatchedOnly,
    MixedMatched,
}

pub struct Mutation {
    pub target_table_identifier: TableIdentifier,
    pub expression: MutationExpression,
    pub strategy: MutationStrategy,
    pub matched_clauses: Vec<MatchedClause>,
    pub unmatched_clauses: Vec<UnmatchedClause>,
}

impl Mutation {
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
    pub async fn bind_mutation(
        &mut self,
        bind_context: &mut BindContext,
        mutation: Mutation,
    ) -> Result<Plan> {
        mutation.check_semantic()?;

        let Mutation {
            target_table_identifier,
            expression,
            strategy,
            matched_clauses,
            unmatched_clauses,
        } = mutation;

        let (catalog_name, database_name, table_name, table_name_alias) = (
            target_table_identifier.catalog_name(),
            target_table_identifier.database_name(),
            target_table_identifier.table_name(),
            target_table_identifier.table_name_alias(),
        );

        // Add table lock before execution.
        let lock_guard = if strategy != MutationStrategy::NotMatchedOnly {
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

        let bind_result = expression
            .bind(
                self,
                bind_context,
                table.clone(),
                &target_table_identifier,
                table_schema.clone(),
            )
            .await?;

        let MutationExpressionBindResult {
            input,
            mut bind_context,
            mutation_type,
            mutation_strategy,
            target_table_index,
            target_table_row_id_index,
            mut required_columns,
            all_source_columns,
            truncate_table,
            predicate_column_index,
            direct_filter,
        } = bind_result;

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

        if table.change_tracking_enabled() && mutation_strategy != MutationStrategy::NotMatchedOnly
        {
            for stream_column in table.stream_columns() {
                let column_index =
                    Self::find_column_index(&target_column_entries, stream_column.column_name())?;
                required_columns.insert(column_index);
            }
        }

        let mut scalar_binder = ScalarBinder::new(
            &mut bind_context,
            self.ctx.clone(),
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
                    all_source_columns.clone(),
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
                    all_source_columns.clone(),
                )
                .await?,
            );
        }

        let mutation = crate::plans::Mutation {
            catalog_name,
            database_name,
            table_name,
            table_name_alias,
            bind_context: Box::new(bind_context),
            metadata: self.metadata.clone(),
            mutation_type,
            required_columns: Box::new(required_columns),
            matched_evaluators,
            unmatched_evaluators,
            target_table_index,
            field_index_map,
            strategy: mutation_strategy.clone(),
            distributed: false,
            row_id_shuffle: true,
            row_id_index: target_table_row_id_index,
            can_try_update_column_only: self.can_try_update_column_only(&matched_clauses),
            lock_guard,
            truncate_table,
            predicate_column_index,
            direct_filter,
        };

        if mutation_strategy == MutationStrategy::NotMatchedOnly && !insert_only(&mutation) {
            return Err(ErrorCode::SemanticError(
                "For unmatched clause, then condition and exprs can only have source fields",
            ));
        }

        let schema = mutation.schema();
        let mut s_expr =
            SExpr::create_unary(Arc::new(RelOperator::Mutation(mutation)), Arc::new(input));

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
        all_source_columns: Option<HashMap<FieldIndex, ScalarExpr>>,
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
                all_source_columns
            } else {
                let mut update_columns = HashMap::with_capacity(update_list.len());
                for update_expr in update_list {
                    let (scalar_expr, _) = scalar_binder.bind(&update_expr.expr)?;
                    if !self.check_allowed_scalar_expr_with_udf(&scalar_expr)? {
                        return Err(ErrorCode::SemanticError(
                            "update clause's can't contain subquery|window|aggregate|udf functions"
                                .to_string(),
                        )
                        .set_span(scalar_expr.span()));
                    }
                    let col_name = update_expr.name.normalized_name();
                    if let Some(tbl_identify) = &update_expr.table {
                        let update_table_name = tbl_identify.normalized_name();
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
        all_source_columns: Option<HashMap<FieldIndex, ScalarExpr>>,
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
            let all_source_columns = all_source_columns.unwrap();
            for idx in 0..default_schema.num_fields() {
                let scalar = all_source_columns.get(&idx).unwrap().clone();
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

fn insert_only(mutation: &crate::plans::Mutation) -> bool {
    let metadata = mutation.metadata.read();
    let target_table_columns: HashSet<usize> = metadata
        .columns_by_table_index(mutation.target_table_index)
        .iter()
        .map(|column| column.index())
        .collect();

    for evaluator in &mutation.unmatched_evaluators {
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
