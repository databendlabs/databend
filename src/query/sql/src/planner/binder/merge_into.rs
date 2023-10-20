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

use common_ast::ast::Join;
use common_ast::ast::JoinCondition;
use common_ast::ast::JoinOperator::LeftOuter;
use common_ast::ast::MatchOperation;
use common_ast::ast::MatchedClause;
use common_ast::ast::MergeIntoStmt;
use common_ast::ast::TableReference;
use common_ast::ast::UnmatchedClause;
use common_catalog::plan::InternalColumn;
use common_catalog::plan::InternalColumnType;
use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::DataType;
use common_expression::FieldIndex;
use common_expression::TableSchemaRef;
use common_expression::ROW_ID_COL_NAME;
use indexmap::IndexMap;

use super::wrap_cast_scalar;
use super::Finder;
use crate::binder::Binder;
use crate::binder::InternalColumnBinding;
use crate::normalize_identifier;
use crate::optimizer::SExpr;
use crate::plans::BoundColumnRef;
use crate::plans::MatchedEvaluator;
use crate::plans::MergeInto;
use crate::plans::Plan;
use crate::plans::UnmatchedEvaluator;
use crate::BindContext;
use crate::ColumnBindingBuilder;
use crate::ColumnEntry;
use crate::IndexType;
use crate::ScalarBinder;
use crate::ScalarExpr;
use crate::Visibility;

// implementation of merge into for now:
//      use an left outer join for target_source and source.
impl Binder {
    #[allow(warnings)]
    #[async_backtrace::framed]
    pub(in crate::planner::binder) async fn bind_merge_into(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &MergeIntoStmt,
    ) -> Result<Plan> {
        if !self
            .ctx
            .get_settings()
            .get_enable_experimental_merge_into()
            .unwrap_or_default()
        {
            return Err(ErrorCode::Unimplemented(
                "merge into is unstable for now, you can use 'set enable_experimental_merge_into = 1' to set up it",
            ));
        }
        let MergeIntoStmt {
            catalog,
            database,
            table_ident,
            source,
            target_alias,
            join_expr,
            merge_options,
            ..
        } = stmt;

        if merge_options.is_empty() {
            return Err(ErrorCode::BadArguments(
                "at least one matched or unmatched clause for merge into",
            ));
        }

        let (matched_clauses, unmatched_clauses) = stmt.split_clauses();
        let mut unmatched_evaluators =
            Vec::<UnmatchedEvaluator>::with_capacity(unmatched_clauses.len());
        let mut matched_evaluators = Vec::<MatchedEvaluator>::with_capacity(matched_clauses.len());
        // check clause semantic
        MergeIntoStmt::check_multi_match_clauses_semantic(&matched_clauses)?;
        MergeIntoStmt::check_multi_unmatch_clauses_semantic(&unmatched_clauses)?;

        let (catalog_name, database_name, table_name) =
            self.normalize_object_identifier_triple(catalog, database, table_ident);

        let table = self
            .ctx
            .get_table(&catalog_name, &database_name, &table_name)
            .await?;
        let table_id = table.get_id();
        let table_schema = table.schema();

        // get target_table_reference
        let target_table = TableReference::Table {
            span: None,
            catalog: catalog.clone(),
            database: database.clone(),
            table: table_ident.clone(),
            alias: target_alias.clone(),
            travel_point: None,
            pivot: None,
            unpivot: None,
        };

        // get_source_table_reference
        let source_data = source.transform_table_reference();

        // bind source data
        let (source_expr, mut left_context) =
            self.bind_single_table(bind_context, &source_data).await?;

        // add all left source columns for read
        // todo: (JackTan25) do column prune after finish "split expr for target and source"
        let mut columns_set = left_context.column_set();

        let update_columns_star = if self.has_star_clause(&matched_clauses, &unmatched_clauses) {
            // when there are "update *"/"insert *", we need to get the index of correlated columns in source.
            let default_target_table_schema = table.schema().remove_computed_fields();
            let mut update_columns = HashMap::with_capacity(
                default_target_table_schema
                    .remove_computed_fields()
                    .num_fields(),
            );
            let source_output_columns = &left_context.columns;
            // we use Vec as the value, because if there could be duplicate names
            let mut name_map = HashMap::<String, Vec<IndexType>>::new();
            for column in source_output_columns {
                name_map
                    .entry(column.column_name.clone())
                    .or_insert_with(|| vec![])
                    .push(column.index);
            }

            for (field_idx, field) in default_target_table_schema.fields.iter().enumerate() {
                let index = match name_map.get(field.name()) {
                    None => {
                        return Err(ErrorCode::SemanticError(
                            format!("can't find {} in source output", field.name).to_string(),
                        ));
                    }
                    Some(indices) => {
                        if indices.len() != 1 {
                            return Err(ErrorCode::SemanticError(
                                format!(
                                    "there should be only one {} in source output,but we get {}",
                                    field.name,
                                    indices.len()
                                )
                                .to_string(),
                            ));
                        } else {
                            indices[0]
                        }
                    }
                };
                let column = ColumnBindingBuilder::new(
                    field.name.to_string(),
                    index,
                    Box::new(field.data_type().into()),
                    Visibility::Visible,
                )
                .build();
                let col = ScalarExpr::BoundColumnRef(BoundColumnRef { span: None, column });
                update_columns.insert(field_idx, col);
            }
            Some(update_columns)
        } else {
            None
        };

        // Todo: (JackTan25) Maybe we can remove bind target_table
        // when the target table has been binded in bind_merge_into_source
        // bind table for target table
        let (mut target_expr, mut right_context) = self
            .bind_single_table(&mut left_context, &target_table)
            .await?;

        // add internal_column (_row_id)
        let table_index = self
            .metadata
            .read()
            .get_table_index(Some(database_name.as_str()), table_name.as_str())
            .expect("can't get target_table binding");

        let row_id_column_binding = InternalColumnBinding {
            database_name: Some(database_name.clone()),
            table_name: Some(table_name.clone()),
            internal_column: InternalColumn {
                column_name: ROW_ID_COL_NAME.to_string(),
                column_type: InternalColumnType::RowId,
            },
        };

        let column_binding = right_context
            .add_internal_column_binding(&row_id_column_binding, self.metadata.clone())?;

        target_expr =
            SExpr::add_internal_column_index(&target_expr, table_index, column_binding.index);

        self.metadata
            .write()
            .set_table_row_id_index(table_index, column_binding.index);
        // add row_id_idx
        columns_set.insert(column_binding.index);

        // add join,use left outer join in V1, we use _row_id to check_duplicate join row.
        let join = Join {
            op: LeftOuter,
            condition: JoinCondition::On(Box::new(join_expr.clone())),
            left: Box::new(source_data.clone()),
            right: Box::new(target_table),
        };

        let (join_sexpr, mut bind_ctx) = self
            .bind_join(
                bind_context,
                left_context,
                right_context.clone(),
                source_expr,
                target_expr,
                &join,
            )
            .await?;

        let name_resolution_ctx = self.name_resolution_ctx.clone();
        let mut scalar_binder = ScalarBinder::new(
            &mut bind_ctx,
            self.ctx.clone(),
            &name_resolution_ctx,
            self.metadata.clone(),
            &[],
            HashMap::new(),
            Box::new(IndexMap::new()),
        );
        // add join condition used column idx
        columns_set = columns_set
            .union(&scalar_binder.bind(join_expr).await?.0.used_columns())
            .cloned()
            .collect();

        let column_entries = self.metadata.read().columns_by_table_index(table_index);
        let mut field_index_map = HashMap::<usize, String>::new();
        // if true, read all columns of target table
        let has_update = self.has_update(&matched_clauses);
        if has_update {
            for (idx, field) in table_schema.fields().iter().enumerate() {
                let used_idx = self.find_column_index(&column_entries, &field.name())?;
                columns_set.insert(used_idx);
                field_index_map.insert(idx, used_idx.to_string());
            }
        }

        // bind matched clause columns and add update fields and exprs
        for clause in &matched_clauses {
            matched_evaluators.push(
                self.bind_matched_clause(
                    &mut scalar_binder,
                    clause,
                    &mut columns_set,
                    table_schema.clone(),
                    update_columns_star.clone(),
                )
                .await?,
            );
        }

        // bind not matched clause columns and add insert exprs
        for clause in &unmatched_clauses {
            unmatched_evaluators.push(
                self.bind_unmatched_clause(
                    &mut scalar_binder,
                    clause,
                    &mut columns_set,
                    table_schema.clone(),
                    update_columns_star.clone(),
                )
                .await?,
            );
        }

        Ok(Plan::MergeInto(Box::new(MergeInto {
            catalog: catalog_name.to_string(),
            database: database_name.to_string(),
            table: table_name,
            target_alias: target_alias.clone(),
            table_id,
            bind_context: Box::new(bind_ctx.clone()),
            meta_data: self.metadata.clone(),
            input: Box::new(join_sexpr.clone()),
            columns_set: Box::new(columns_set),
            matched_evaluators,
            unmatched_evaluators,
            target_table_idx: table_index,
            field_index_map,
        })))
    }

    async fn bind_matched_clause<'a>(
        &mut self,
        scalar_binder: &mut ScalarBinder<'a>,
        clause: &MatchedClause,
        columns: &mut HashSet<IndexType>,
        schema: TableSchemaRef,
        update_columns_star: Option<HashMap<FieldIndex, ScalarExpr>>,
    ) -> Result<MatchedEvaluator> {
        // not supported for update clauses
        let f = |scalar: &ScalarExpr| {
            matches!(
                scalar,
                ScalarExpr::WindowFunction(_)
                    | ScalarExpr::AggregateFunction(_)
                    | ScalarExpr::SubqueryExpr(_)
            )
        };

        let condition = if let Some(expr) = &clause.selection {
            let (scalar_expr, _) = scalar_binder.bind(expr).await?;
            for idx in scalar_expr.used_columns() {
                columns.insert(idx);
            }
            let finder = Finder::new(&f);
            let finder = scalar_expr.accept(finder)?;
            if !finder.scalars().is_empty() {
                return Err(ErrorCode::SemanticError(
                    "update clause's condition can't contain subquery|window|aggregate functions"
                        .to_string(),
                )
                .set_span(scalar_expr.span()));
            }
            Some(scalar_expr)
        } else {
            None
        };

        if let MatchOperation::Update {
            update_list,
            is_star,
        } = &clause.operation
        {
            if *is_star {
                Ok(MatchedEvaluator {
                    condition,
                    update: update_columns_star,
                })
            } else {
                let mut update_columns = HashMap::with_capacity(update_list.len());
                for update_expr in update_list {
                    let (scalar_expr, _) = scalar_binder.bind(&update_expr.expr).await?;
                    let col_name =
                        normalize_identifier(&update_expr.name, &self.name_resolution_ctx).name;
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

                    let finder = Finder::new(&f);
                    let finder = scalar_expr.accept(finder)?;
                    if !finder.scalars().is_empty() {
                        return Err(ErrorCode::SemanticError(
                            "update_list in update clause can't contain subquery|window|aggregate functions".to_string(),
                        )
                        .set_span(scalar_expr.span()));
                    }

                    update_columns.insert(index, scalar_expr.clone());
                }

                Ok(MatchedEvaluator {
                    condition,
                    update: Some(update_columns),
                })
            }
        } else {
            // delete
            Ok(MatchedEvaluator {
                condition,
                update: None,
            })
        }
    }

    async fn bind_unmatched_clause<'a>(
        &mut self,
        scalar_binder: &mut ScalarBinder<'a>,
        clause: &UnmatchedClause,
        columns: &mut HashSet<IndexType>,
        table_schema: TableSchemaRef,
        update_columns_star: Option<HashMap<FieldIndex, ScalarExpr>>,
    ) -> Result<UnmatchedEvaluator> {
        let condition = if let Some(expr) = &clause.selection {
            let (scalar_expr, _) = scalar_binder.bind(expr).await?;
            for idx in scalar_expr.used_columns() {
                columns.insert(idx);
            }
            Some(scalar_expr)
        } else {
            None
        };
        if clause.insert_operation.is_star {
            let default_schema = table_schema.remove_computed_fields();
            let mut values = Vec::with_capacity(default_schema.num_fields());
            let update_columns_star = update_columns_star.unwrap();
            for idx in 0..default_schema.num_fields() {
                let scalar = update_columns_star.get(&idx).unwrap().clone();
                // cast expr
                values.push(wrap_cast_scalar(
                    &scalar,
                    &scalar.data_type()?,
                    &DataType::from(default_schema.field(idx).data_type()),
                )?);
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
                let (mut scalar_expr, _) = scalar_binder.bind(expr).await?;
                // type cast
                scalar_expr = wrap_cast_scalar(
                    &scalar_expr,
                    &scalar_expr.data_type()?,
                    &DataType::from(source_schema.field(idx).data_type()),
                )?;

                values.push(scalar_expr.clone());
                for idx in scalar_expr.used_columns() {
                    columns.insert(idx);
                }
            }

            Ok(UnmatchedEvaluator {
                source_schema: Arc::new(source_schema.into()),
                condition,
                values,
            })
        }
    }

    fn find_column_index(
        &self,
        column_entries: &Vec<ColumnEntry>,
        col_name: &str,
    ) -> Result<usize> {
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
            if let MatchOperation::Update {
                update_list: _,
                is_star: _,
            } = clause.operation
            {
                return true;
            }
        }
        false
    }

    fn has_star_clause(
        &self,
        matched_clauses: &Vec<MatchedClause>,
        unmatched_clauses: &Vec<UnmatchedClause>,
    ) -> bool {
        for item in matched_clauses {
            if let MatchOperation::Update {
                update_list: _,
                is_star,
            } = item.operation
            {
                if is_star {
                    return true;
                }
            }
        }

        for item in unmatched_clauses {
            if item.insert_operation.is_star {
                return true;
            }
        }
        false
    }
}
