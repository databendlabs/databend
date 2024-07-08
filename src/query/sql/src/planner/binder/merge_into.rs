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
use databend_common_ast::ast::Join;
use databend_common_ast::ast::JoinCondition;
use databend_common_ast::ast::JoinOperator;
use databend_common_ast::ast::JoinOperator::Inner;
use databend_common_ast::ast::JoinOperator::RightAnti;
use databend_common_ast::ast::JoinOperator::RightOuter;
use databend_common_ast::ast::MatchOperation;
use databend_common_ast::ast::MatchedClause;
use databend_common_ast::ast::MergeIntoStmt;
use databend_common_ast::ast::TableReference;
use databend_common_ast::ast::UnmatchedClause;
use databend_common_catalog::lock::LockTableOption;
use databend_common_catalog::plan::InternalColumn;
use databend_common_catalog::plan::InternalColumnType;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::FieldIndex;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::ROW_ID_COL_NAME;
use indexmap::IndexMap;

use crate::binder::wrap_cast;
use crate::binder::Binder;
use crate::binder::InternalColumnBinding;
use crate::normalize_identifier;
use crate::optimizer::RelExpr;
use crate::optimizer::SExpr;
use crate::plans::BoundColumnRef;
use crate::plans::MatchedEvaluator;
use crate::plans::MaterializedCte;
use crate::plans::MergeInto;
use crate::plans::Plan;
use crate::plans::RelOperator;
use crate::plans::UnmatchedEvaluator;
use crate::BindContext;
use crate::ColumnBinding;
use crate::ColumnBindingBuilder;
use crate::ColumnEntry;
use crate::IndexType;
use crate::ScalarBinder;
use crate::ScalarExpr;
use crate::Visibility;
use crate::DUMMY_TABLE_INDEX;

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum MergeIntoType {
    MatchedOnly,
    FullOperation,
    InsertOnly,
}

// Optimize Rule:
// for now we think right source table is small table in default.
// 1. insert only:
//      right anti join
// 2. (matched and unmatched)
//      right outer
// 3. matched only:
//      inner join
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
                "merge into is experimental for now, you can use 'set enable_experimental_merge_into = 1' to set it up",
            ));
        };

        let (matched_clauses, unmatched_clauses) = stmt.split_clauses();
        let merge_type = get_merge_type(matched_clauses.len(), unmatched_clauses.len())?;

        let s_expr = self
            .bind_merge_into_with_join_type(
                bind_context,
                stmt,
                match merge_type {
                    MergeIntoType::MatchedOnly => Inner,
                    MergeIntoType::InsertOnly => RightAnti,
                    _ => RightOuter,
                },
                matched_clauses.clone(),
                unmatched_clauses.clone(),
                merge_type.clone(),
            )
            .await?;

        let mut merge_into_plan = s_expr.plan().clone().try_into()?;
        if merge_type == MergeIntoType::InsertOnly && !insert_only(&merge_into_plan) {
            return Err(ErrorCode::SemanticError(
                "for unmatched clause, then condition and exprs can only have source fields",
            ));
        }

        Ok(Plan::MergeInto {
            s_expr: Box::new(s_expr),
            schema: merge_into_plan.schema(),
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

    fn try_add_internal_column_binding(
        &mut self,
        table: &TableReference,
        context: &mut BindContext,
        expr: &mut SExpr,
        table_index: &mut usize,
        row_id_index: &mut Option<usize>,
    ) -> Result<()> {
        if let TableReference::Table {
            catalog,
            database,
            table,
            ..
        } = table
        {
            let (_, database_name, table_name) =
                self.normalize_object_identifier_triple(catalog, database, table);

            // add internal_column (_row_id)
            *table_index = self
                .metadata
                .read()
                .get_table_index(Some(database_name.as_str()), table_name.as_str())
                .ok_or_else(|| ErrorCode::Internal("can't get table binding"))?;

            let row_id_column_binding = InternalColumnBinding {
                database_name: Some(database_name.clone()),
                table_name: Some(table_name.clone()),
                internal_column: InternalColumn {
                    column_name: ROW_ID_COL_NAME.to_string(),
                    column_type: InternalColumnType::RowId,
                },
            };

            let column_binding = context.add_internal_column_binding(
                &row_id_column_binding,
                self.metadata.clone(),
                true,
            )?;

            let row_id_idx: usize = column_binding.index;

            *expr = SExpr::add_internal_column_index(expr, *table_index, row_id_idx, &None);

            self.metadata
                .write()
                .set_table_row_id_index(*table_index, row_id_idx);

            *row_id_index = Some(row_id_idx);
        }
        Ok(())
    }

    async fn bind_merge_into_with_join_type(
        &mut self,
        bind_context: &mut BindContext,
        stmt: &MergeIntoStmt,
        join_type: JoinOperator,
        matched_clauses: Vec<MatchedClause>,
        unmatched_clauses: Vec<UnmatchedClause>,
        merge_type: MergeIntoType,
    ) -> Result<SExpr> {
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
        let settings = self.ctx.get_settings();

        if merge_options.is_empty() {
            return Err(ErrorCode::BadArguments(
                "at least one matched or unmatched clause for merge into",
            ));
        }

        let mut unmatched_evaluators =
            Vec::<UnmatchedEvaluator>::with_capacity(unmatched_clauses.len());
        let mut matched_evaluators = Vec::<MatchedEvaluator>::with_capacity(matched_clauses.len());
        // check clause semantic
        MergeIntoStmt::check_multi_match_clauses_semantic(&matched_clauses)?;
        MergeIntoStmt::check_multi_unmatch_clauses_semantic(&unmatched_clauses)?;

        let (catalog_name, database_name, table_name) =
            self.normalize_object_identifier_triple(catalog, database, table_ident);

        // Add table lock before execution.
        let lock_guard = if merge_type != MergeIntoType::InsertOnly {
            self.ctx
                .clone()
                .acquire_table_lock(
                    &catalog_name,
                    &database_name,
                    &table_name,
                    &LockTableOption::LockWithRetry,
                )
                .await?
        } else {
            None
        };

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
            temporal: None,
            consume: false,
            pivot: None,
            unpivot: None,
        };

        // get_source_table_reference
        let source_data = source.transform_table_reference();

        // bind source data
        let (mut source_expr, mut source_context) =
            self.bind_table_reference(bind_context, &source_data)?;

        // try add internal_column (_row_id) for source_table
        let mut source_table_index = DUMMY_TABLE_INDEX;
        let mut source_row_id_index = None;

        if settings.get_enable_distributed_merge_into()? {
            self.try_add_internal_column_binding(
                &source_data,
                &mut source_context,
                &mut source_expr,
                &mut source_table_index,
                &mut source_row_id_index,
            )?;
        }

        // remove stream column.
        source_context
            .columns
            .retain(|v| v.visibility == Visibility::Visible);

        // Wrap `LogicalMaterializedCte` to `source_expr`
        for (_, cte_info) in self.ctes_map.iter().rev() {
            if !cte_info.materialized || cte_info.used_count == 0 {
                continue;
            }
            let cte_s_expr = self.m_cte_bound_s_expr.get(&cte_info.cte_idx).unwrap();
            let left_output_columns = cte_info.columns.clone();
            source_expr = SExpr::create_binary(
                Arc::new(RelOperator::MaterializedCte(MaterializedCte {
                    left_output_columns,
                    cte_idx: cte_info.cte_idx,
                })),
                Arc::new(cte_s_expr.clone()),
                Arc::new(source_expr),
            );
        }

        let update_or_insert_columns_star =
            if self.has_star_clause(&matched_clauses, &unmatched_clauses) {
                // when there are "update *"/"insert *", we need to get the index of correlated columns in source.
                let default_target_table_schema = table_schema.remove_computed_fields();
                let mut update_columns =
                    HashMap::with_capacity(default_target_table_schema.num_fields());
                // we use Vec as the value, because there could be duplicate names
                let mut name_map = HashMap::<String, Vec<ColumnBinding>>::new();
                for column in source_context.columns.iter() {
                    name_map
                        .entry(column.column_name.clone())
                        .or_default()
                        .push(column.clone());
                }

                for (field_idx, field) in default_target_table_schema.fields.iter().enumerate() {
                    let column = match name_map.get(field.name()) {
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
                            }

                            indices[0].clone()
                        }
                    };
                    let column = ColumnBindingBuilder::new(
                        field.name.to_string(),
                        column.index,
                        column.data_type.clone(),
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
        let (mut target_expr, mut target_context) =
            self.bind_table_reference(bind_context, &target_table)?;

        if table.change_tracking_enabled() && merge_type != MergeIntoType::InsertOnly {
            if let RelOperator::Scan(scan) = target_expr.plan() {
                let new_scan = scan.update_stream_columns(true);
                target_expr = SExpr::create_leaf(Arc::new(new_scan.into()))
            }
        }

        // add internal_column (_row_id) for target_table
        let mut target_table_index = DUMMY_TABLE_INDEX;
        let mut target_row_id_index = None;
        self.try_add_internal_column_binding(
            &target_table,
            &mut target_context,
            &mut target_expr,
            &mut target_table_index,
            &mut target_row_id_index,
        )?;

        let row_id_index = target_row_id_index
            .ok_or_else(|| ErrorCode::InvalidRowIdIndex("can't get target_table row_id"))?;

        // add all left source columns for read
        // todo: (JackTan25) do column prune after finish "split expr for target and source"
        let mut columns_set = HashSet::<IndexType>::new();

        // add row_id_idx
        if merge_type != MergeIntoType::InsertOnly {
            columns_set.insert(row_id_index);
        }

        // add join, we use _row_id to check_duplicate join row.
        let join = Join {
            op: join_type,
            condition: JoinCondition::On(Box::new(join_expr.clone())),
            left: Box::new(target_table),
            // use source as build table
            right: Box::new(source_data.clone()),
        };

        let target_prop = RelExpr::with_s_expr(&target_expr).derive_relational_prop()?;
        let (join_sexpr, mut bind_ctx) = self
            .bind_merge_into_join(
                bind_context,
                target_context.clone(),
                source_context,
                target_expr,
                source_expr,
                &join,
            )
            .await?;
        {
            // add join used column idx
            let join_column_set = bind_ctx.column_set();
            columns_set = columns_set.union(&join_column_set).cloned().collect();
        }

        // Collect lazy columns.
        let join: crate::plans::Join = join_sexpr.plan().clone().try_into()?;
        let mut required_columns = HashSet::new();
        for condition in join.equi_conditions.iter() {
            let left_condition = &condition.left;
            let right_condition = &condition.right;
            let left_used_columns = left_condition.used_columns();
            let right_used_columns = right_condition.used_columns();
            if left_used_columns.is_subset(&target_prop.output_columns) {
                required_columns.extend(left_used_columns);
            } else if right_used_columns.is_subset(&target_prop.output_columns) {
                required_columns.extend(right_used_columns);
            }
        }
        for condition in join.non_equi_conditions.iter() {
            for column_index in condition.used_columns() {
                if target_prop.output_columns.contains(&column_index) {
                    required_columns.insert(column_index);
                }
            }
        }

        let target_table_position = target_table_position(&join_sexpr, target_table_index)?;
        let target_side_child = join_sexpr.child(target_table_position)?;
        assert!(matches!(
            target_side_child.plan(),
            RelOperator::Scan(_) | RelOperator::Filter(_)
        ));
        if let RelOperator::Filter(filter) = target_side_child.plan() {
            for predicate in filter.predicates.iter() {
                for column_index in predicate.used_columns() {
                    if target_prop.output_columns.contains(&column_index) {
                        required_columns.insert(column_index);
                    }
                }
            }
        }

        let mut lazy_columns = HashSet::new();
        for column in &target_context.columns {
            if !required_columns.contains(&column.index)
                && column.visibility != Visibility::InVisible
            {
                lazy_columns.insert(column.index);
            }
        }

        // If the table alias is not None, after the binding phase, the bound columns will have
        // a database of 'None' and the table named as the alias.
        // Thus, we adjust them accordingly.
        let target_name = if let Some(target_identify) = target_alias {
            normalize_identifier(&target_identify.name, &self.name_resolution_ctx)
                .name
                .clone()
        } else {
            table_name.clone()
        };

        let has_update = self.has_update(&matched_clauses);
        let update_row_version = if table.change_tracking_enabled() && has_update {
            Some(Self::update_row_version(
                table.schema_with_stream(),
                &bind_ctx.columns,
                if target_alias.is_none() {
                    Some(&database_name)
                } else {
                    None
                },
                Some(&target_name),
            )?)
        } else {
            None
        };

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

        let column_entries = self
            .metadata
            .read()
            .columns_by_table_index(target_table_index);
        let mut field_index_map = HashMap::<usize, String>::new();
        // if true, read all columns of target table
        if has_update {
            for (idx, field) in table.schema_with_stream().fields().iter().enumerate() {
                let used_idx = self.find_column_index(&column_entries, field.name())?;
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
                    update_or_insert_columns_star.clone(),
                    update_row_version.clone(),
                    target_name.as_ref(),
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
                    update_or_insert_columns_star.clone(),
                )
                .await?,
            );
        }

        let merge_into = MergeInto {
            catalog: catalog_name.to_string(),
            database: database_name.to_string(),
            table: table_name,
            target_alias: target_alias.clone(),
            table_id,
            bind_context: Box::new(bind_ctx),
            meta_data: self.metadata.clone(),
            columns_set: Box::new(columns_set),
            matched_evaluators,
            unmatched_evaluators,
            target_table_index,
            field_index_map,
            merge_type,
            distributed: false,
            change_join_order: false,
            row_id_index,
            source_row_id_index,
            can_try_update_column_only: self.can_try_update_column_only(&matched_clauses),
            enable_right_broadcast: false,
            lazy_columns,
            lock_guard,
        };

        let s_expr = SExpr::create_unary(
            Arc::new(RelOperator::MergeInto(merge_into)),
            Arc::new(join_sexpr),
        );

        Ok(s_expr)
    }

    async fn bind_matched_clause<'a>(
        &mut self,
        scalar_binder: &mut ScalarBinder<'a>,
        clause: &MatchedClause,
        columns: &mut HashSet<IndexType>,
        schema: TableSchemaRef,
        update_or_insert_columns_star: Option<HashMap<FieldIndex, ScalarExpr>>,
        update_row_version: Option<(FieldIndex, ScalarExpr)>,
        target_name: &str,
    ) -> Result<MatchedEvaluator> {
        let condition = if let Some(expr) = &clause.selection {
            let (scalar_expr, _) = scalar_binder.bind(expr)?;
            for idx in scalar_expr.used_columns() {
                columns.insert(idx);
            }

            if !self.check_allowed_scalar_expr(&scalar_expr)? {
                return Err(ErrorCode::SemanticError(
                    "matched clause's condition can't contain subquery|window|aggregate|async functions"
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
                            "update clause's can't contain subquery|window|aggregate|async functions"
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
        columns: &mut HashSet<IndexType>,
        table_schema: TableSchemaRef,
        update_or_insert_columns_star: Option<HashMap<FieldIndex, ScalarExpr>>,
    ) -> Result<UnmatchedEvaluator> {
        let condition = if let Some(expr) = &clause.selection {
            let (scalar_expr, _) = scalar_binder.bind(expr)?;
            if !self.check_allowed_scalar_expr(&scalar_expr)? {
                return Err(ErrorCode::SemanticError(
                    "unmatched clause's condition can't contain subquery|window|aggregate|async functions"
                        .to_string(),
                )
                .set_span(scalar_expr.span()));
            }

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
                        "insert clause's can't contain subquery|window|aggregate|async functions"
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

fn get_merge_type(matched_len: usize, unmatched_len: usize) -> Result<MergeIntoType> {
    if matched_len == 0 && unmatched_len > 0 {
        Ok(MergeIntoType::InsertOnly)
    } else if unmatched_len == 0 && matched_len > 0 {
        Ok(MergeIntoType::MatchedOnly)
    } else if unmatched_len > 0 && matched_len > 0 {
        Ok(MergeIntoType::FullOperation)
    } else {
        Err(ErrorCode::SemanticError(
            "we must have matched or unmatched clause at least one",
        ))
    }
}

fn insert_only(merge_plan: &MergeInto) -> bool {
    let meta_data = merge_plan.meta_data.read();
    let target_table_columns: HashSet<usize> = meta_data
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

pub fn target_table_position(join_s_expr: &SExpr, target_table_index: usize) -> Result<usize> {
    fn contains_target_table(s_expr: &SExpr, target_table_index: usize) -> bool {
        if let RelOperator::Scan(ref scan) = s_expr.plan() {
            scan.table_index == target_table_index
        } else {
            s_expr
                .children()
                .any(|child| contains_target_table(child, target_table_index))
        }
    }

    debug_assert!(matches!(join_s_expr.plan(), RelOperator::Join(_)));
    if contains_target_table(join_s_expr.child(0)?, target_table_index) {
        Ok(0)
    } else {
        Ok(1)
    }
}
