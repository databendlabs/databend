// Copyright 2020 Datafuse Labs.
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

use common_datavalues::DataType;
use common_exception::ErrorCode;
use common_exception::Result;

use crate::catalogs::catalog::Catalog;
use crate::sessions::DatafuseQueryContextRef;
use crate::sql::expression::Expression;
use crate::sql::parser::ast::Identifier;
use crate::sql::parser::ast::Indirection;
use crate::sql::parser::ast::Join;
use crate::sql::parser::ast::JoinCondition;
use crate::sql::parser::ast::JoinOperator;
use crate::sql::parser::ast::Query;
use crate::sql::parser::ast::SelectStmt;
use crate::sql::parser::ast::SelectTarget;
use crate::sql::parser::ast::SetExpr;
use crate::sql::parser::ast::Statement;
use crate::sql::parser::ast::TableAlias;
use crate::sql::parser::ast::TableReference;
use crate::sql::planner::expression_binder::ExpressionBinder;
use crate::sql::planner::logical_plan::LogicalAggregation;
use crate::sql::planner::logical_plan::LogicalEquiJoin;
use crate::sql::planner::logical_plan::LogicalFilter;
use crate::sql::planner::logical_plan::LogicalGet;
use crate::sql::planner::logical_plan::LogicalPlan;
use crate::sql::planner::logical_plan::LogicalProjection;
use crate::sql::planner::IndexType;

// Intermediate structures of binding TableReference
#[derive(Debug)]
struct BoundTableReference {
    pub plan: LogicalPlan,
}

// Intermediate structures of binding Statement
#[derive(Debug)]
pub enum BoundStatementType {
    SelectStatement,
}

#[derive(Debug)]
pub struct BoundStatement {
    pub tp: BoundStatementType,

    pub plan: LogicalPlan,
}

// Intermediate structures of binding Query
#[derive(Debug)]
struct BoundQuery {
    // Index of query root result set
    pub index: IndexType,
    pub columns: Vec<String>,
    pub data_types: Vec<DataType>,
    pub plan: LogicalPlan,
}

pub struct Binder<'a> {
    context: DatafuseQueryContextRef,
    // Root BindContext of binding procedure
    bind_context: BindContext,
    stmt: &'a Statement,

    catalog: &'a dyn Catalog,
}

impl<'a> Binder<'a> {
    pub fn new(
        stmt: &'a Statement,
        catalog: &'a dyn Catalog,
        context: DatafuseQueryContextRef,
    ) -> Self {
        Binder {
            context,
            bind_context: BindContext::new(0),
            stmt,

            // TODO(leiysky): create Catalog object in a more reasonable way
            catalog,
        }
    }

    fn new_with_parent(parent: &Binder<'a>) -> Self {
        Binder {
            context: parent.context.clone(),
            bind_context: BindContext::new(parent.bind_context.table_index_count),
            stmt: parent.stmt,
            catalog: parent.catalog,
        }
    }

    pub fn bind(mut self) -> Result<BoundStatement> {
        self.bind_statement(self.stmt)
    }

    fn bind_statement(&mut self, stmt: &Statement) -> Result<BoundStatement> {
        match stmt {
            Statement::Select(query) => {
                let bound_query = self.bind_query(&query)?;
                Ok(BoundStatement {
                    tp: BoundStatementType::SelectStatement,
                    plan: bound_query.plan,
                })
            }
            _ => {
                todo!()
            }
        }
    }

    fn bind_query(&mut self, query: &Query) -> Result<BoundQuery> {
        self.bind_set_expr(&query.body)
    }

    fn bind_set_expr(&mut self, set_expr: &SetExpr) -> Result<BoundQuery> {
        match set_expr {
            SetExpr::Select(select) => self.bind_select_stmt(&select),
            SetExpr::Query(query) => self.bind_query(&query),
            _ => Err(ErrorCode::UnImplement("Unsupported UNION clause")),
        }
    }

    fn bind_select_stmt(&mut self, select_stmt: &SelectStmt) -> Result<BoundQuery> {
        let bound_table_reference = self.bind_table_reference(&select_stmt.from)?;
        let mut plan = bound_table_reference.plan;
        let bind_context = &self.bind_context;

        if let Some(expr) = &select_stmt.selection {
            let mut binder = ExpressionBinder::new(bind_context);
            let result = binder.bind(expr)?;
            let predicates = Expression::split_predicates(result);
            let filter = LogicalFilter::new(predicates, plan);
            plan = LogicalPlan::Filter(filter);
        }

        // Resolve select list first, since we may have GROUP BY clause that depends on this.
        // Projections with index as position, `(Expression, Alias)`
        let mut positional_projections: Vec<(Expression, String)> = vec![];
        {
            let mut binder = ExpressionBinder::new(bind_context);
            for item in select_stmt.select_list.iter() {
                let mut projections =
                    Self::resolve_select_target(item, &bind_context, &mut binder)?;
                positional_projections.append(&mut projections);
            }
        }
        // Make immutable
        let positional_projections = positional_projections;

        // GROUP BY clause
        if !select_stmt.group_by.is_empty() {
            let mut projection_column_bindings: HashSet<ColumnBinding> = HashSet::new();
            for (expr, _) in positional_projections.iter() {
                if expr.is_aggregate() {
                    // Skip aggregate function
                    continue;
                }
                // TODO(leiysky): use a function like `get_non_aggregated_column_bindings` instead
                projection_column_bindings = projection_column_bindings
                    .union(&expr.get_column_bindings())
                    .cloned()
                    .collect();
            }
            let mut group_keys: HashSet<ColumnBinding> = HashSet::new();
            // Collect GROUP BY expressions
            let group_bys: Vec<Expression> = select_stmt
                .group_by
                .iter()
                .map(|expr| {
                    let mut binder = ExpressionBinder::new(bind_context);
                    binder.bind(expr)
                })
                .collect::<Result<_>>()?;
            // Do some validity check
            LogicalAggregation::check_group_by(&group_bys)?;
            for expr in group_bys.iter() {
                group_keys = group_keys
                    .union(&expr.get_column_bindings())
                    .cloned()
                    .collect();
            }
            // Projection set should be subset of group keys
            if !projection_column_bindings.is_subset(&group_keys) {
                return Err(ErrorCode::InvalidColumnAlias("result columns must appear in the GROUP BY clause or be used in an aggregate function"));
            }

            let agg_funcs: Vec<Expression> = positional_projections
                .iter()
                .map(|(expr, _)| expr.get_aggregate_functions())
                .flatten()
                .collect();

            let aggregation_plan = LogicalAggregation::new(group_bys, agg_funcs, plan);
            plan = LogicalPlan::Aggregation(aggregation_plan);
        }

        // HAVING clause
        if let Some(expr) = &select_stmt.having {
            let mut binder = ExpressionBinder::new(bind_context);
            let expression = binder.bind(expr)?;
            let predicates = Expression::split_predicates(expression);
            let having_plan = LogicalFilter::new(predicates, plan);
            plan = LogicalPlan::Filter(having_plan);
        }

        let projection = LogicalProjection::new(positional_projections, plan);
        let query_index = self.bind_context.next_table_index();
        let (columns, data_types): (Vec<String>, Vec<DataType>) = projection
            .alias
            .iter()
            .map(|(expr, name)| (name.to_owned(), expr.data_type()))
            .unzip();
        plan = LogicalPlan::Projection(projection);

        Ok(BoundQuery {
            index: query_index,
            columns,
            data_types,
            plan,
        })
    }

    fn bind_table_reference(
        &mut self,
        table_reference: &TableReference,
    ) -> Result<BoundTableReference> {
        match table_reference {
            TableReference::Table {
                database,
                table,
                alias,
            } => self.bind_table(database, table, alias),
            TableReference::Subquery { subquery, alias } => self.bind_subquery(subquery, alias),
            TableReference::Join(join) => self.bind_join(join),
            TableReference::TableFunction { .. } => {
                Err(ErrorCode::UnImplement("Table function is unsupported"))
            }
        }
    }

    fn bind_table(
        &mut self,
        database: &Option<Identifier>,
        table_ident: &Identifier,
        alias: &Option<TableAlias>,
    ) -> Result<BoundTableReference> {
        let db_name = database
            .clone()
            .map_or(self.context.get_current_database(), |ident| {
                ident.get_name()
            });
        let mut table_name = table_ident.get_name();
        let original_table_name = table_name.clone();

        // Get table metadata from catalog
        // TODO(leiysky): maybe use a more reasonable handle to maintain the information?
        let table_meta = self
            .catalog
            .get_table(db_name.as_str(), table_name.as_str())?;
        let schema = table_meta.datasource().schema()?;
        let fields = schema.fields();

        // Replace table name with alias and alias columns
        let mut columns: Vec<String> = fields.iter().map(|field| field.name().to_owned()).collect();
        if let Some(alias) = alias {
            table_name = alias.name.name.to_owned();
            columns = Self::build_alias_column_names(
                table_name.as_str(),
                columns,
                alias.columns.iter().map(|ident| ident.get_name()).collect(),
            )?;
        }
        // Collect data types
        let data_types: Vec<DataType> = fields
            .iter()
            .map(|field| field.data_type().to_owned())
            .collect();

        // Get table index for current TableReference
        let index = self.bind_context.next_table_index();

        let table_binding = TableBinding::new(
            index,
            table_name.clone(),
            columns.clone(),
            data_types.clone(),
        );
        self.bind_context.add_table_binding(table_binding);

        Ok(BoundTableReference {
            plan: LogicalPlan::Get(LogicalGet::new(
                db_name,
                original_table_name,
                table_name,
                index,
                columns,
                data_types,
            )),
        })
    }

    fn bind_join(&mut self, join: &Join) -> Result<BoundTableReference> {
        match join.condition {
            JoinCondition::Using(_) => {
                return Err(ErrorCode::SyntaxException(
                    "Unsupported join condition: USING",
                ));
            }
            JoinCondition::Natural => {
                return Err(ErrorCode::SyntaxException(
                    "Unsupported join condition: NATURAL",
                ));
            }
            _ => {}
        }
        match join.op {
            JoinOperator::CrossJoin => {
                return Err(ErrorCode::SyntaxException(
                    "Unsupported join type: CROSS JOIN",
                ));
            }
            _ => {}
        }

        let mut left_binder = Binder::new_with_parent(&self);
        let left_result = left_binder.bind_table_reference(&join.left)?;
        self.bind_context
            .merge_bind_context(left_binder.bind_context)?;

        // Create right_binder after bound left table to make sure table index count is updated
        let mut right_binder = Binder::new_with_parent(&self);
        let right_result = right_binder.bind_table_reference(&join.right)?;
        self.bind_context
            .merge_bind_context(right_binder.bind_context)?;

        let mut join_condition: Vec<Expression> = vec![];
        if let JoinCondition::On(expr) = &join.condition {
            let mut expr_binder = ExpressionBinder::new(&self.bind_context);
            let predicate = expr_binder.bind(expr)?;
            // Conjunctions split by AND expression
            join_condition = Expression::split_predicates(predicate);
        }

        let join_plan = LogicalEquiJoin::new(
            join.op.to_owned(),
            join_condition,
            left_result.plan,
            right_result.plan,
        )?;
        Ok(BoundTableReference {
            plan: LogicalPlan::EquiJoin(join_plan),
        })
    }

    fn bind_subquery(
        &mut self,
        subquery: &Query,
        alias: &TableAlias,
    ) -> Result<BoundTableReference> {
        let mut binder = Binder::new_with_parent(&self);
        let result = binder.bind_query(subquery)?;
        let columns = Self::build_alias_column_names(
            alias.name.get_name().as_str(),
            result.columns,
            alias.columns.iter().map(|ident| ident.get_name()).collect(),
        )?;
        let data_types = result.data_types;
        let index = result.index;
        let table_binding = TableBinding::new(index, alias.name.get_name(), columns, data_types);
        self.bind_context.add_table_binding(table_binding);
        Ok(BoundTableReference { plan: result.plan })
    }

    fn build_alias_column_names(
        table: &str,
        original_names: Vec<String>,
        alias_names: Vec<String>,
    ) -> Result<Vec<String>> {
        let mut result = vec![];
        if alias_names.len() > original_names.len() {
            return Err(ErrorCode::InvalidColumnAlias(format!(
                "table \"{}\" has {} columns available but {} columns specified",
                table,
                original_names.len(),
                alias_names.len()
            )));
        }
        for name in alias_names.iter() {
            result.push(name.clone());
        }
        for name in original_names.into_iter().skip(alias_names.len()) {
            result.push(name);
        }
        Ok(result)
    }

    fn resolve_select_target(
        item: &SelectTarget,
        bind_context: &BindContext,
        binder: &mut ExpressionBinder,
    ) -> Result<Vec<(Expression, String)>> {
        let mut positional_projections = vec![];
        match item {
            SelectTarget::Projection { expr, alias } => {
                let expression = binder.bind(expr)?;
                let alias = alias
                    .clone()
                    .map_or_else(|| expr.to_string(), |ident| ident.get_name());
                positional_projections.push((expression, alias));
            }
            SelectTarget::Indirections(indirections) => {
                // TODO: we only support one level indirection now, that is,
                // indirect by column name like `SELECT a FROM t` or `SELECT * FROM t`
                if indirections.len() != 1 {
                    return Err(ErrorCode::SyntaxException(
                        "Invalid select list indirection",
                    ));
                }
                // Checked get
                let indirection = &indirections[0];
                match indirection {
                    Indirection::Identifier(ident) => {
                        let name = ident.get_name();
                        let binding =
                            bind_context.get_column_binding_by_column_name(name.as_str())?;
                        let expression = Expression::ColumnRef {
                            name: name.clone(),
                            binding,
                            data_type: None,
                        };
                        positional_projections.push((expression, name));
                    }
                    Indirection::Star => {
                        // NOTICE: this may produce duplicated names, which is actually fine
                        let bindings = bind_context.get_all_column_bindings()?;
                        positional_projections.append(
                            &mut bindings
                                .into_iter()
                                .map(|(name, binding)| {
                                    (
                                        Expression::ColumnRef {
                                            name: name.clone(),
                                            binding,
                                            data_type: None,
                                        },
                                        name,
                                    )
                                })
                                .collect(),
                        );
                    }
                }
            }
        }
        Ok(positional_projections)
    }
}

#[derive(Debug, Clone)]
pub struct BindContext {
    // Table name -> Table binding index
    pub name_binding_map: HashMap<String, TableBinding>,
    // Append only bindings list, used to access `TableBinding` with index
    pub indexed_bindings: Vec<(String, TableBinding)>,
    // Unique table index counter, should maintained by root BindContext.
    pub table_index_count: IndexType,
}

impl BindContext {
    pub fn new(index: IndexType) -> Self {
        BindContext {
            name_binding_map: HashMap::new(),
            indexed_bindings: Vec::new(),
            table_index_count: index,
        }
    }

    pub fn get_table_binding_by_name(&self, table_name: &str) -> Result<TableBinding> {
        self.name_binding_map
            .get(table_name)
            .cloned()
            .ok_or(ErrorCode::LogicalError(format!(
                "Cannot find table {} in BindContext",
                table_name
            )))
    }

    pub fn get_column_binding_by_column_name(&self, column_name: &str) -> Result<ColumnBinding> {
        for (_, table_binding) in self.indexed_bindings.iter() {
            for (index, col) in table_binding.columns.iter().enumerate() {
                if col.as_str() == column_name {
                    return Ok(ColumnBinding {
                        table_index: table_binding.index,
                        column_index: index,
                    });
                }
            }
        }
        Err(ErrorCode::InvalidColumnAlias(format!(
            "column \"{}\" doesn't exists",
            column_name
        )))
    }

    // Use to generate column bindings for wildcard like `SELECT * FROM t`
    pub fn get_all_column_bindings(&self) -> Result<Vec<(String, ColumnBinding)>> {
        let mut result = vec![];
        for (_, binding) in self.indexed_bindings.iter() {
            result.append(
                &mut binding
                    .columns
                    .iter()
                    .cloned()
                    .zip(binding.get_column_bindings())
                    .collect(),
            );
        }
        Ok(result)
    }

    pub fn add_table_binding(&mut self, table_binding: TableBinding) -> Result<()> {
        if let Some(binding) = self.name_binding_map.get(table_binding.table_name.as_str()) {
            return Err(ErrorCode::DuplicatedName(format!(
                "table name \"{}\" specified more than once",
                binding.table_name
            )));
        }
        self.name_binding_map
            .insert(table_binding.table_name.to_owned(), table_binding.clone());
        self.indexed_bindings
            .push((table_binding.table_name.to_owned(), table_binding));
        Ok(())
    }

    pub fn merge_bind_context(&mut self, bind_context: BindContext) -> Result<()> {
        for (_, binding) in bind_context.indexed_bindings.into_iter() {
            self.add_table_binding(binding)?;
        }
        self.table_index_count = bind_context.table_index_count;
        Ok(())
    }

    pub fn add_table(
        &mut self,
        index: IndexType,
        table_name: String,
        columns: Vec<String>,
        data_types: Vec<DataType>,
    ) -> Result<()> {
        let table_binding = TableBinding::new(index, table_name, columns, data_types);

        self.add_table_binding(table_binding)
    }

    pub fn next_table_index(&mut self) -> IndexType {
        let res = self.table_index_count;
        self.table_index_count += 1;
        res
    }
}

#[derive(Debug, Clone)]
pub struct TableBinding {
    // Index of TableBinding, used in BindContext
    pub index: IndexType,
    // Table name or alias
    pub table_name: String,
    // Column names
    pub columns: Vec<String>,
    // Data types of columns
    pub data_types: Vec<DataType>,
}

impl TableBinding {
    pub fn new(
        index: IndexType,
        table_name: String,
        columns: Vec<String>,
        data_types: Vec<DataType>,
    ) -> Self {
        TableBinding {
            index,
            table_name,
            columns,
            data_types,
        }
    }

    pub fn get_column_bindings(&self) -> Vec<ColumnBinding> {
        self.columns
            .iter()
            .enumerate()
            .map(|(index, _)| ColumnBinding {
                table_index: self.index,
                column_index: index,
            })
            .collect()
    }

    pub fn get_column_by_index(&self, column_index: IndexType) -> Result<(String, DataType)> {
        assert_eq!(self.columns.len(), self.data_types.len());
        if column_index > self.columns.len() {
            Err(ErrorCode::LogicalError(format!(
                "Invalid column index {} for table binding {:?}",
                column_index, &self
            )))
        } else {
            Ok((
                self.columns[column_index].clone(),
                self.data_types[column_index].clone(),
            ))
        }
    }

    pub fn get_column_by_name(&self, column_name: &str) -> Result<(IndexType, DataType)> {
        assert_eq!(self.columns.len(), self.data_types.len());
        for (index, column) in self.columns.iter().enumerate() {
            if column_name == column.as_str() {
                return Ok((index, self.data_types[index].clone()));
            }
        }
        Err(ErrorCode::LogicalError(format!(
            "Cannot find column name {} in table binding {:?}",
            column_name, &self
        )))
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ColumnBinding {
    // Index of the table binding this column belongs to, used in BindContext
    pub table_index: IndexType,
    // Index of column binding inside table binding
    pub column_index: IndexType,
}
