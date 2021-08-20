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

use common_exception::ErrorCode;
use common_exception::Result;
use sqlparser::ast::BinaryOperator as SqlparserBinaryOperator;
use sqlparser::ast::DataType as SqlparserDataType;
use sqlparser::ast::Expr as SqlparserExpr;
use sqlparser::ast::FunctionArg;
use sqlparser::ast::Ident;
use sqlparser::ast::JoinConstraint;
use sqlparser::ast::JoinOperator as SqlparserJoinOperator;
use sqlparser::ast::ObjectName;
use sqlparser::ast::Query as SqlparserQuery;
use sqlparser::ast::Select as SqlparserSelect;
use sqlparser::ast::SelectItem;
use sqlparser::ast::SetExpr as SqlparserSetExpr;
use sqlparser::ast::SetOperator as SqlparserSetOperator;
use sqlparser::ast::Statement as SqlparserStatement;
use sqlparser::ast::TableAlias as SqlparserTableAlias;
use sqlparser::ast::TableFactor;
use sqlparser::ast::TableWithJoins;
use sqlparser::ast::UnaryOperator as SqlparserUnaryOperator;
use sqlparser::ast::Value;

use super::AstTransformer;
use crate::sql::parser::ast::Statement::Explain;
use crate::sql::parser::ast::*;

// Implementation of `AstTransformer` for sqlparser-rs
pub struct TransformerSqlparser {
    pub orig_stmt: SqlparserStatement,
}

impl TransformerSqlparser {
    #[allow(unused)]
    pub fn new(stmt: SqlparserStatement) -> Self {
        TransformerSqlparser { orig_stmt: stmt }
    }

    pub fn transform_impl(&self) -> Result<Statement> {
        self.transform_statement(&self.orig_stmt)
    }

    fn transform_statement(&self, orig_ast: &SqlparserStatement) -> Result<Statement> {
        match orig_ast {
            SqlparserStatement::Query(q) => {
                Ok(Statement::Select(Box::new(self.transform_query(q)?)))
            }
            SqlparserStatement::Explain {
                analyze, statement, ..
            } => Ok(Explain {
                analyze: analyze.to_owned(),
                query: Box::new(self.transform_statement(statement)?),
            }),

            // Transform DDL
            SqlparserStatement::Truncate { .. }
            | SqlparserStatement::CreateTable { .. }
            | SqlparserStatement::AlterTable { .. }
            | SqlparserStatement::Drop { .. }
            | SqlparserStatement::CreateDatabase { .. } => self.transform_ddl(orig_ast),

            _ => Err(ErrorCode::SyntaxException(format!(
                "Unsupported SQL statement: {}",
                self.orig_stmt
            ))),
        }
    }

    fn transform_ddl(&self, _: &SqlparserStatement) -> Result<Statement> {
        // TODO: support transform DDL
        Err(ErrorCode::SyntaxException(format!(
            "Unsupported SQL statement: {}",
            self.orig_stmt
        )))
    }

    fn transform_query(&self, orig_ast: &SqlparserQuery) -> Result<Query> {
        let body = self.transform_set_expr(&orig_ast.body)?;
        let order_by = orig_ast
            .order_by
            .iter()
            .map(|expr| {
                Ok(OrderByExpr {
                    expr: self.transform_expr(&expr.expr)?,
                    asc: expr.asc,
                    nulls_first: expr.nulls_first,
                })
            })
            .collect::<Result<_>>()?;
        let limit = orig_ast
            .limit
            .as_ref()
            .map(|expr| self.transform_expr(expr))
            .transpose()?;
        Ok(Query {
            body,
            order_by,
            limit,
        })
    }

    fn transform_set_expr(&self, orig_ast: &SqlparserSetExpr) -> Result<SetExpr> {
        match orig_ast {
            SqlparserSetExpr::SetOperation {
                op,
                all,
                left,
                right,
            } => {
                let left = self.transform_set_expr(left.as_ref())?;
                let right = self.transform_set_expr(right.as_ref())?;
                let op = SetOperator::from(op.to_owned());
                Ok(SetExpr::SetOperation {
                    op,
                    all: all.to_owned(),
                    left: Box::new(left),
                    right: Box::new(right),
                })
            }
            SqlparserSetExpr::Query(query) => {
                Ok(SetExpr::Query(Box::new(self.transform_query(query)?)))
            }
            SqlparserSetExpr::Select(select) => {
                Ok(SetExpr::Select(Box::new(self.transform_select(select)?)))
            }
            _ => Err(ErrorCode::SyntaxException(std::format!(
                "Unsupported SQL statement: {}",
                self.orig_stmt
            ))),
        }
    }

    fn transform_select(&self, orig_ast: &SqlparserSelect) -> Result<SelectStmt> {
        let distinct = orig_ast.distinct;
        let select_list: Vec<SelectTarget> = orig_ast
            .projection
            .iter()
            .map(|x| {
                match x {
                    SelectItem::UnnamedExpr(expr) => Ok(SelectTarget::Projection {
                        expr: self.transform_expr(expr)?,
                        alias: None,
                    }),
                    SelectItem::ExprWithAlias { expr, alias } => Ok(SelectTarget::Projection {
                        expr: self.transform_expr(expr)?,
                        alias: Some(Identifier::from(alias)),
                    }),
                    SelectItem::QualifiedWildcard(object_name) => {
                        // Transform fields to `Indirection`s first
                        let mut v: Vec<Indirection> = Self::transform_object_name(object_name)
                            .into_iter()
                            .map(Indirection::Identifier)
                            .collect();
                        // Push a wildcard star to the end
                        v.push(Indirection::Star);
                        Ok(SelectTarget::Indirections(v))
                    }
                    SelectItem::Wildcard => Ok(SelectTarget::Indirections(vec![Indirection::Star])),
                }
            })
            .collect::<Result<Vec<SelectTarget>>>()?;

        let from = self.transform_from(&orig_ast.from)?;
        let selection = orig_ast
            .selection
            .as_ref()
            .map(|expr| self.transform_expr(expr))
            .transpose()?;
        let group_by = orig_ast
            .group_by
            .iter()
            .map(|expr| self.transform_expr(expr))
            .collect::<Result<_>>()?;
        let having = orig_ast
            .having
            .as_ref()
            .map(|expr| self.transform_expr(expr))
            .transpose()?;

        Ok(SelectStmt {
            distinct,
            select_list,
            from,
            selection,
            group_by,
            having,
        })
    }

    fn transform_from(&self, orig_ast: &[TableWithJoins]) -> Result<TableReference> {
        let mut table_refs: Vec<TableReference> = orig_ast
            .iter()
            .map(|v| self.transform_table_with_joins(v))
            .collect::<Result<_>>()?;
        if !table_refs.is_empty() {
            let head = table_refs.drain(0..1).next().unwrap();
            table_refs.into_iter().fold(Ok(head), |acc, r| {
                Ok(TableReference::Join(Join {
                    op: JoinOperator::CrossJoin,
                    condition: JoinCondition::None,
                    left: Box::new(acc?),
                    right: Box::new(r),
                }))
            })
        } else {
            Err(ErrorCode::SyntaxException(format!(
                "Invalid SQL statement: {}",
                self.orig_stmt
            )))
        }
    }

    fn transform_table_with_joins(
        &self,
        table_with_joins: &TableWithJoins,
    ) -> Result<TableReference> {
        // Take `table_with_joins.relation` as initial left table, then we can build a left-deep tree.
        let mut left_table = self.transform_table_factor(&table_with_joins.relation)?;

        // Join the tables and finally produce a left-deep tree
        for join in table_with_joins.joins.iter() {
            let right_table = self.transform_table_factor(&join.relation)?;
            left_table = match &join.join_operator {
                SqlparserJoinOperator::Inner(constraint) => match constraint {
                    JoinConstraint::On(cond) => Ok(TableReference::Join(Join {
                        op: JoinOperator::Inner,
                        condition: JoinCondition::On(self.transform_expr(cond)?),
                        left: Box::from(left_table),
                        right: Box::from(right_table),
                    })),
                    JoinConstraint::Using(idents) => Ok(TableReference::Join(Join {
                        op: JoinOperator::Inner,
                        condition: JoinCondition::Using(
                            idents.iter().map(Identifier::from).collect(),
                        ),
                        left: Box::from(left_table),
                        right: Box::from(right_table),
                    })),
                    JoinConstraint::Natural => Ok(TableReference::Join(Join {
                        op: JoinOperator::Inner,
                        condition: JoinCondition::Natural,
                        left: Box::from(left_table),
                        right: Box::from(right_table),
                    })),
                    JoinConstraint::None => Ok(TableReference::Join(Join {
                        op: JoinOperator::Inner,
                        condition: JoinCondition::None,
                        left: Box::from(left_table),
                        right: Box::from(right_table),
                    })),
                },
                SqlparserJoinOperator::LeftOuter(constraint) => match constraint {
                    JoinConstraint::On(cond) => Ok(TableReference::Join(Join {
                        op: JoinOperator::LeftOuter,
                        condition: JoinCondition::On(self.transform_expr(cond)?),
                        left: Box::from(left_table),
                        right: Box::from(right_table),
                    })),
                    JoinConstraint::Using(idents) => Ok(TableReference::Join(Join {
                        op: JoinOperator::LeftOuter,
                        condition: JoinCondition::Using(
                            idents.iter().map(Identifier::from).collect(),
                        ),
                        left: Box::from(left_table),
                        right: Box::from(right_table),
                    })),
                    JoinConstraint::Natural => Ok(TableReference::Join(Join {
                        op: JoinOperator::LeftOuter,
                        condition: JoinCondition::Natural,
                        left: Box::from(left_table),
                        right: Box::from(right_table),
                    })),
                    // Cannot run OUTER JOIN without condition
                    JoinConstraint::None => Err(ErrorCode::SyntaxException(format!(
                        "Invalid SQL statement: {}",
                        self.orig_stmt
                    ))),
                },

                SqlparserJoinOperator::RightOuter(constraint) => match constraint {
                    JoinConstraint::On(cond) => Ok(TableReference::Join(Join {
                        op: JoinOperator::RightOuter,
                        condition: JoinCondition::On(self.transform_expr(cond)?),
                        left: Box::from(left_table),
                        right: Box::from(right_table),
                    })),
                    JoinConstraint::Using(idents) => Ok(TableReference::Join(Join {
                        op: JoinOperator::RightOuter,
                        condition: JoinCondition::Using(
                            idents.iter().map(Identifier::from).collect(),
                        ),
                        left: Box::from(left_table),
                        right: Box::from(right_table),
                    })),
                    JoinConstraint::Natural => Ok(TableReference::Join(Join {
                        op: JoinOperator::RightOuter,
                        condition: JoinCondition::Natural,
                        left: Box::from(left_table),
                        right: Box::from(right_table),
                    })),
                    // Cannot run OUTER JOIN without condition
                    JoinConstraint::None => Err(ErrorCode::SyntaxException(format!(
                        "Invalid SQL statement: {}",
                        self.orig_stmt
                    ))),
                },

                SqlparserJoinOperator::FullOuter(constraint) => match constraint {
                    JoinConstraint::On(cond) => Ok(TableReference::Join(Join {
                        op: JoinOperator::FullOuter,
                        condition: JoinCondition::On(self.transform_expr(cond)?),
                        left: Box::from(left_table),
                        right: Box::from(right_table),
                    })),
                    JoinConstraint::Using(idents) => Ok(TableReference::Join(Join {
                        op: JoinOperator::FullOuter,
                        condition: JoinCondition::Using(
                            idents.iter().map(Identifier::from).collect(),
                        ),
                        left: Box::from(left_table),
                        right: Box::from(right_table),
                    })),
                    JoinConstraint::Natural => Ok(TableReference::Join(Join {
                        op: JoinOperator::FullOuter,
                        condition: JoinCondition::Natural,
                        left: Box::from(left_table),
                        right: Box::from(right_table),
                    })),
                    // Cannot run OUTER JOIN without condition
                    JoinConstraint::None => Err(ErrorCode::SyntaxException(format!(
                        "Invalid SQL statement: {}",
                        self.orig_stmt
                    ))),
                },

                SqlparserJoinOperator::CrossJoin => Ok(TableReference::Join(Join {
                    op: JoinOperator::CrossJoin,
                    condition: JoinCondition::None,
                    left: Box::from(left_table),
                    right: Box::from(right_table),
                })),
                _ => Err(ErrorCode::SyntaxException(format!(
                    "Unsupported SQL statement: {}",
                    self.orig_stmt
                ))),
            }?;
        }

        Ok(left_table)
    }

    fn transform_table_factor(&self, table_factor: &TableFactor) -> Result<TableReference> {
        let result = match table_factor {
            TableFactor::Table { name, alias, .. } => {
                let idents = &name.0;
                if idents.len() == 1 {
                    Ok(TableReference::Table {
                        database: None,
                        table: Identifier::from(&idents[0]),
                        alias: alias.as_ref().map(|v| Self::transform_table_alias(v)),
                    })
                } else if idents.len() == 2 {
                    Ok(TableReference::Table {
                        database: Some(Identifier::from(&idents[0])),
                        table: Identifier::from(&idents[1]),
                        alias: alias.as_ref().map(|v| Self::transform_table_alias(v)),
                    })
                } else {
                    Err(ErrorCode::SyntaxException(format!(
                        "Unsupported SQL statement: {}",
                        self.orig_stmt
                    )))
                }
            }
            TableFactor::Derived {
                subquery, alias, ..
            } => Ok(TableReference::Subquery {
                subquery: Box::from(self.transform_query(subquery.as_ref())?),
                alias: alias.as_ref().map(|v| Self::transform_table_alias(v)),
            }),
            TableFactor::TableFunction { expr, alias } => Ok(TableReference::TableFunction {
                expr: self.transform_expr(expr)?,
                alias: alias.as_ref().map(|v| Self::transform_table_alias(v)),
            }),
            TableFactor::NestedJoin(nested_join) => self.transform_table_with_joins(nested_join),
        };
        result
    }

    #[inline]
    fn transform_object_name(object_name: &ObjectName) -> Vec<Identifier> {
        object_name.0.iter().map(Identifier::from).collect()
    }

    #[inline]
    fn transform_table_alias(table_alias: &SqlparserTableAlias) -> TableAlias {
        TableAlias {
            name: Identifier::from(&table_alias.name),
            columns: table_alias.columns.iter().map(Identifier::from).collect(),
        }
    }

    fn transform_expr(&self, orig_ast: &SqlparserExpr) -> Result<Expr> {
        match orig_ast {
            SqlparserExpr::Identifier(ident) => Ok(Expr::ColumnRef {
                database: None,
                table: None,
                column: Identifier::from(ident),
            }),
            SqlparserExpr::Wildcard => Ok(Expr::Wildcard),
            SqlparserExpr::CompoundIdentifier(idents) => {
                if idents.len() == 3 {
                    Ok(Expr::ColumnRef {
                        database: Some(Identifier::from(&idents[0])),
                        table: Some(Identifier::from(&idents[1])),
                        column: Identifier::from(&idents[2]),
                    })
                } else if idents.len() == 2 {
                    Ok(Expr::ColumnRef {
                        database: None,
                        table: Some(Identifier::from(&idents[0])),
                        column: Identifier::from(&idents[1]),
                    })
                } else if idents.len() == 1 {
                    Ok(Expr::ColumnRef {
                        database: None,
                        table: None,
                        column: Identifier::from(&idents[0]),
                    })
                } else {
                    Err(ErrorCode::SyntaxException(std::format!(
                        "Unsupported SQL statement: {}",
                        self.orig_stmt
                    )))
                }
            }
            SqlparserExpr::IsNull(arg) => Ok(Expr::IsNull(Box::new(self.transform_expr(arg)?))),
            SqlparserExpr::IsNotNull(arg) => Ok(Expr::IsNull(Box::new(self.transform_expr(arg)?))),
            SqlparserExpr::InList {
                expr,
                list,
                negated,
            } => Ok(Expr::InList {
                expr: Box::new(self.transform_expr(expr)?),
                list: list
                    .iter()
                    .map(|expr| self.transform_expr(expr))
                    .collect::<Result<_>>()?,
                not: negated.to_owned(),
            }),
            SqlparserExpr::InSubquery {
                expr,
                subquery,
                negated,
            } => Ok(Expr::InSubquery {
                expr: Box::new(self.transform_expr(expr)?),
                subquery: Box::new(self.transform_query(subquery)?),
                not: negated.to_owned(),
            }),
            SqlparserExpr::Between {
                expr,
                negated,
                low,
                high,
            } => Ok(Expr::Between {
                expr: Box::new(self.transform_expr(expr)?),
                negated: negated.to_owned(),
                low: Box::new(self.transform_expr(low)?),
                high: Box::new(self.transform_expr(high)?),
            }),
            SqlparserExpr::BinaryOp { left, op, right } => Ok(Expr::BinaryOp {
                op: self.transform_binary_operator(op)?,
                left: Box::new(self.transform_expr(left)?),
                right: Box::new(self.transform_expr(right)?),
            }),
            SqlparserExpr::UnaryOp { op, expr } => Ok(Expr::UnaryOp {
                op: self.transform_unary_operator(op)?,
                expr: Box::new(self.transform_expr(expr)?),
            }),
            SqlparserExpr::Cast { expr, data_type } => Ok(Expr::Cast {
                expr: Box::new(self.transform_expr(expr)?),
                target_type: self.transform_data_type(data_type)?,
            }),
            SqlparserExpr::Value(literal) => {
                let lit = match literal {
                    Value::Number(str, _) => Ok(Literal::Number(str.to_owned())),
                    Value::SingleQuotedString(str) => Ok(Literal::String(str.to_owned())),
                    Value::NationalStringLiteral(str) => Ok(Literal::String(str.to_owned())),
                    Value::HexStringLiteral(str) => Ok(Literal::String(str.to_owned())),
                    Value::DoubleQuotedString(str) => Ok(Literal::String(str.to_owned())),
                    Value::Boolean(v) => Ok(Literal::Boolean(v.to_owned())),
                    Value::Null => Ok(Literal::Null),
                    _ => Err(ErrorCode::SyntaxException(std::format!(
                        "Unsupported SQL statement: {}",
                        self.orig_stmt
                    ))),
                }?;
                Ok(Expr::Literal(lit))
            }
            SqlparserExpr::Function(func) => Ok(Expr::FunctionCall {
                distinct: func.distinct,
                name: func
                    .name
                    .0
                    .get(0)
                    .ok_or_else(|| {
                        ErrorCode::SyntaxException(std::format!(
                            "Unsupported SQL statement: {}",
                            self.orig_stmt
                        ))
                    })?
                    .value
                    .to_owned(),
                args: func
                    .args
                    .iter()
                    .map(|arg| match arg {
                        FunctionArg::Unnamed(expr) => self.transform_expr(expr),
                        FunctionArg::Named { .. } => Err(ErrorCode::SyntaxException(std::format!(
                            "Unsupported SQL statement: {}",
                            self.orig_stmt
                        ))),
                    })
                    .collect::<Result<_>>()?,
                // TODO(leiysky): wait for bumping to https://github.com/datafuse-extras/sqlparser-rs/pull/5
                params: vec![],
            }),
            SqlparserExpr::Case {
                operand,
                conditions,
                results,
                else_result,
            } => Ok(Expr::Case {
                operand: operand
                    .as_ref()
                    .map(|expr| self.transform_expr(expr))
                    .transpose()?
                    .map(Box::new),
                conditions: conditions
                    .iter()
                    .map(|expr| self.transform_expr(expr))
                    .collect::<Result<_>>()?,
                results: results
                    .iter()
                    .map(|expr| self.transform_expr(expr))
                    .collect::<Result<_>>()?,
                else_result: else_result
                    .as_ref()
                    .map(|expr| self.transform_expr(expr))
                    .transpose()?
                    .map(Box::new),
            }),
            SqlparserExpr::Exists(subquery) => {
                Ok(Expr::Exists(Box::new(self.transform_query(subquery)?)))
            }
            SqlparserExpr::Subquery(subquery) => {
                Ok(Expr::Subquery(Box::new(self.transform_query(subquery)?)))
            }
            _ => Err(ErrorCode::SyntaxException(std::format!(
                "Unsupported SQL statement: {}",
                self.orig_stmt
            ))),
        }
    }

    fn transform_binary_operator(&self, op: &SqlparserBinaryOperator) -> Result<BinaryOperator> {
        match op {
            SqlparserBinaryOperator::Plus => Ok(BinaryOperator::Plus),
            SqlparserBinaryOperator::Minus => Ok(BinaryOperator::Minus),
            SqlparserBinaryOperator::Multiply => Ok(BinaryOperator::Multiply),
            SqlparserBinaryOperator::Divide => Ok(BinaryOperator::Divide),
            SqlparserBinaryOperator::Modulus => Ok(BinaryOperator::Modulus),
            SqlparserBinaryOperator::StringConcat => Ok(BinaryOperator::StringConcat),
            SqlparserBinaryOperator::Gt => Ok(BinaryOperator::Gt),
            SqlparserBinaryOperator::Lt => Ok(BinaryOperator::Lt),
            SqlparserBinaryOperator::GtEq => Ok(BinaryOperator::Gte),
            SqlparserBinaryOperator::LtEq => Ok(BinaryOperator::Lte),
            SqlparserBinaryOperator::Eq => Ok(BinaryOperator::Eq),
            SqlparserBinaryOperator::NotEq => Ok(BinaryOperator::NotEq),
            SqlparserBinaryOperator::And => Ok(BinaryOperator::And),
            SqlparserBinaryOperator::Or => Ok(BinaryOperator::Or),
            SqlparserBinaryOperator::Like => Ok(BinaryOperator::Like),
            SqlparserBinaryOperator::NotLike => Ok(BinaryOperator::NotLike),
            SqlparserBinaryOperator::BitwiseOr => Ok(BinaryOperator::BitwiseOr),
            SqlparserBinaryOperator::BitwiseAnd => Ok(BinaryOperator::BitwiseAnd),
            SqlparserBinaryOperator::BitwiseXor => Ok(BinaryOperator::BitwiseXor),
            _ => Err(ErrorCode::SyntaxException(std::format!(
                "Unsupported SQL statement: {}",
                self.orig_stmt
            ))),
        }
    }

    fn transform_unary_operator(&self, op: &SqlparserUnaryOperator) -> Result<UnaryOperator> {
        match op {
            SqlparserUnaryOperator::Plus => Ok(UnaryOperator::Plus),
            SqlparserUnaryOperator::Minus => Ok(UnaryOperator::Minus),
            SqlparserUnaryOperator::Not => Ok(UnaryOperator::Not),
            _ => Err(ErrorCode::SyntaxException(std::format!(
                "Unsupported SQL statement: {}",
                self.orig_stmt
            ))),
        }
    }

    fn transform_data_type(&self, data_type: &SqlparserDataType) -> Result<TypeName> {
        match data_type {
            SqlparserDataType::Char(length) => Ok(TypeName::Char(length.to_owned())),
            SqlparserDataType::Varchar(length) => Ok(TypeName::Varchar(length.to_owned())),
            SqlparserDataType::Decimal(prec, scale) => {
                Ok(TypeName::Decimal(prec.to_owned(), scale.to_owned()))
            }
            SqlparserDataType::Float(length) => Ok(TypeName::Float(length.to_owned())),
            SqlparserDataType::Int => Ok(TypeName::Int),
            SqlparserDataType::TinyInt => Ok(TypeName::TinyInt),
            SqlparserDataType::SmallInt => Ok(TypeName::SmallInt),
            SqlparserDataType::BigInt => Ok(TypeName::BigInt),
            SqlparserDataType::Real => Ok(TypeName::Real),
            SqlparserDataType::Double => Ok(TypeName::Double),
            SqlparserDataType::Boolean => Ok(TypeName::Boolean),
            SqlparserDataType::Date => Ok(TypeName::Date),
            SqlparserDataType::Time => Ok(TypeName::Time),
            SqlparserDataType::Timestamp => Ok(TypeName::Timestamp),
            SqlparserDataType::Interval => Ok(TypeName::Interval),
            SqlparserDataType::Text => Ok(TypeName::Text),
            _ => Err(ErrorCode::SyntaxException(std::format!(
                "Unsupported SQL statement: {}",
                self.orig_stmt
            ))),
        }
    }
}

impl From<Ident> for Identifier {
    fn from(ident: Ident) -> Self {
        Identifier {
            name: ident.value,
            quote: ident.quote_style,
        }
    }
}

impl From<&Ident> for Identifier {
    fn from(ident: &Ident) -> Self {
        Identifier {
            name: ident.value.to_owned(),
            quote: ident.quote_style.to_owned(),
        }
    }
}

impl From<SqlparserSetOperator> for SetOperator {
    fn from(src: SqlparserSetOperator) -> Self {
        match src {
            SqlparserSetOperator::Union => SetOperator::Union,
            SqlparserSetOperator::Except => SetOperator::Except,
            SqlparserSetOperator::Intersect => SetOperator::Intersect,
        }
    }
}

impl AstTransformer for TransformerSqlparser {
    fn transform(&self) -> Result<Statement> {
        self.transform_impl()
    }
}
