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

use std::collections::BTreeMap;
use std::collections::HashSet;
use std::str::FromStr;

use databend_common_ast::Span;
use databend_common_ast::ast::ColumnID;
use databend_common_ast::ast::ColumnRef;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::Identifier;
use databend_common_catalog::plan::InternalColumn;
use databend_common_catalog::plan::InternalColumnType;
use databend_common_catalog::plan::InvertedIndexInfo;
use databend_common_catalog::plan::InvertedIndexOption;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::SEARCH_MATCHED_COL_NAME;
use databend_common_expression::SEARCH_SCORE_COL_NAME;
use databend_common_expression::types::DataType;
use databend_common_expression::types::F32;
use databend_common_expression::types::NumberDataType;
use databend_common_meta_app::schema::TableIndexType;
use itertools::Itertools;
use tantivy_query_grammar::UserInputAst;
use tantivy_query_grammar::UserInputLeaf;
use tantivy_query_grammar::parse_query_lenient;

use super::TypeChecker;
use crate::binder::ExprContext;
use crate::binder::InternalColumnBinding;
use crate::plans::BoundColumnRef;
use crate::plans::ConstantExpr;
use crate::plans::ScalarExpr;

impl<'a> TypeChecker<'a> {
    pub(super) fn resolve_score_search_function(
        &mut self,
        span: Span,
        func_name: &str,
        args: &[&Expr],
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        if !args.is_empty() {
            return Err(ErrorCode::SemanticError(format!(
                "invalid arguments for search function, {} expects 0 argument, but got {}",
                func_name,
                args.len()
            ))
            .set_span(span));
        }
        let internal_column =
            InternalColumn::new(SEARCH_SCORE_COL_NAME, InternalColumnType::SearchScore);

        let internal_column_binding = InternalColumnBinding {
            database_name: None,
            table_name: None,
            internal_column,
        };
        let column = self.bind_context.add_internal_column_binding(
            &internal_column_binding,
            self.metadata.clone(),
            None,
            false,
        )?;

        let scalar_expr = ScalarExpr::BoundColumnRef(BoundColumnRef { span, column });
        let data_type = DataType::Number(NumberDataType::Float32);
        Ok(Box::new((scalar_expr, data_type)))
    }

    /// Resolve match search function.
    /// The first argument is the field or fields to match against,
    /// multiple fields can have a optional per-field boosting that
    /// gives preferential weight to fields being searched in.
    /// For example: title^5, content^1.2
    /// The second argument is the query text without query syntax.
    pub(super) fn resolve_match_search_function(
        &mut self,
        span: Span,
        func_name: &str,
        args: &[&Expr],
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        if !matches!(self.bind_context.expr_context, ExprContext::WhereClause) {
            return Err(ErrorCode::SemanticError(format!(
                "search function {} can only be used in where clause",
                func_name
            ))
            .set_span(span));
        }

        // The optional third argument is additional configuration option.
        if args.len() != 2 && args.len() != 3 {
            return Err(ErrorCode::SemanticError(format!(
                "invalid arguments for search function, {} expects 2 or 3 arguments, but got {}",
                func_name,
                args.len()
            ))
            .set_span(span));
        }

        let field_arg = args[0];
        let query_arg = args[1];
        let option_arg = if args.len() == 3 { Some(args[2]) } else { None };

        let box (field_scalar, _) = self.resolve(field_arg)?;
        let column_refs = match field_scalar {
            // single field without boost
            ScalarExpr::BoundColumnRef(column_ref) => {
                vec![(column_ref, None)]
            }
            // constant multiple fields with boosts
            ScalarExpr::ConstantExpr(constant_expr) => {
                let Some(constant_field) = constant_expr.value.as_string() else {
                    return Err(ErrorCode::SemanticError(format!(
                        "invalid arguments for search function, field must be a column or constant string, but got {}",
                        constant_expr.value
                    ))
                    .set_span(constant_expr.span));
                };

                // fields are separated by commas and boost is separated by ^
                let field_strs: Vec<&str> = constant_field.split(',').collect();
                let mut column_refs = Vec::with_capacity(field_strs.len());
                for field_str in field_strs {
                    let field_boosts: Vec<&str> = field_str.split('^').collect();
                    if field_boosts.len() > 2 {
                        return Err(ErrorCode::SemanticError(format!(
                            "invalid arguments for search function, field string must have only one boost, but got {}",
                            constant_field
                        ))
                        .set_span(constant_expr.span));
                    }
                    let column_expr = Expr::ColumnRef {
                        span: constant_expr.span,
                        column: ColumnRef {
                            database: None,
                            table: None,
                            column: ColumnID::Name(Identifier::from_name(
                                constant_expr.span,
                                field_boosts[0].trim(),
                            )),
                        },
                    };
                    let box (field_scalar, _) = self.resolve(&column_expr)?;
                    let Ok(column_ref) = BoundColumnRef::try_from(field_scalar) else {
                        return Err(ErrorCode::SemanticError(
                            "invalid arguments for search function, field must be a column"
                                .to_string(),
                        )
                        .set_span(constant_expr.span));
                    };
                    let boost = if field_boosts.len() == 2 {
                        match f32::from_str(field_boosts[1].trim()) {
                            Ok(boost) => Some(F32::from(boost)),
                            Err(_) => {
                                return Err(ErrorCode::SemanticError(format!(
                                    "invalid arguments for search function, boost must be a float value, but got {}",
                                    field_boosts[1]
                                ))
                                .set_span(constant_expr.span));
                            }
                        }
                    } else {
                        None
                    };
                    column_refs.push((column_ref, boost));
                }
                column_refs
            }
            _ => {
                return Err(ErrorCode::SemanticError(
                    "invalid arguments for search function, field must be a column or constant string".to_string(),
                )
                .set_span(span));
            }
        };

        let box (query_scalar, _) = self.resolve(query_arg)?;
        let Ok(query_expr) = ConstantExpr::try_from(query_scalar.clone()) else {
            return Err(ErrorCode::SemanticError(format!(
                "invalid arguments for search function, query text must be a constant string, but got {}",
                query_arg
            ))
            .set_span(query_scalar.span()));
        };
        let Some(query_text) = query_expr.value.as_string() else {
            return Err(ErrorCode::SemanticError(format!(
                "invalid arguments for search function, query text must be a constant string, but got {}",
                query_arg
            ))
            .set_span(query_scalar.span()));
        };

        let inverted_index_option = self.resolve_search_option(option_arg)?;

        self.resolve_search_function(span, column_refs, query_text, inverted_index_option)
    }

    /// Resolve query search function.
    /// The first argument query text with query syntax.
    /// The following query syntax is supported:
    /// 1. simple terms, like `title:quick`
    /// 2. bool operator terms, like `title:fox AND dog OR cat`
    /// 3. must and negative operator terms, like `title:+fox -cat`
    /// 4. phrase terms, like `title:"quick brown fox"`
    /// 5. multiple field with boost terms, like `title:fox^5 content:dog^2`
    pub(super) fn resolve_query_search_function(
        &mut self,
        span: Span,
        func_name: &str,
        args: &[&Expr],
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        if !matches!(self.bind_context.expr_context, ExprContext::WhereClause) {
            return Err(ErrorCode::SemanticError(format!(
                "search function {} can only be used in where clause",
                func_name
            ))
            .set_span(span));
        }

        // The optional second argument is additional configuration option.
        if args.len() != 1 && args.len() != 2 {
            return Err(ErrorCode::SemanticError(format!(
                "invalid arguments for search function, {} expects 1 argument, but got {}",
                func_name,
                args.len()
            ))
            .set_span(span));
        }

        let query_arg = args[0];
        let option_arg = if args.len() == 2 { Some(args[1]) } else { None };

        let box (query_scalar, _) = self.resolve(query_arg)?;
        let Ok(query_expr) = ConstantExpr::try_from(query_scalar.clone()) else {
            return Err(ErrorCode::SemanticError(format!(
                "invalid arguments for search function, query text must be a constant string, but got {}",
                query_arg
            ))
            .set_span(query_scalar.span()));
        };
        let Some(query_text) = query_expr.value.as_string() else {
            return Err(ErrorCode::SemanticError(format!(
                "invalid arguments for search function, query text must be a constant string, but got {}",
                query_arg
            ))
            .set_span(query_scalar.span()));
        };

        // Extract the first subfield from the query field as the field name,
        // as queries may contain dot separators when the field is JSON type.
        // For example: The value of the `info` field is: `{“tags”:{“id”:10,“env”:“prod”,‘name’:“test”}}`
        // The query statement can be written as `info.tags.env:prod`, the field `info` can be extracted.
        fn extract_first_subfield(field: &str) -> String {
            field.split('.').next().unwrap_or(field).to_string()
        }

        fn collect_fields(ast: &UserInputAst, fields: &mut HashSet<String>) {
            match ast {
                UserInputAst::Clause(clauses) => {
                    for (_, sub_ast) in clauses {
                        collect_fields(sub_ast, fields);
                    }
                }
                UserInputAst::Boost(inner_ast, _) => {
                    collect_fields(inner_ast, fields);
                }
                UserInputAst::Leaf(leaf) => match &**leaf {
                    UserInputLeaf::Literal(literal) => {
                        if let Some(field) = &literal.field_name {
                            fields.insert(extract_first_subfield(field));
                        }
                    }
                    UserInputLeaf::Range { field, .. } => {
                        if let Some(field) = field {
                            fields.insert(extract_first_subfield(field));
                        }
                    }
                    UserInputLeaf::Set { field, .. } => {
                        if let Some(field) = field {
                            fields.insert(extract_first_subfield(field));
                        }
                    }
                    UserInputLeaf::Exists { field } => {
                        fields.insert(extract_first_subfield(field));
                    }
                    UserInputLeaf::Regex { field, .. } => {
                        if let Some(field) = field {
                            fields.insert(extract_first_subfield(field));
                        }
                    }
                    UserInputLeaf::All => {}
                },
            }
        }

        let (query_ast, errs) = parse_query_lenient(query_text);
        if !errs.is_empty() {
            let err_msg = errs
                .into_iter()
                .map(|err| format!("{} pos {}", err.message, err.pos))
                .join(", ");
            return Err(
                ErrorCode::SemanticError(format!("invalid query: {err_msg}",))
                    .set_span(query_scalar.span()),
            );
        }
        let mut fields = HashSet::new();
        collect_fields(&query_ast, &mut fields);

        let mut column_refs = Vec::with_capacity(fields.len());
        for field in fields {
            let column_expr = Expr::ColumnRef {
                span: query_scalar.span(),
                column: ColumnRef {
                    database: None,
                    table: None,
                    column: ColumnID::Name(Identifier::from_name(query_scalar.span(), field)),
                },
            };
            let box (field_scalar, _) = self.resolve(&column_expr)?;
            let Ok(column_ref) = BoundColumnRef::try_from(field_scalar) else {
                return Err(ErrorCode::SemanticError(
                    "invalid arguments for search function, field must be a column".to_string(),
                )
                .set_span(query_scalar.span()));
            };
            column_refs.push((column_ref, None));
        }
        let inverted_index_option = self.resolve_search_option(option_arg)?;

        self.resolve_search_function(span, column_refs, query_text, inverted_index_option)
    }

    fn resolve_search_option(
        &mut self,
        option_arg: Option<&Expr>,
    ) -> Result<Option<InvertedIndexOption>> {
        if let Some(option_arg) = option_arg {
            let box (option_scalar, _) = self.resolve(option_arg)?;
            let Ok(option_expr) = ConstantExpr::try_from(option_scalar.clone()) else {
                return Err(ErrorCode::SemanticError(format!(
                    "invalid arguments for search function, option must be a constant string, but got {}",
                    option_arg
                ))
                .set_span(option_scalar.span()));
            };
            let Some(option_text) = option_expr.value.as_string() else {
                return Err(ErrorCode::SemanticError(format!(
                    "invalid arguments for search function, option text must be a constant string, but got {}",
                    option_arg
                ))
                .set_span(option_scalar.span()));
            };

            let mut lenient = None;
            let mut operator = None;
            let mut fuzziness = None;

            // additional configuration options are separated by semicolon `;`
            for option_str in option_text.split(';') {
                if option_str.trim().is_empty() {
                    continue;
                }
                let option_vals: Vec<&str> = option_str.split('=').collect();
                if option_vals.len() != 2 {
                    return Err(ErrorCode::SemanticError(format!(
                        "invalid arguments for search function, each option must have key and value joined by equal sign, but got {}",
                        option_arg
                    ))
                    .set_span(option_scalar.span()));
                }
                let option_key = option_vals[0].trim().to_lowercase();
                let option_val = option_vals[1].trim().to_lowercase();
                match option_key.as_str() {
                    "fuzziness" => {
                        // fuzziness is only support 1 and 2 currently.
                        if fuzziness.is_none() {
                            if option_val == "1" {
                                fuzziness = Some(1);
                                continue;
                            } else if option_val == "2" {
                                fuzziness = Some(2);
                                continue;
                            }
                        }
                    }
                    "operator" => {
                        if operator.is_none() {
                            if option_val == "or" {
                                operator = Some(false);
                                continue;
                            } else if option_val == "and" {
                                operator = Some(true);
                                continue;
                            }
                        }
                    }
                    "lenient" => {
                        if lenient.is_none() {
                            if option_val == "false" {
                                lenient = Some(false);
                                continue;
                            } else if option_val == "true" {
                                lenient = Some(true);
                                continue;
                            }
                        }
                    }
                    _ => {}
                }
                return Err(ErrorCode::SemanticError(format!(
                    "invalid arguments for search function, unsupported option: {}",
                    option_arg
                ))
                .set_span(option_scalar.span()));
            }

            let inverted_index_option = InvertedIndexOption {
                lenient: lenient.unwrap_or_default(),
                operator: operator.unwrap_or_default(),
                fuzziness,
            };

            return Ok(Some(inverted_index_option));
        }
        Ok(None)
    }

    fn resolve_search_function(
        &mut self,
        span: Span,
        column_refs: Vec<(BoundColumnRef, Option<F32>)>,
        query_text: &String,
        inverted_index_option: Option<InvertedIndexOption>,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        if column_refs.is_empty() {
            return Err(ErrorCode::SemanticError(
                "invalid arguments for search function, must specify at least one search column"
                    .to_string(),
            )
            .set_span(span));
        }
        if !column_refs.windows(2).all(|c| {
            c[0].0.column.table_index.is_some()
                && c[0].0.column.table_index == c[1].0.column.table_index
        }) {
            return Err(ErrorCode::SemanticError(
                "invalid arguments for search function, all columns must in a table".to_string(),
            )
            .set_span(span));
        }
        let table_index = column_refs[0].0.column.table_index.unwrap();

        let table_entry = self.metadata.read().table(table_index).clone();
        let table = table_entry.table();
        let table_info = table.get_table_info();
        let table_schema = table.schema();
        let table_indexes = &table_info.meta.indexes;

        let mut query_fields = Vec::with_capacity(column_refs.len());
        let mut column_ids = Vec::with_capacity(column_refs.len());
        for (column_ref, boost) in &column_refs {
            let column_name = &column_ref.column.column_name;
            let column_id = table_schema.column_id_of(column_name)?;
            column_ids.push(column_id);
            query_fields.push((column_name.clone(), *boost));
        }

        // find inverted index and check schema
        let mut index_name = "".to_string();
        let mut index_version = "".to_string();
        let mut index_schema = None;
        let mut index_options = BTreeMap::new();
        for table_index in table_indexes.values() {
            if table_index.index_type != TableIndexType::Inverted {
                continue;
            }
            if column_ids
                .iter()
                .all(|id| table_index.column_ids.contains(id))
            {
                index_name = table_index.name.clone();
                index_version = table_index.version.clone();

                let mut index_fields = Vec::with_capacity(table_index.column_ids.len());
                for column_id in &table_index.column_ids {
                    let table_field = table_schema.field_of_column_id(*column_id)?;
                    let field = DataField::from(table_field);
                    index_fields.push(field);
                }
                index_schema = Some(DataSchema::new(index_fields));
                index_options = table_index.options.clone();
                break;
            }
        }

        if index_schema.is_none() {
            let column_names = query_fields.iter().map(|c| c.0.clone()).join(", ");
            return Err(ErrorCode::SemanticError(format!(
                "columns {} don't have inverted index",
                column_names
            ))
            .set_span(span));
        }

        if self
            .bind_context
            .inverted_index_map
            .contains_key(&table_index)
        {
            return Err(ErrorCode::SemanticError(format!(
                "duplicate search function for table {table_index}"
            ))
            .set_span(span));
        }
        let index_info = InvertedIndexInfo {
            index_name,
            index_version,
            index_options,
            index_schema: index_schema.unwrap(),
            query_fields,
            query_text: query_text.to_string(),
            has_score: false,
            inverted_index_option,
        };

        self.bind_context
            .inverted_index_map
            .insert(table_index, index_info);

        let internal_column =
            InternalColumn::new(SEARCH_MATCHED_COL_NAME, InternalColumnType::SearchMatched);

        let internal_column_binding = InternalColumnBinding {
            database_name: column_refs[0].0.column.database_name.clone(),
            table_name: column_refs[0].0.column.table_name.clone(),
            internal_column,
        };
        let column = self.bind_context.add_internal_column_binding(
            &internal_column_binding,
            self.metadata.clone(),
            None,
            false,
        )?;

        let scalar_expr = ScalarExpr::BoundColumnRef(BoundColumnRef { span, column });
        let data_type = DataType::Boolean;
        Ok(Box::new((scalar_expr, data_type)))
    }
}
