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
use std::sync::Arc;

use databend_common_ast::Span;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::FunctionCall as ASTFunctionCall;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::Literal;
use databend_common_ast::ast::SampleConfig;
use databend_common_ast::ast::SelectStmt;
use databend_common_ast::ast::SelectTarget;
use databend_common_ast::ast::TableAlias;
use databend_common_ast::ast::TableReference;
use databend_common_catalog::catalog::CatalogManager;
use databend_common_catalog::catalog_kind::CATALOG_DEFAULT;
use databend_common_catalog::table_args::TableArgs;
use databend_common_catalog::table_function::TableFunction;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::FunctionKind;
use databend_common_expression::Scalar;
use databend_common_expression::display::scalar_ref_to_string;
use databend_common_expression::types::NumberScalar;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_meta_app::principal::UDFDefinition;
use databend_common_storages_basic::ResultCacheMetaManager;
use databend_common_storages_basic::ResultScan;
use databend_common_users::UserApiProvider;
use derive_visitor::DriveMut;

use crate::BindContext;
use crate::Planner;
use crate::ScalarExpr;
use crate::UDFArgVisitor;
use crate::binder::Binder;
use crate::binder::ColumnBindingBuilder;
use crate::binder::Visibility;
use crate::binder::scalar::ScalarBinder;
use crate::binder::table_args::bind_table_args;
use crate::optimizer::ir::SExpr;
use crate::planner::semantic::normalize_identifier;
use crate::plans::BoundColumnRef;
use crate::plans::CastExpr;
use crate::plans::EvalScalar;
use crate::plans::FunctionCall;
use crate::plans::Plan;
use crate::plans::RelOperator;
use crate::plans::ScalarItem;

impl Binder {
    /// Bind a table function.
    pub(crate) fn bind_table_function(
        &mut self,
        bind_context: &mut BindContext,
        span: &Span,
        name: &Identifier,
        params: &[Expr],
        named_params: &[(Identifier, Expr)],
        alias: &Option<TableAlias>,
        sample: &Option<SampleConfig>,
    ) -> Result<(SExpr, BindContext)> {
        let func_name = normalize_identifier(name, &self.name_resolution_ctx);

        if BUILTIN_FUNCTIONS
            .get_property(&func_name.name)
            .map(|p| p.kind == FunctionKind::SRF)
            .unwrap_or(false)
        {
            // If it is a set-returning function, we bind it as a subquery.
            let args = parse_table_function_args(span, &func_name, params, named_params)?;

            let select_stmt = SelectStmt {
                span: *span,
                hints: None,
                distinct: false,
                top_n: None,
                select_list: vec![SelectTarget::AliasedExpr {
                    expr: Box::new(databend_common_ast::ast::Expr::FunctionCall {
                        span: *span,
                        func: ASTFunctionCall {
                            distinct: false,
                            name: databend_common_ast::ast::Identifier::from_name(
                                *span,
                                &func_name.name,
                            ),
                            params: vec![],
                            args,
                            order_by: vec![],
                            window: None,
                            lambda: None,
                        },
                    }),
                    alias: None,
                }],
                from: vec![],
                selection: None,
                group_by: None,
                having: None,
                window_list: None,
                qualify: None,
            };
            let (srf_expr, mut bind_context) =
                self.bind_select(bind_context, &select_stmt, &[], None)?;

            return self.extract_srf_table_function_columns(
                &mut bind_context,
                span,
                &func_name,
                srf_expr,
                alias,
            );
        }

        if func_name.name.eq_ignore_ascii_case("obfuscate") {
            return self.bind_obfuscate(bind_context, params, named_params);
        }

        let mut scalar_binder = ScalarBinder::new(
            bind_context,
            self.ctx.clone(),
            &self.name_resolution_ctx,
            self.metadata.clone(),
            &[],
        );
        let table_args = bind_table_args(
            &mut scalar_binder,
            params,
            named_params,
            &self.subquery_executor,
        )?;

        let tenant = self.ctx.get_tenant();
        let udtf_result = databend_common_base::runtime::block_on(async {
            match UserApiProvider::instance()
                .get_udf(&tenant, &func_name.name)
                .await?
                .map(|udf| udf.definition)
            {
                Some(UDFDefinition::UDTF(udtf)) => {
                    let mut stmt = Planner::new(self.ctx.clone())
                        .parse_sql(&udtf.sql)?
                        .statement;

                    if udtf.arg_types.len() != table_args.positioned.len() {
                        return Err(ErrorCode::SyntaxException(format!(
                            "UDTF '{}' argument types length {} does not match input arguments length {}",
                            func_name,
                            udtf.arg_types.len(),
                            table_args.positioned.len()
                        )));
                    }

                    let args_expr = table_args
                        .positioned
                        .iter()
                        .map(|scalar| Expr::Literal {
                            span: None,
                            value: Literal::String(scalar_ref_to_string(&scalar.as_ref())),
                        })
                        .collect::<Vec<_>>();
                    let mut visitor = UDFArgVisitor::new(&udtf.arg_types, &args_expr);
                    stmt.drive_mut(&mut visitor);

                    let binder = Binder::new(
                        self.ctx.clone(),
                        CatalogManager::instance(),
                        self.name_resolution_ctx.clone(),
                        self.metadata.clone(),
                    )
                    .with_subquery_executor(self.subquery_executor.clone());
                    let plan = binder.bind(&stmt).await?;

                    let Plan::Query {
                        s_expr,
                        mut bind_context,
                        ..
                    } = plan
                    else {
                        return Err(ErrorCode::UDFRuntimeError(
                            "Query in UDTF returned no result set",
                        ));
                    };
                    let mut output_bindings = Vec::with_capacity(bind_context.columns.len());
                    let mut output_items = Vec::with_capacity(bind_context.columns.len());

                    if udtf.return_types.len() != bind_context.columns.len() {
                        return Err(ErrorCode::UDFSchemaMismatch(format!(
                            "UDTF '{}' return types length {} does not match output columns length {}",
                            func_name,
                            udtf.return_types.len(),
                            bind_context.columns.len()
                        )));
                    }

                    for ((return_name, return_type), output_binding) in udtf
                        .return_types
                        .into_iter()
                        .zip(bind_context.columns.iter())
                    {
                        let input_expr = ScalarExpr::BoundColumnRef(BoundColumnRef {
                            span: None,
                            column: output_binding.clone(),
                        });
                        let cast_expr = ScalarExpr::CastExpr(CastExpr {
                            span: None,
                            is_try: false,
                            argument: Box::new(input_expr),
                            target_type: Box::new(return_type.clone()),
                        });
                        let index = self
                            .metadata
                            .write()
                            .add_derived_column(return_name.clone(), return_type.clone());
                        let output_binding = ColumnBindingBuilder::new(
                            return_name,
                            index,
                            Box::new(return_type),
                            Visibility::Visible,
                        )
                        .build();

                        output_items.push(ScalarItem {
                            scalar: cast_expr,
                            index: output_binding.index,
                        });
                        output_bindings.push(output_binding);
                    }
                    bind_context.columns = output_bindings;
                    let s_expr = SExpr::create_unary(
                        Arc::new(
                            EvalScalar {
                                items: output_items,
                            }
                            .into(),
                        ),
                        s_expr,
                    );

                    return Ok(Some((s_expr, *bind_context)));
                }
                Some(UDFDefinition::UDTFServer(udtf)) => {
                    let table = self
                        .catalogs
                        .get_default_catalog(self.ctx.session_state()?)?
                        .transform_udtf_as_table_function(
                            self.ctx.as_ref(),
                            &table_args,
                            udtf,
                            &func_name.name,
                        )?;
                    let (s_expr, bind_context) =
                        self.bind_base_table_inner(bind_context, alias, sample, table)?;
                    return Ok(Some((s_expr, bind_context)));
                }
                _ => (),
            }
            Ok(None)
        });
        if let Some(result) = udtf_result? {
            return Ok(result);
        }

        if func_name.name.eq_ignore_ascii_case("result_scan") {
            self.bind_result_scan(bind_context, span, alias, &table_args)
        } else {
            // Other table functions always reside is default catalog
            let table_meta: Arc<dyn TableFunction> = self
                .catalogs
                .get_default_catalog(self.ctx.session_state()?)?
                .get_table_function(&func_name.name, table_args)?;
            self.bind_base_table_inner(bind_context, alias, sample, table_meta)
        }
    }

    fn bind_base_table_inner(
        &mut self,
        bind_context: &mut BindContext,
        alias: &Option<TableAlias>,
        sample: &Option<SampleConfig>,
        table: Arc<dyn TableFunction>,
    ) -> Result<(SExpr, BindContext)> {
        let table_alias_name = if let Some(table_alias) = alias {
            Some(normalize_identifier(&table_alias.name, &self.name_resolution_ctx).name)
        } else {
            None
        };
        let table_index = self.metadata.write().add_table(
            CATALOG_DEFAULT.to_string(),
            "system".to_string(),
            table.as_table(),
            None,
            table_alias_name,
            false,
            false,
            false,
            None,
        );
        let (s_expr, mut bind_context) =
            self.bind_base_table(bind_context, "system", table_index, None, sample)?;
        if let Some(alias) = alias {
            bind_context.apply_table_alias(alias, &self.name_resolution_ctx)?;
        }
        Ok((s_expr, bind_context))
    }

    fn bind_result_scan(
        &mut self,
        bind_context: &mut BindContext,
        span: &Span,
        alias: &Option<TableAlias>,
        table_args: &TableArgs,
    ) -> Result<(SExpr, BindContext)> {
        let query_id = parse_result_scan_args(table_args)?;
        if query_id.is_empty() {
            return Err(ErrorCode::InvalidArgument("The `RESULT_SCAN` function requires a 'query_id' parameter. Please specify a valid query ID.")
                .set_span(*span));
        }
        let kv_store = UserApiProvider::instance().get_meta_store_client();
        let meta_key = self.ctx.get_result_cache_key(&query_id);
        if meta_key.is_none() {
            return Err(ErrorCode::EmptyData(format!(
                "`RESULT_SCAN` failed: No cache key found in current session for query ID '{}'.",
                query_id
            ))
            .set_span(*span));
        }

        databend_common_base::runtime::block_on(async move {
            let result_cache_mgr = ResultCacheMetaManager::create(kv_store, 0);
            let meta_key = meta_key.unwrap();
            let location = match result_cache_mgr.get(meta_key.clone()).await? {
                Some(value) => value.location,
                None => {
                    return Err(ErrorCode::EmptyData(format!(
                        "`RESULT_SCAN` failed: Unable to fetch cached data for query ID '{}'. The data may have exceeded its TTL or been cleaned up. Cache key: '{}'",
                        query_id, meta_key
                    )).set_span(*span));
                }
            };
            let table = ResultScan::try_create(query_id, location).await?;

            let table_alias_name = if let Some(table_alias) = alias {
                Some(normalize_identifier(&table_alias.name, &self.name_resolution_ctx).name)
            } else {
                None
            };

            let table_index = self.metadata.write().add_table(
                CATALOG_DEFAULT.to_string(),
                "system".to_string(),
                table.clone(),
                None,
                table_alias_name,
                false,
                false,
                false,
                None,
            );

            let (s_expr, mut bind_context) =
                self.bind_base_table(bind_context, "system", table_index, None, &None)?;
            if let Some(alias) = alias {
                bind_context.apply_table_alias(alias, &self.name_resolution_ctx)?;
            }
            Ok((s_expr, bind_context))
        })
    }

    /// Extract the srf inner tuple fields as columns.
    fn extract_srf_table_function_columns(
        &mut self,
        bind_context: &mut BindContext,
        span: &Span,
        func_name: &Identifier,
        srf_expr: SExpr,
        alias: &Option<TableAlias>,
    ) -> Result<(SExpr, BindContext)> {
        let fields = if func_name.name.eq_ignore_ascii_case("flatten") {
            Some(vec![
                "seq".to_string(),
                "key".to_string(),
                "path".to_string(),
                "index".to_string(),
                "value".to_string(),
                "this".to_string(),
            ])
        } else if func_name.name.eq_ignore_ascii_case("json_each") {
            Some(vec!["key".to_string(), "value".to_string()])
        } else {
            None
        };

        if let Some(fields) = fields {
            if let RelOperator::EvalScalar(plan) = (*srf_expr.plan).clone() {
                if plan.items.len() != 1 {
                    return Err(ErrorCode::Internal(format!(
                        "Invalid table function subquery EvalScalar items, expect 1, but got {}",
                        plan.items.len()
                    )));
                }
                // Delete srf result tuple column, extract tuple inner columns instead
                let _ = bind_context.columns.pop();
                let scalar = &plan.items[0].scalar;

                // Add tuple inner columns
                let mut items = Vec::with_capacity(fields.len());
                for (i, field) in fields.into_iter().enumerate() {
                    let field_expr = ScalarExpr::FunctionCall(FunctionCall {
                        span: *span,
                        func_name: "get".to_string(),
                        params: vec![Scalar::Number(NumberScalar::Int64((i + 1) as i64))],
                        arguments: vec![scalar.clone()],
                    });
                    let data_type = field_expr.data_type()?;
                    let index = self
                        .metadata
                        .write()
                        .add_derived_column(field.clone(), data_type.clone());

                    let column_binding = ColumnBindingBuilder::new(
                        field,
                        index,
                        Box::new(data_type),
                        Visibility::Visible,
                    )
                    .is_srf(true)
                    .build();
                    bind_context.add_column_binding(column_binding);

                    items.push(ScalarItem {
                        scalar: field_expr,
                        index,
                    });
                }
                let eval_scalar = EvalScalar { items };
                let new_expr =
                    SExpr::create_unary(Arc::new(eval_scalar.into()), srf_expr.children[0].clone());

                if let Some(alias) = alias {
                    bind_context.apply_table_alias(alias, &self.name_resolution_ctx)?;
                }
                return Ok((new_expr, bind_context.clone()));
            } else {
                return Err(ErrorCode::Internal(
                    "Invalid subquery in table function: Table functions do not support this type of subquery.",
                ));
            }
        }
        // Preserve the legacy output column name for generate_series to keep existing queries working.
        if func_name.name.eq_ignore_ascii_case("generate_series") {
            bind_context.columns[0].column_name = func_name.name.clone();
            if let Some(alias) = alias {
                bind_context.apply_table_alias(alias, &self.name_resolution_ctx)?;
            }
            return Ok((srf_expr, bind_context.clone()));
        }
        // Set name for srf result column
        bind_context.columns[0].column_name = "value".to_string();
        if let Some(alias) = alias {
            bind_context.apply_table_alias(alias, &self.name_resolution_ctx)?;
        }
        Ok((srf_expr, bind_context.clone()))
    }

    /// Bind a lateral table function.
    pub(crate) fn bind_lateral_table_function(
        &mut self,
        parent_context: &mut BindContext,
        child: SExpr,
        table_ref: &TableReference,
    ) -> Result<(SExpr, BindContext)> {
        match table_ref {
            TableReference::TableFunction {
                span,
                name,
                params,
                named_params,
                alias,
                ..
            } => {
                let mut bind_context = BindContext::with_parent(parent_context.clone())?;
                let func_name = normalize_identifier(name, &self.name_resolution_ctx);

                if BUILTIN_FUNCTIONS
                    .get_property(&func_name.name)
                    .map(|p| p.kind == FunctionKind::SRF)
                    .unwrap_or(false)
                {
                    let args = parse_table_function_args(span, &func_name, params, named_params)?;

                    // convert lateral join table function to srf function
                    let srf = Expr::FunctionCall {
                        span: *span,
                        func: ASTFunctionCall {
                            distinct: false,
                            name: func_name.clone(),
                            args,
                            params: vec![],
                            order_by: vec![],
                            window: None,
                            lambda: None,
                        },
                    };
                    let select_list = vec![SelectTarget::AliasedExpr {
                        expr: Box::new(srf.clone()),
                        alias: None,
                    }];
                    let mut select_list =
                        self.normalize_select_list(&mut bind_context, &select_list)?;
                    // analyze Set-returning functions.
                    self.analyze_project_set_select(&mut bind_context, &mut select_list)?;
                    // bind Set-returning functions.
                    let srf_expr = self.bind_project_set(&mut bind_context, child, false)?;
                    // clear Set-returning functions, avoid duplicate bind.
                    bind_context.srf_info = Default::default();

                    if let Some(item) = select_list.items.pop() {
                        let srf_result = item.scalar;
                        let column_binding =
                            if let ScalarExpr::BoundColumnRef(column_ref) = &srf_result {
                                column_ref.column.clone()
                            } else {
                                // Add result column to metadata
                                let data_type = srf_result.data_type()?;
                                let index = self
                                    .metadata
                                    .write()
                                    .add_derived_column(srf.to_string(), data_type.clone());
                                ColumnBindingBuilder::new(
                                    srf.to_string(),
                                    index,
                                    Box::new(data_type),
                                    Visibility::Visible,
                                )
                                .build()
                            };

                        let eval_scalar = EvalScalar {
                            items: vec![ScalarItem {
                                scalar: srf_result,
                                index: column_binding.index,
                            }],
                        };
                        // Add srf result column
                        bind_context.add_column_binding(column_binding);

                        let flatten_expr =
                            SExpr::create_unary(Arc::new(eval_scalar.into()), Arc::new(srf_expr));

                        let (new_expr, mut bind_context) = self
                            .extract_srf_table_function_columns(
                                &mut bind_context,
                                span,
                                &func_name,
                                flatten_expr,
                                alias,
                            )?;

                        // add left table columns.
                        let mut new_columns = parent_context.columns.clone();
                        new_columns.extend_from_slice(&bind_context.columns);
                        bind_context.columns = new_columns;

                        Ok((new_expr, bind_context))
                    } else {
                        Err(ErrorCode::Internal("Failed to bind project_set for lateral join. This may indicate an issue with the SRF (Set Returning Function) processing or an internal logic error.")
                            .set_span(*span))
                    }
                } else {
                    Err(ErrorCode::InvalidArgument(format!(
                        "The function '{}' is not supported for lateral joins. Lateral joins currently support only Set Returning Functions (SRFs).",
                        func_name
                    )).set_span(*span))
                }
            }
            _ => unreachable!(),
        }
    }
}

// parse flatten named params to arguments
fn parse_table_function_args(
    span: &Span,
    func_name: &Identifier,
    params: &[Expr],
    named_params: &[(Identifier, Expr)],
) -> Result<Vec<Expr>> {
    if func_name.name.eq_ignore_ascii_case("flatten") {
        // build flatten function arguments.
        let mut named_args: HashMap<String, Expr> = named_params
            .iter()
            .map(|(name, value)| (name.name.to_lowercase(), value.clone()))
            .collect::<HashMap<_, _>>();

        let mut args = Vec::with_capacity(named_args.len() + params.len());
        let names = vec!["input", "path", "outer", "recursive", "mode"];
        for name in names {
            if named_args.is_empty() {
                break;
            }
            match named_args.remove(name) {
                Some(val) => args.push(val),
                None => args.push(Expr::Literal {
                    span: None,
                    value: Literal::Null,
                }),
            }
        }
        if !named_args.is_empty() {
            let invalid_names = named_args.into_keys().collect::<Vec<String>>().join(", ");
            return Err(ErrorCode::InvalidArgument(format!(
                "Invalid named parameters for 'flatten': {}, valid parameters are: [input, path, outer, recursive, mode]",
                invalid_names,
            ))
                .set_span(*span));
        }

        if !params.is_empty() {
            args.extend(params.iter().cloned());
        }
        Ok(args)
    } else {
        if !named_params.is_empty() {
            let invalid_names = named_params
                .iter()
                .map(|(name, _)| name.name.clone())
                .collect::<Vec<String>>()
                .join(", ");
            return Err(ErrorCode::InvalidArgument(format!(
                "Named parameters are not allowed for '{}'. Invalid parameters provided: {}.",
                func_name.name, invalid_names
            ))
            .set_span(*span));
        }

        Ok(params.to_vec())
    }
}

// copy from common-storages-fuse to avoid cyclic dependency.
fn string_value(value: &Scalar) -> Result<String> {
    match value {
        Scalar::String(val) => Ok(val.clone()),
        other => Err(ErrorCode::BadArguments(format!(
            "RESULT_SCAN(<query_id>) expected a string for <query_id>, but found {}.",
            other
        ))),
    }
}

#[inline(always)]
pub fn parse_result_scan_args(table_args: &TableArgs) -> Result<String> {
    let args = table_args.expect_all_positioned("RESULT_SCAN", Some(1))?;
    string_value(&args[0])
}
