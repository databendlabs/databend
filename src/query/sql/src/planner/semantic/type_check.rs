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

use std::borrow::Cow;
use std::collections::BTreeMap;
use std::collections::HashMap;
use std::collections::HashSet;
use std::collections::VecDeque;
use std::mem;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use std::vec;

use databend_common_ast::Span;
use databend_common_ast::ast::BinaryOperator;
use databend_common_ast::ast::ColumnID;
use databend_common_ast::ast::ColumnRef;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::FileLocation;
use databend_common_ast::ast::FunctionCall as ASTFunctionCall;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::IntervalKind as ASTIntervalKind;
use databend_common_ast::ast::Lambda;
use databend_common_ast::ast::Literal;
use databend_common_ast::ast::MapAccessor;
use databend_common_ast::ast::OrderByExpr;
use databend_common_ast::ast::Query;
use databend_common_ast::ast::SelectTarget;
use databend_common_ast::ast::SetExpr;
use databend_common_ast::ast::SubqueryModifier;
use databend_common_ast::ast::TrimWhere;
use databend_common_ast::ast::TypeName;
use databend_common_ast::ast::UnaryOperator;
use databend_common_ast::ast::UriLocation;
use databend_common_ast::ast::Weekday as ASTWeekday;
use databend_common_ast::ast::Window;
use databend_common_ast::ast::WindowFrame;
use databend_common_ast::ast::WindowFrameBound;
use databend_common_ast::ast::WindowFrameUnits;
use databend_common_ast::parser::Dialect;
use databend_common_ast::parser::parse_expr;
use databend_common_ast::parser::tokenize_sql;
use databend_common_base::runtime::GLOBAL_MEM_STAT;
use databend_common_catalog::catalog::CatalogManager;
use databend_common_catalog::plan::InternalColumn;
use databend_common_catalog::plan::InternalColumnType;
use databend_common_catalog::plan::InvertedIndexInfo;
use databend_common_catalog::plan::InvertedIndexOption;
use databend_common_catalog::plan::VectorIndexInfo;
use databend_common_catalog::table_context::TableContext;
use databend_common_cloud_control::client_config::build_client_config;
use databend_common_cloud_control::client_config::make_request;
use databend_common_cloud_control::cloud_api::CloudControlApiProvider;
use databend_common_cloud_control::pb::CreateWorkerRequest;
use databend_common_compress::CompressAlgorithm;
use databend_common_compress::DecompressDecoder;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::Column;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::ColumnId;
use databend_common_expression::ColumnIndex;
use databend_common_expression::Constant;
use databend_common_expression::ConstantFolder;
use databend_common_expression::DataField;
use databend_common_expression::DataSchema;
use databend_common_expression::Expr as EExpr;
use databend_common_expression::FunctionContext;
use databend_common_expression::FunctionKind;
use databend_common_expression::RawExpr;
use databend_common_expression::SEARCH_MATCHED_COL_NAME;
use databend_common_expression::SEARCH_SCORE_COL_NAME;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::VECTOR_SCORE_COL_NAME;
use databend_common_expression::cast_scalar;
use databend_common_expression::display::display_tuple_field_name;
use databend_common_expression::expr;
use databend_common_expression::infer_schema_type;
use databend_common_expression::shrink_scalar;
use databend_common_expression::type_check;
use databend_common_expression::type_check::check_number;
use databend_common_expression::type_check::common_super_type;
use databend_common_expression::type_check::convert_escape_pattern;
use databend_common_expression::types::DataType;
use databend_common_expression::types::Decimal;
use databend_common_expression::types::F32;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::decimal::DecimalScalar;
use databend_common_expression::types::decimal::DecimalSize;
use databend_common_expression::types::i256;
use databend_common_expression::types::vector::VectorDataType;
use databend_common_expression::udf_client::UDFFlightClient;
use databend_common_functions::ASYNC_FUNCTIONS;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_functions::GENERAL_LAMBDA_FUNCTIONS;
use databend_common_functions::GENERAL_SEARCH_FUNCTIONS;
use databend_common_functions::GENERAL_WINDOW_FUNCTIONS;
use databend_common_functions::GENERAL_WITHIN_GROUP_FUNCTIONS;
use databend_common_functions::RANK_WINDOW_FUNCTIONS;
use databend_common_functions::aggregates::AggregateFunctionFactory;
use databend_common_functions::is_builtin_function;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_meta_app::principal::LambdaUDF;
use databend_common_meta_app::principal::ScalarUDF;
use databend_common_meta_app::principal::StageInfo;
use databend_common_meta_app::principal::StageType;
use databend_common_meta_app::principal::UDAFScript;
use databend_common_meta_app::principal::UDFDefinition;
use databend_common_meta_app::principal::UDFScript;
use databend_common_meta_app::principal::UDFServer;
use databend_common_meta_app::schema::DictionaryIdentity;
use databend_common_meta_app::schema::GetSequenceReq;
use databend_common_meta_app::schema::SequenceIdent;
use databend_common_meta_app::schema::TableIndexType;
use databend_common_meta_app::schema::dictionary_name_ident::DictionaryNameIdent;
use databend_common_meta_app::storage::StorageParams;
use databend_common_storage::init_stage_operator;
use databend_common_users::Object;
use databend_common_users::UserApiProvider;
use derive_visitor::Drive;
use derive_visitor::DriveMut;
use derive_visitor::Visitor;
use derive_visitor::VisitorMut;
use itertools::Itertools;
use jsonb::keypath::KeyPath;
use jsonb::keypath::KeyPaths;
use jsonb::keypath::parse_key_paths;
use serde_json::json;
use serde_json::to_string;
use simsearch::SimSearch;
use tantivy_query_grammar::UserInputAst;
use tantivy_query_grammar::UserInputLeaf;
use tantivy_query_grammar::parse_query_lenient;
use unicase::Ascii;

use super::name_resolution::NameResolutionContext;
use super::normalize_identifier;
use crate::BaseTableColumn;
use crate::BindContext;
use crate::ColumnBinding;
use crate::ColumnEntry;
use crate::DefaultExprBinder;
use crate::IndexType;
use crate::MetadataRef;
use crate::UDFArgVisitor;
use crate::binder::Binder;
use crate::binder::ExprContext;
use crate::binder::InternalColumnBinding;
use crate::binder::NameResolutionResult;
use crate::binder::VirtualColumnName;
use crate::binder::bind_values;
use crate::binder::parse_stage_name;
use crate::binder::resolve_file_location;
use crate::binder::resolve_stage_location;
use crate::binder::resolve_stage_locations;
use crate::binder::wrap_cast;
use crate::optimizer::ir::RelExpr;
use crate::optimizer::ir::SExpr;
use crate::parse_lambda_expr;
use crate::planner::expression::UDFValidator;
use crate::planner::metadata::optimize_remove_count_args;
use crate::planner::semantic::lowering::TypeCheck;
use crate::plans::Aggregate;
use crate::plans::AggregateFunction;
use crate::plans::AggregateFunctionScalarSortDesc;
use crate::plans::AggregateMode;
use crate::plans::AsyncFunctionArgument;
use crate::plans::AsyncFunctionCall;
use crate::plans::BoundColumnRef;
use crate::plans::CastExpr;
use crate::plans::ConstantExpr;
use crate::plans::DictGetFunctionArgument;
use crate::plans::DictionarySource;
use crate::plans::FunctionCall;
use crate::plans::LagLeadFunction;
use crate::plans::LambdaFunc;
use crate::plans::NthValueFunction;
use crate::plans::NtileFunction;
use crate::plans::ReadFileFunctionArgument;
use crate::plans::RedisSource;
use crate::plans::ScalarExpr;
use crate::plans::ScalarItem;
use crate::plans::SqlSource;
use crate::plans::SubqueryComparisonOp;
use crate::plans::SubqueryExpr;
use crate::plans::SubqueryType;
use crate::plans::UDAFCall;
use crate::plans::UDFCall;
use crate::plans::UDFField;
use crate::plans::UDFLambdaCall;
use crate::plans::UDFLanguage;
use crate::plans::UDFScriptCode;
use crate::plans::UDFType;
use crate::plans::Visitor as ScalarVisitor;
use crate::plans::WindowFunc;
use crate::plans::WindowFuncFrame;
use crate::plans::WindowFuncFrameBound;
use crate::plans::WindowFuncFrameUnits;
use crate::plans::WindowFuncType;
use crate::plans::WindowOrderBy;

const DEFAULT_DECIMAL_PRECISION: i64 = 38;
const DEFAULT_DECIMAL_SCALE: i64 = 0;

#[derive(serde::Serialize, serde::Deserialize)]
pub struct StageLocationParam {
    pub param_name: String,
    pub relative_path: String,
    pub stage_info: StageInfo,
}

#[derive(Debug, Clone)]
struct UdfAsset {
    location: String,
    url: String,
    headers: BTreeMap<String, String>,
}

// UDF server expects unsigned types in UINT* form instead of SQL unsigned names.
fn udf_type_string(data_type: &DataType) -> String {
    let sql_name = data_type.sql_name();
    match sql_name.as_str() {
        "TINYINT UNSIGNED" => "UINT8".to_string(),
        "SMALLINT UNSIGNED" => "UINT16".to_string(),
        "INT UNSIGNED" => "UINT32".to_string(),
        "BIGINT UNSIGNED" => "UINT64".to_string(),
        _ => sql_name,
    }
}

fn build_udf_cloud_script(
    code: &str,
    handler: &str,
    imports: &[UdfAsset],
    packages: &[String],
    input_types: &[String],
    result_type: &str,
) -> Result<String> {
    const UDF_CLOUD_SCRIPT_TEMPLATE: &str = include_str!("udf_cloud_script.sh.tpl");
    const UDF_IMPORTS_BLOCK_TEMPLATE: &str = include_str!("udf_cloud_imports_block.sh.tpl");
    const UDF_IMPORT_DOWNLOADER_TEMPLATE: &str = include_str!("udf_import_downloader.sh.tpl");
    let server_stub = build_udf_cloud_server_stub(handler, input_types, result_type)?;
    let imports_json = if imports.is_empty() {
        None
    } else {
        let imports_payload = imports
            .iter()
            .map(|asset| {
                json!({
                    "location": asset.location.as_str(),
                    "url": asset.url.as_str(),
                    "headers": &asset.headers,
                })
            })
            .collect::<Vec<_>>();
        Some(to_string(&imports_payload)?)
    };
    let mut contents = vec![code, server_stub.as_str()];
    if let Some(json) = imports_json.as_ref() {
        contents.push(json);
    }
    let code_marker = unique_heredoc_marker("UDF_CODE", &contents);
    let server_marker = unique_heredoc_marker("UDF_SERVER", &contents);
    let imports_marker = imports_json
        .as_ref()
        .map(|_| unique_heredoc_marker("UDF_IMPORTS", &contents));

    let packages = packages
        .iter()
        .map(|pkg| pkg.trim())
        .filter(|pkg| !pkg.is_empty())
        .collect::<Vec<_>>();

    let packages_block = if packages.is_empty() {
        String::new()
    } else {
        let mut block = String::from("python -m pip install --no-cache-dir");
        for pkg in &packages {
            block.push(' ');
            block.push_str(pkg);
        }
        block
    };

    let imports_block = if let Some(json) = imports_json {
        let marker = imports_marker.expect("imports marker must exist");
        let mut replacements = BTreeMap::new();
        replacements.insert("IMPORTS_MARKER", marker);
        replacements.insert("IMPORTS_JSON", json);
        replacements.insert(
            "IMPORTS_DOWNLOADER",
            UDF_IMPORT_DOWNLOADER_TEMPLATE.to_string(),
        );
        render_template(UDF_IMPORTS_BLOCK_TEMPLATE, &replacements)?
    } else {
        String::new()
    };

    let mut replacements = BTreeMap::new();
    replacements.insert("PACKAGES_BLOCK", packages_block);
    replacements.insert("IMPORTS_BLOCK", imports_block);
    replacements.insert("CODE_MARKER", code_marker);
    replacements.insert("UDF_CODE", code.to_string());
    replacements.insert("SERVER_MARKER", server_marker);
    replacements.insert("SERVER_STUB", server_stub);
    render_template(UDF_CLOUD_SCRIPT_TEMPLATE, &replacements)
}

fn build_udf_cloud_server_stub(
    handler: &str,
    input_types: &[String],
    result_type: &str,
) -> Result<String> {
    const UDF_CLOUD_SERVER_STUB_TEMPLATE: &str = include_str!("udf_cloud_server_stub.py.tpl");
    let handler_literal = escape_python_double_quoted(handler);
    let input_types_literal = python_string_list(input_types);
    let result_type_literal = escape_python_double_quoted(result_type);
    let mut replacements = BTreeMap::new();
    replacements.insert("HANDLER_LITERAL", handler_literal);
    replacements.insert("INPUT_TYPES_LITERAL", input_types_literal);
    replacements.insert("RESULT_TYPE_LITERAL", result_type_literal);
    render_template(UDF_CLOUD_SERVER_STUB_TEMPLATE, &replacements)
}

fn render_template(template: &str, replacements: &BTreeMap<&str, String>) -> Result<String> {
    let mut output = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(start) = rest.find("{{") {
        output.push_str(&rest[..start]);
        let after_start = &rest[start + 2..];
        if let Some(end) = after_start.find("}}") {
            let key = &after_start[..end];
            let value = replacements.get(key).ok_or_else(|| {
                ErrorCode::Internal(format!("missing template placeholder '{key}'"))
            })?;
            output.push_str(value);
            rest = &after_start[end + 2..];
        } else {
            output.push_str(rest);
            return Ok(output);
        }
    }
    output.push_str(rest);
    Ok(output)
}

fn python_string_list(values: &[String]) -> String {
    let items = values
        .iter()
        .map(|value| format!("\"{}\"", escape_python_double_quoted(value)))
        .collect::<Vec<_>>()
        .join(", ");
    format!("[{items}]")
}

fn escape_python_double_quoted(value: &str) -> String {
    value.replace('\\', "\\\\").replace('"', "\\\"")
}

fn extract_script_metadata_deps(script: &str) -> Vec<String> {
    let mut ss = String::new();
    let mut meta_start = false;
    for line in script.lines() {
        if meta_start {
            if line.starts_with("# ///") {
                break;
            }
            ss.push_str(line.trim_start_matches('#').trim());
            ss.push('\n');
        }
        if !meta_start && line.starts_with("# /// script") {
            meta_start = true;
        }
    }

    let parsed = ss.parse::<toml::Value>().unwrap();

    if parsed.get("dependencies").is_none() {
        return Vec::new();
    }

    if let Some(deps) = parsed["dependencies"].as_array() {
        deps.iter()
            .filter_map(|value| value.as_str().map(|item| item.to_string()))
            .collect()
    } else {
        Vec::new()
    }
}

fn unique_heredoc_marker(base: &str, contents: &[&str]) -> String {
    let mut suffix = 0;
    let mut marker = base.to_string();
    while contents.iter().any(|content| content.contains(&marker)) {
        suffix += 1;
        marker = format!("{base}_{suffix}");
    }
    marker
}

/// A helper for type checking.
///
/// `TypeChecker::resolve` will resolve types of `Expr` and transform `Expr` into
/// a typed expression `Scalar`. At the same time, name resolution will be performed,
/// which check validity of unbound `ColumnRef` and try to replace it with qualified
/// `BoundColumnRef`.
///
/// If failed, a `SemanticError` will be raised. This may caused by incompatible
/// argument types of expressions, or unresolvable columns.
pub struct TypeChecker<'a> {
    bind_context: &'a mut BindContext,
    ctx: Arc<dyn TableContext>,
    dialect: Dialect,
    func_ctx: FunctionContext,
    name_resolution_ctx: &'a NameResolutionContext,
    metadata: MetadataRef,

    aliases: &'a [(String, ScalarExpr)],

    // true if current expr is inside an aggregate function.
    // This is used to check if there is nested aggregate function.
    in_aggregate_function: bool,

    // true if current expr is inside a window function.
    // This is used to allow aggregation function in window's aggregate function.
    in_window_function: bool,
    forbid_udf: bool,

    // true if currently resolving a masking policy expression.
    // This prevents infinite recursion when a masking policy references the masked column itself.
    in_masking_policy: bool,

    // Skip sequence existence checks when resolving `nextval`.
    skip_sequence_check: bool,
}

impl<'a> TypeChecker<'a> {
    pub fn try_create(
        bind_context: &'a mut BindContext,
        ctx: Arc<dyn TableContext>,
        name_resolution_ctx: &'a NameResolutionContext,
        metadata: MetadataRef,
        aliases: &'a [(String, ScalarExpr)],
        forbid_udf: bool,
    ) -> Result<Self> {
        let func_ctx = ctx.get_function_context()?;
        let dialect = ctx.get_settings().get_sql_dialect()?;
        Ok(Self {
            bind_context,
            ctx,
            dialect,
            func_ctx,
            name_resolution_ctx,
            metadata,
            aliases,
            in_aggregate_function: false,
            in_window_function: false,
            forbid_udf,
            in_masking_policy: false,
            skip_sequence_check: false,
        })
    }

    pub fn set_skip_sequence_check(&mut self, skip: bool) {
        self.skip_sequence_check = skip;
    }

    #[recursive::recursive]
    pub fn resolve(&mut self, expr: &Expr) -> Result<Box<(ScalarExpr, DataType)>> {
        match expr {
            Expr::ColumnRef {
                span,
                column:
                    ColumnRef {
                        database,
                        table,
                        column: ident,
                    },
            } => {
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
                                // First check: is DataMask feature enabled? (cheapest check)
                                && LicenseManagerSwitch::instance()
                                    .check_enterprise_enabled(self.ctx.get_license_key(), Feature::DataMask)
                                    .is_ok()
                                // Second check: does this column reference a table with masking policy?
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
                                            if let (Some(db), Some(tbl)) = (db_name.as_ref(), tbl_name.as_ref()) {
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

                                            if table_info.meta.column_mask_policy_columns_ids.is_empty()
                                            {
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
                                // Only do expensive async work if we know there's a policy
                                let mask_expr = databend_common_base::runtime::block_on(async {
                                    self.get_masking_policy_expr_for_column(
                                        &column,
                                        database.as_deref(),
                                        table.as_deref(),
                                    )
                                    .await
                                })?;

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
                            (
                                BoundColumnRef {
                                    span: *span,
                                    column,
                                }
                                .into(),
                                data_type,
                            )
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
                        (
                            BoundColumnRef {
                                span: *span,
                                column,
                            }
                            .into(),
                            data_type,
                        )
                    }
                    NameResolutionResult::Alias { scalar, .. } => {
                        (scalar.clone(), scalar.data_type()?)
                    }
                };

                Ok(Box::new((scalar, data_type)))
            }

            Expr::IsNull {
                span, expr, not, ..
            } => {
                let args = &[expr.as_ref()];
                if *not {
                    self.resolve_function(*span, "is_not_null", vec![], args)
                } else {
                    self.resolve_function(*span, "is_null", vec![], args)
                }
            }

            Expr::IsDistinctFrom {
                span,
                left,
                right,
                not,
            } => {
                let left_null_expr = Box::new(Expr::IsNull {
                    span: *span,
                    expr: left.clone(),
                    not: false,
                });
                let right_null_expr = Box::new(Expr::IsNull {
                    span: *span,
                    expr: right.clone(),
                    not: false,
                });
                let op = if *not {
                    BinaryOperator::Eq
                } else {
                    BinaryOperator::NotEq
                };
                let (scalar, _) = *self.resolve_function(*span, "if", vec![], &[
                    &Expr::BinaryOp {
                        span: *span,
                        op: BinaryOperator::And,
                        left: left_null_expr.clone(),
                        right: right_null_expr.clone(),
                    },
                    &Expr::Literal {
                        span: *span,
                        value: Literal::Boolean(*not),
                    },
                    &Expr::BinaryOp {
                        span: *span,
                        op: BinaryOperator::Or,
                        left: left_null_expr.clone(),
                        right: right_null_expr.clone(),
                    },
                    &Expr::Literal {
                        span: *span,
                        value: Literal::Boolean(!*not),
                    },
                    &Expr::BinaryOp {
                        span: *span,
                        op,
                        left: left.clone(),
                        right: right.clone(),
                    },
                ])?;
                self.resolve_scalar_function_call(*span, "assume_not_null", vec![], vec![scalar])
            }

            Expr::InList {
                span,
                expr,
                list,
                not,
                ..
            } => {
                if list.len() >= self.ctx.get_settings().get_inlist_to_join_threshold()? {
                    if *not {
                        return self.resolve_unary_op(*span, &UnaryOperator::Not, &Expr::InList {
                            span: *span,
                            expr: expr.clone(),
                            list: list.clone(),
                            not: false,
                        });
                    }
                    return self.convert_inlist_to_subquery(expr, list);
                }

                let get_max_inlist_to_or = self.ctx.get_settings().get_max_inlist_to_or()? as usize;
                if list.len() > get_max_inlist_to_or && list.iter().all(satisfy_contain_func) {
                    let array_expr = Expr::Array {
                        span: *span,
                        exprs: list.clone(),
                    };
                    // Deduplicate the array.
                    let array_expr = Expr::FunctionCall {
                        span: *span,
                        func: ASTFunctionCall {
                            name: Identifier::from_name(*span, "array_distinct"),
                            args: vec![array_expr],
                            params: vec![],
                            order_by: vec![],
                            window: None,
                            lambda: None,
                            distinct: false,
                        },
                    };
                    let args = vec![&array_expr, expr.as_ref()];
                    if *not {
                        self.resolve_unary_op(*span, &UnaryOperator::Not, &Expr::FunctionCall {
                            span: *span,
                            func: ASTFunctionCall {
                                distinct: false,
                                name: Identifier::from_name(*span, "contains"),
                                args: args.iter().copied().cloned().collect(),
                                params: vec![],
                                order_by: vec![],
                                window: None,
                                lambda: None,
                            },
                        })
                    } else {
                        self.resolve_function(*span, "contains", vec![], &args)
                    }
                } else {
                    let mut result = list
                        .iter()
                        .map(|e| Expr::BinaryOp {
                            span: *span,
                            op: BinaryOperator::Eq,
                            left: expr.clone(),
                            right: Box::new(e.clone()),
                        })
                        .fold(None, |mut acc, e| {
                            match acc.as_mut() {
                                None => acc = Some(e),
                                Some(acc) => {
                                    *acc = Expr::BinaryOp {
                                        span: *span,
                                        op: BinaryOperator::Or,
                                        left: Box::new(acc.clone()),
                                        right: Box::new(e),
                                    }
                                }
                            }
                            acc
                        })
                        .unwrap();

                    if *not {
                        result = Expr::UnaryOp {
                            span: *span,
                            op: UnaryOperator::Not,
                            expr: Box::new(result),
                        };
                    }
                    self.resolve(&result)
                }
            }

            Expr::Between {
                span,
                expr,
                low,
                high,
                not,
                ..
            } => {
                if !*not {
                    // Rewrite `expr BETWEEN low AND high`
                    // into `expr >= low AND expr <= high`
                    let (ge_func, _left_type) = *self.resolve_binary_op(
                        *span,
                        &BinaryOperator::Gte,
                        expr.as_ref(),
                        low.as_ref(),
                    )?;
                    let (le_func, _right_type) = *self.resolve_binary_op(
                        *span,
                        &BinaryOperator::Lte,
                        expr.as_ref(),
                        high.as_ref(),
                    )?;

                    self.resolve_scalar_function_call(*span, "and", vec![], vec![
                        ge_func.clone(),
                        le_func.clone(),
                    ])
                } else {
                    // Rewrite `expr NOT BETWEEN low AND high`
                    // into `expr < low OR expr > high`
                    let (lt_func, _left_type) = *self.resolve_binary_op(
                        *span,
                        &BinaryOperator::Lt,
                        expr.as_ref(),
                        low.as_ref(),
                    )?;
                    let (gt_func, _right_type) = *self.resolve_binary_op(
                        *span,
                        &BinaryOperator::Gt,
                        expr.as_ref(),
                        high.as_ref(),
                    )?;

                    self.resolve_scalar_function_call(*span, "or", vec![], vec![lt_func, gt_func])
                }
            }

            Expr::BinaryOp {
                span,
                op,
                left,
                right,
                ..
            } => self.resolve_binary_op_or_subquery(span, op, left, right),

            Expr::JsonOp {
                span,
                op,
                left,
                right,
            } => {
                let func_name = op.to_func_name();
                self.resolve_function(*span, func_name.as_str(), vec![], &[left, right])
            }

            Expr::UnaryOp { span, op, expr, .. } => self.resolve_unary_op(*span, op, expr.as_ref()),

            Expr::Cast {
                expr, target_type, ..
            } => {
                let box (scalar, data_type) = self.resolve(expr)?;
                if target_type == &TypeName::Variant {
                    if let Some(result) =
                        self.resolve_cast_to_variant(expr.span(), &data_type, &scalar, false)
                    {
                        return result;
                    }
                }

                let raw_expr = RawExpr::Cast {
                    span: expr.span(),
                    is_try: false,
                    expr: Box::new(scalar.as_raw_expr()),
                    dest_type: DataType::from(&resolve_type_name(target_type, true)?),
                };
                let registry = &BUILTIN_FUNCTIONS;
                let checked_expr = type_check::check(&raw_expr, registry)?;

                if let Some(constant) = self.try_fold_constant(&checked_expr, false) {
                    return Ok(constant);
                }

                // cast variant to other type should nest wrap nullable,
                // as we cast JSON null to SQL NULL.
                let target_type = if data_type.remove_nullable() == DataType::Variant {
                    let target_type = checked_expr.data_type().nest_wrap_nullable();
                    target_type
                // if the source type is nullable, cast target type should also be nullable.
                } else if data_type.is_nullable_or_null() {
                    checked_expr.data_type().wrap_nullable()
                } else {
                    checked_expr.data_type().clone()
                };

                Ok(Box::new((
                    CastExpr {
                        span: expr.span(),
                        is_try: false,
                        argument: Box::new(scalar),
                        target_type: Box::new(target_type.clone()),
                    }
                    .into(),
                    target_type,
                )))
            }

            Expr::TryCast {
                expr, target_type, ..
            } => {
                let box (scalar, data_type) = self.resolve(expr)?;
                if target_type == &TypeName::Variant {
                    if let Some(result) =
                        self.resolve_cast_to_variant(expr.span(), &data_type, &scalar, true)
                    {
                        return result;
                    }
                }

                let raw_expr = RawExpr::Cast {
                    span: expr.span(),
                    is_try: true,
                    expr: Box::new(scalar.as_raw_expr()),
                    dest_type: DataType::from(&resolve_type_name(target_type, true)?),
                };
                let registry = &BUILTIN_FUNCTIONS;
                let checked_expr = type_check::check(&raw_expr, registry)?;

                if let Some(constant) = self.try_fold_constant(&checked_expr, false) {
                    return Ok(constant);
                }

                // cast variant to other type should nest wrap nullable,
                // as we cast JSON null to SQL NULL.
                let target_type = if data_type.remove_nullable() == DataType::Variant {
                    let target_type = checked_expr.data_type().nest_wrap_nullable();
                    target_type
                } else {
                    checked_expr.data_type().clone()
                };
                Ok(Box::new((
                    CastExpr {
                        span: expr.span(),
                        is_try: true,
                        argument: Box::new(scalar),
                        target_type: Box::new(target_type.clone()),
                    }
                    .into(),
                    target_type,
                )))
            }

            Expr::Case {
                span,
                operand,
                conditions,
                results,
                else_result,
            } => {
                let mut arguments = Vec::with_capacity(conditions.len() * 2 + 1);
                for (c, r) in conditions.iter().zip(results.iter()) {
                    match operand {
                        Some(operand) => {
                            // compare case operand with each conditions until one of them is equal
                            let equal_expr = Expr::FunctionCall {
                                span: *span,
                                func: ASTFunctionCall {
                                    distinct: false,
                                    name: Identifier::from_name(*span, "eq"),
                                    args: vec![*operand.clone(), c.clone()],
                                    params: vec![],
                                    order_by: vec![],
                                    window: None,
                                    lambda: None,
                                },
                            };
                            arguments.push(equal_expr)
                        }
                        None => arguments.push(c.clone()),
                    }
                    arguments.push(r.clone());
                }
                let null_arg = Expr::Literal {
                    span: None,
                    value: Literal::Null,
                };

                if let Some(expr) = else_result {
                    arguments.push(*expr.clone());
                } else {
                    arguments.push(null_arg)
                }
                let args_ref: Vec<&Expr> = arguments.iter().collect();

                self.resolve_function(*span, "if", vec![], &args_ref)
            }

            Expr::Substring {
                span,
                expr,
                substring_from,
                substring_for,
                ..
            } => {
                let mut arguments = vec![expr.as_ref(), substring_from.as_ref()];
                if let Some(substring_for) = substring_for {
                    arguments.push(substring_for.as_ref());
                }
                self.resolve_function(*span, "substring", vec![], &arguments)
            }

            Expr::Literal { span, value } => self.resolve_literal(*span, value),

            Expr::FunctionCall {
                span,
                func:
                    ASTFunctionCall {
                        distinct,
                        name,
                        args,
                        params,
                        order_by,
                        window,
                        lambda,
                    },
            } => {
                let func_name = normalize_identifier(name, self.name_resolution_ctx).to_string();
                let func_name = func_name.as_str();
                let uni_case_func_name = Ascii::new(func_name);
                if !is_builtin_function(func_name)
                    && !Self::all_sugar_functions().contains(&uni_case_func_name)
                {
                    if let Some(udf) = self.resolve_udf(*span, func_name, args)? {
                        return Ok(udf);
                    }

                    // Function not found, try to find and suggest similar function name.
                    let all_funcs = BUILTIN_FUNCTIONS
                        .all_function_names()
                        .into_iter()
                        .chain(AggregateFunctionFactory::instance().registered_names())
                        .chain(
                            GENERAL_WINDOW_FUNCTIONS
                                .iter()
                                .cloned()
                                .map(|ascii| ascii.into_inner().to_string()),
                        )
                        .chain(
                            GENERAL_LAMBDA_FUNCTIONS
                                .iter()
                                .cloned()
                                .map(|ascii| ascii.into_inner().to_string()),
                        )
                        .chain(
                            GENERAL_SEARCH_FUNCTIONS
                                .iter()
                                .cloned()
                                .map(|ascii| ascii.into_inner().to_string()),
                        )
                        .chain(
                            ASYNC_FUNCTIONS
                                .iter()
                                .cloned()
                                .map(|ascii| ascii.into_inner().to_string()),
                        )
                        .chain(
                            Self::all_sugar_functions()
                                .iter()
                                .cloned()
                                .map(|ascii| ascii.into_inner().to_string()),
                        );
                    let mut engine: SimSearch<String> = SimSearch::new();
                    for func_name in all_funcs {
                        engine.insert(func_name.clone(), &func_name);
                    }
                    let possible_funcs = engine
                        .search(func_name)
                        .iter()
                        .map(|name| format!("'{name}'"))
                        .collect::<Vec<_>>();
                    if possible_funcs.is_empty() {
                        return Err(ErrorCode::UnknownFunction(format!(
                            "no function matches the given name: {func_name}"
                        ))
                        .set_span(*span));
                    } else {
                        return Err(ErrorCode::UnknownFunction(format!(
                            "no function matches the given name: '{func_name}', do you mean {}?",
                            possible_funcs.join(", ")
                        ))
                        .set_span(*span));
                    }
                }

                // check within group legal
                if !order_by.is_empty()
                    && !GENERAL_WITHIN_GROUP_FUNCTIONS.contains(&uni_case_func_name)
                {
                    return Err(ErrorCode::SemanticError(
                        "only aggregate functions allowed in within group syntax",
                    )
                    .set_span(*span));
                }
                // check window function legal
                if window.is_some()
                    && !AggregateFunctionFactory::instance().contains(func_name)
                    && !GENERAL_WINDOW_FUNCTIONS.contains(&uni_case_func_name)
                {
                    return Err(ErrorCode::SemanticError(
                        "only window and aggregate functions allowed in window syntax",
                    )
                    .set_span(*span));
                }
                // check lambda function legal
                if lambda.is_some() && !GENERAL_LAMBDA_FUNCTIONS.contains(&uni_case_func_name) {
                    return Err(ErrorCode::SemanticError(
                        "only lambda functions allowed in lambda syntax",
                    )
                    .set_span(*span));
                }

                let args: Vec<&Expr> = args.iter().collect();

                if GENERAL_WINDOW_FUNCTIONS.contains(&uni_case_func_name) {
                    // general window function
                    if window.is_none() {
                        return Err(ErrorCode::SemanticError(format!(
                            "window function {func_name} can only be used in window clause"
                        ))
                        .set_span(*span));
                    }
                    let window = window.as_ref().unwrap();
                    if !RANK_WINDOW_FUNCTIONS.contains(&func_name) && window.ignore_nulls.is_some()
                    {
                        return Err(ErrorCode::SemanticError(format!(
                            "window function {func_name} not support IGNORE/RESPECT NULLS option"
                        ))
                        .set_span(*span));
                    }
                    let func = self.resolve_general_window_function(
                        *span,
                        func_name,
                        &args,
                        &window.ignore_nulls,
                    )?;
                    let display_name = format!("{:#}", expr);
                    self.resolve_window(*span, display_name, &window.window, func)
                } else if AggregateFunctionFactory::instance().contains(func_name) {
                    let mut new_params = Vec::with_capacity(params.len());
                    for param in params {
                        let box (scalar, _data_type) = self.resolve(param)?;
                        let expr = scalar.as_expr()?;
                        let (expr, _) =
                            ConstantFolder::fold(&expr, &self.func_ctx, &BUILTIN_FUNCTIONS);
                        let constant = expr
                            .into_constant()
                            .map_err(|_| {
                                ErrorCode::SemanticError(format!(
                                    "invalid parameter {param} for aggregate function, expected constant",
                                ))
                                    .set_span(*span)
                            })?
                            .scalar;
                        new_params.push(constant);
                    }
                    let in_window = self.in_window_function;
                    self.in_window_function = self.in_window_function || window.is_some();
                    let in_aggregate_function = self.in_aggregate_function;
                    let (new_agg_func, data_type) = self.resolve_aggregate_function(
                        *span, func_name, expr, *distinct, new_params, &args, order_by,
                    )?;
                    self.in_window_function = in_window;
                    self.in_aggregate_function = in_aggregate_function;
                    if let Some(window) = window {
                        // aggregate window function
                        let display_name = format!("{:#}", expr);
                        if window.ignore_nulls.is_some() {
                            return Err(ErrorCode::SemanticError(format!(
                                "window function {func_name} not support IGNORE/RESPECT NULLS option"
                            ))
                                .set_span(*span));
                        }
                        // general window function
                        let func = WindowFuncType::Aggregate(new_agg_func);
                        self.resolve_window(*span, display_name, &window.window, func)
                    } else {
                        // aggregate function
                        Ok(Box::new((new_agg_func.into(), data_type)))
                    }
                } else if GENERAL_LAMBDA_FUNCTIONS.contains(&uni_case_func_name) {
                    if lambda.is_none() {
                        return Err(ErrorCode::SemanticError(format!(
                            "function {func_name} must have a lambda expression",
                        ))
                        .set_span(*span));
                    }
                    let lambda = lambda.as_ref().unwrap();
                    self.resolve_lambda_function(*span, func_name, &args, lambda)
                } else if GENERAL_SEARCH_FUNCTIONS.contains(&uni_case_func_name) {
                    match func_name.to_lowercase().as_str() {
                        "score" => self.resolve_score_search_function(*span, func_name, &args),
                        "match" => self.resolve_match_search_function(*span, func_name, &args),
                        "query" => self.resolve_query_search_function(*span, func_name, &args),
                        _ => {
                            return Err(ErrorCode::SemanticError(format!(
                                "cannot find search function {}",
                                func_name
                            ))
                            .set_span(*span));
                        }
                    }
                } else if ASYNC_FUNCTIONS.contains(&uni_case_func_name) {
                    self.resolve_async_function(*span, func_name, &args)
                } else if BUILTIN_FUNCTIONS
                    .get_property(func_name)
                    .map(|property| property.kind == FunctionKind::SRF)
                    .unwrap_or(false)
                {
                    // Set returning function
                    self.resolve_set_returning_function(*span, func_name, &args)
                } else {
                    // Scalar function
                    let mut new_params: Vec<Scalar> = Vec::with_capacity(params.len());
                    for param in params {
                        let box (scalar, _) = self.resolve(param)?;
                        let expr = scalar.as_expr()?;
                        let (expr, _) =
                            ConstantFolder::fold(&expr, &self.func_ctx, &BUILTIN_FUNCTIONS);
                        let constant = expr
                            .into_constant()
                            .map_err(|_| {
                                ErrorCode::SemanticError(format!(
                                    "invalid parameter {param} for scalar function, expected constant",
                                ))
                                .set_span(*span)
                            })?
                            .scalar;
                        new_params.push(constant);
                    }
                    self.resolve_function(*span, func_name, new_params, &args)
                }
            }

            Expr::CountAll { span, window, .. } => {
                let (new_agg_func, data_type) =
                    self.resolve_aggregate_function(*span, "count", expr, false, vec![], &[], &[])?;

                if let Some(window) = window {
                    // aggregate window function
                    let display_name = format!("{:#}", expr);
                    let func = WindowFuncType::Aggregate(new_agg_func);
                    self.resolve_window(*span, display_name, window, func)
                } else {
                    // aggregate function
                    Ok(Box::new((new_agg_func.into(), data_type)))
                }
            }

            Expr::Exists { subquery, not, .. } => self.resolve_subquery(
                if !*not {
                    SubqueryType::Exists
                } else {
                    SubqueryType::NotExists
                },
                subquery,
                None,
                None,
            ),

            Expr::Subquery { subquery, .. } => {
                self.resolve_subquery(SubqueryType::Scalar, subquery, None, None)
            }

            Expr::InSubquery {
                subquery,
                not,
                expr,
                span,
            } => {
                // Not in subquery will be transformed to not(Expr = Any(...))
                if *not {
                    return self.resolve_unary_op(*span, &UnaryOperator::Not, &Expr::InSubquery {
                        subquery: subquery.clone(),
                        not: false,
                        expr: expr.clone(),
                        span: *span,
                    });
                }
                // InSubquery will be transformed to Expr = Any(...)
                self.resolve_subquery(
                    SubqueryType::Any,
                    subquery,
                    Some(*expr.clone()),
                    Some(SubqueryComparisonOp::Equal),
                )
            }

            Expr::LikeSubquery {
                subquery,
                expr,
                span,
                modifier,
                escape,
            } => self.resolve_scalar_subquery(
                subquery,
                expr,
                span,
                span,
                modifier,
                &BinaryOperator::Like(escape.clone()),
            ),

            Expr::LikeAnyWithEscape {
                span,
                left,
                right,
                escape,
            } => self.resolve_binary_op_or_subquery(
                span,
                &BinaryOperator::LikeAny(Some(escape.clone())),
                left,
                right,
            ),

            Expr::LikeWithEscape {
                span,
                left,
                right,
                is_not,
                escape,
            } => {
                let like_op = if *is_not {
                    BinaryOperator::NotLike(Some(escape.clone()))
                } else {
                    BinaryOperator::Like(Some(escape.clone()))
                };

                self.resolve_binary_op_or_subquery(span, &like_op, left, right)
            }

            expr @ Expr::MapAccess { span, .. } => {
                let mut expr = expr;
                let mut paths = VecDeque::new();
                while let Expr::MapAccess {
                    span,
                    expr: inner_expr,
                    accessor,
                } = expr
                {
                    expr = &**inner_expr;
                    let path = match accessor {
                        MapAccessor::Bracket {
                            key: box Expr::Literal { value, .. },
                        } => {
                            if !matches!(value, Literal::UInt64(_) | Literal::String(_)) {
                                return Err(ErrorCode::SemanticError(format!(
                                    "Unsupported accessor: {:?}",
                                    value
                                ))
                                .set_span(*span));
                            }
                            value.clone()
                        }
                        MapAccessor::Colon { key } => Literal::String(key.name.clone()),
                        MapAccessor::DotNumber { key } => Literal::UInt64(*key),
                        _ => {
                            return Err(ErrorCode::SemanticError(format!(
                                "Unsupported accessor: {:?}",
                                accessor
                            ))
                            .set_span(*span));
                        }
                    };
                    paths.push_front((*span, path));
                }
                self.resolve_map_access(*span, expr, paths)
            }

            Expr::Extract {
                span, kind, expr, ..
            } => self.resolve_extract_expr(*span, kind, expr),

            Expr::DatePart {
                span, kind, expr, ..
            } => self.resolve_extract_expr(*span, kind, expr),

            Expr::Interval { span, expr, unit } => {
                let ex = Expr::Cast {
                    span: *span,
                    expr: Box::new(expr.as_ref().clone()),
                    target_type: TypeName::String,
                    pg_style: false,
                };
                let ex = Expr::FunctionCall {
                    span: *span,
                    func: ASTFunctionCall {
                        name: Identifier::from_name(None, "concat".to_string()),
                        args: vec![ex, Expr::Literal {
                            span: *span,
                            value: Literal::String(format!(" {}", unit)),
                        }],
                        params: vec![],
                        distinct: false,
                        order_by: vec![],
                        window: None,
                        lambda: None,
                    },
                };
                let ex = Expr::FunctionCall {
                    span: *span,
                    func: ASTFunctionCall {
                        name: Identifier::from_name(None, "to_interval".to_string()),
                        args: vec![ex],
                        params: vec![],
                        distinct: false,
                        order_by: vec![],
                        window: None,
                        lambda: None,
                    },
                };
                self.resolve(&ex)
            }
            Expr::DateAdd {
                span,
                unit,
                interval,
                date,
                ..
            } => self.resolve_date_arith(*span, unit, interval, date, expr),
            Expr::DateDiff {
                span,
                unit,
                date_start,
                date_end,
                ..
            } => self.resolve_date_arith(*span, unit, date_start, date_end, expr),
            Expr::DateBetween {
                span,
                unit,
                date_start,
                date_end,
                ..
            } => self.resolve_date_arith(*span, unit, date_start, date_end, expr),
            Expr::DateSub {
                span,
                unit,
                interval,
                date,
                ..
            } => self.resolve_date_arith(
                *span,
                unit,
                &Expr::UnaryOp {
                    span: *span,
                    op: UnaryOperator::Minus,
                    expr: interval.clone(),
                },
                date,
                expr,
            ),
            Expr::DateTrunc {
                span, unit, date, ..
            } => self.resolve_date_trunc(*span, date, unit),
            Expr::TimeSlice {
                span,
                unit,
                date,
                slice_length,
                start_or_end,
            } => {
                self.resolve_time_slice(*span, date, *slice_length, unit, start_or_end.to_string())
            }
            Expr::LastDay {
                span, unit, date, ..
            } => self.resolve_last_day(*span, date, unit),
            Expr::PreviousDay {
                span, unit, date, ..
            } => self.resolve_previous_or_next_day(*span, date, unit, true),
            Expr::NextDay {
                span, unit, date, ..
            } => self.resolve_previous_or_next_day(*span, date, unit, false),
            Expr::Trim {
                span,
                expr,
                trim_where,
                ..
            } => self.resolve_trim_function(*span, expr, trim_where),

            Expr::Array { span, exprs, .. } => self.resolve_array(*span, exprs),

            Expr::Position {
                substr_expr,
                str_expr,
                span,
                ..
            } => self.resolve_function(*span, "locate", vec![], &[
                substr_expr.as_ref(),
                str_expr.as_ref(),
            ]),

            Expr::Map { span, kvs, .. } => self.resolve_map(*span, kvs),

            Expr::Tuple { span, exprs, .. } => self.resolve_tuple(*span, exprs),

            Expr::Hole { span, .. } | Expr::Placeholder { span } => {
                return Err(ErrorCode::SemanticError(
                    "Hole or Placeholder expression is impossible in trivial query".to_string(),
                )
                .set_span(*span));
            }
            Expr::StageLocation { span, location } => self.resolve_stage_location(*span, location),
        }
    }

    fn resolve_binary_op_or_subquery(
        &mut self,
        span: &Span,
        op: &BinaryOperator,
        left: &Expr,
        right: &Expr,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        if let Expr::Subquery {
            subquery,
            modifier: Some(subquery_modifier),
            ..
        } = right
        {
            self.resolve_scalar_subquery(subquery, left, span, &right.span(), subquery_modifier, op)
        } else {
            self.resolve_binary_op(*span, op, left, right)
        }
    }

    fn resolve_scalar_subquery(
        &mut self,
        subquery: &Query,
        expr: &Expr,
        span: &Span,
        right_span: &Span,
        modifier: &SubqueryModifier,
        op: &BinaryOperator,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        Ok(match modifier {
            SubqueryModifier::Any | SubqueryModifier::Some => {
                let comparison_op = SubqueryComparisonOp::try_from(op)?;
                self.resolve_subquery(
                    SubqueryType::Any,
                    subquery,
                    Some(expr.clone()),
                    Some(comparison_op),
                )?
            }
            SubqueryModifier::All => {
                let contrary_op = op.to_contrary()?;
                let rewritten_subquery = Expr::Subquery {
                    span: *right_span,
                    modifier: Some(SubqueryModifier::Any),
                    subquery: Box::new(subquery.clone()),
                };
                self.resolve_unary_op(*span, &UnaryOperator::Not, &Expr::BinaryOp {
                    span: *span,
                    op: contrary_op,
                    left: Box::new(expr.clone()),
                    right: Box::new(rewritten_subquery),
                })?
            }
        })
    }

    // TODO: remove this function
    fn rewrite_substring(args: &mut [ScalarExpr]) {
        if let ScalarExpr::ConstantExpr(expr) = &args[1] {
            if let Scalar::Number(NumberScalar::UInt8(0)) = expr.value {
                args[1] = ConstantExpr {
                    span: expr.span,
                    value: Scalar::Number(1i64.into()),
                }
                .into();
            }
        }
    }

    fn resolve_window(
        &mut self,
        span: Span,
        display_name: String,
        window: &Window,
        func: WindowFuncType,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        if self.in_aggregate_function {
            // Reset the state
            self.in_aggregate_function = false;
            return Err(ErrorCode::SemanticError(
                "aggregate function calls cannot contain window function calls".to_string(),
            )
            .set_span(span));
        }
        if self.in_window_function {
            // Reset the state
            self.in_window_function = false;
            return Err(ErrorCode::SemanticError(
                "window function calls cannot be nested".to_string(),
            )
            .set_span(span));
        }

        let spec = match window {
            Window::WindowSpec(spec) => spec.clone(),
            Window::WindowReference(w) => self
                .bind_context
                .window_definitions
                .get(&w.window_name.name)
                .ok_or_else(|| {
                    ErrorCode::SyntaxException(format!(
                        "Window definition {} not found",
                        w.window_name.name
                    ))
                })?
                .value()
                .clone(),
        };

        self.in_window_function = true;
        let mut partitions = Vec::with_capacity(spec.partition_by.len());
        for p in spec.partition_by.iter() {
            let box (part, _part_type) = self.resolve(p)?;
            partitions.push(part);
        }

        let mut order_by = Vec::with_capacity(spec.order_by.len());
        for o in spec.order_by.iter() {
            let box (order, _) = self.resolve(&o.expr)?;

            if matches!(order, ScalarExpr::ConstantExpr(_)) {
                continue;
            }

            order_by.push(WindowOrderBy {
                expr: order,
                asc: o.asc,
                nulls_first: o.nulls_first,
            })
        }
        self.in_window_function = false;

        let frame =
            self.resolve_window_frame(span, &func, &mut order_by, spec.window_frame.clone())?;

        if matches!(&frame.start_bound, WindowFuncFrameBound::Following(None)) {
            return Err(ErrorCode::SemanticError(
                "Frame start cannot be UNBOUNDED FOLLOWING".to_string(),
            )
            .set_span(span));
        }

        if matches!(&frame.end_bound, WindowFuncFrameBound::Preceding(None)) {
            return Err(ErrorCode::SemanticError(
                "Frame end cannot be UNBOUNDED PRECEDING".to_string(),
            )
            .set_span(span));
        }

        let data_type = func.return_type();
        let window_func = WindowFunc {
            span,
            display_name,
            func,
            partition_by: partitions,
            order_by,
            frame,
        };
        Ok(Box::new((window_func.into(), data_type)))
    }

    // just support integer
    #[inline]
    fn resolve_rows_offset(&self, expr: &Expr) -> Result<Scalar> {
        if let Expr::Literal { value, .. } = expr {
            let box (value, _) = self.resolve_literal_scalar(value)?;
            match value {
                Scalar::Number(NumberScalar::UInt8(v)) => {
                    return Ok(Scalar::Number(NumberScalar::UInt64(v as u64)));
                }
                Scalar::Number(NumberScalar::UInt16(v)) => {
                    return Ok(Scalar::Number(NumberScalar::UInt64(v as u64)));
                }
                Scalar::Number(NumberScalar::UInt32(v)) => {
                    return Ok(Scalar::Number(NumberScalar::UInt64(v as u64)));
                }
                Scalar::Number(NumberScalar::UInt64(_)) => return Ok(value),
                _ => {}
            }
        }

        Err(ErrorCode::SemanticError(
            "Only unsigned numbers are allowed in ROWS offset".to_string(),
        )
        .set_span(expr.span()))
    }

    #[inline]
    fn resolve_literal(
        &self,
        span: Span,
        literal: &databend_common_ast::ast::Literal,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let box (value, data_type) = self.resolve_literal_scalar(literal)?;

        let scalar_expr = ScalarExpr::ConstantExpr(ConstantExpr { span, value });
        Ok(Box::new((scalar_expr, data_type)))
    }

    fn resolve_window_rows_frame(&self, frame: WindowFrame) -> Result<WindowFuncFrame> {
        let units = match frame.units {
            WindowFrameUnits::Rows => WindowFuncFrameUnits::Rows,
            WindowFrameUnits::Range => WindowFuncFrameUnits::Range,
        };
        let start = match frame.start_bound {
            WindowFrameBound::CurrentRow => WindowFuncFrameBound::CurrentRow,
            WindowFrameBound::Preceding(f) => {
                if let Some(box expr) = f {
                    WindowFuncFrameBound::Preceding(Some(self.resolve_rows_offset(&expr)?))
                } else {
                    WindowFuncFrameBound::Preceding(None)
                }
            }
            WindowFrameBound::Following(f) => {
                if let Some(box expr) = f {
                    WindowFuncFrameBound::Following(Some(self.resolve_rows_offset(&expr)?))
                } else {
                    WindowFuncFrameBound::Following(None)
                }
            }
        };
        let end = match frame.end_bound {
            WindowFrameBound::CurrentRow => WindowFuncFrameBound::CurrentRow,
            WindowFrameBound::Preceding(f) => {
                if let Some(box expr) = f {
                    WindowFuncFrameBound::Preceding(Some(self.resolve_rows_offset(&expr)?))
                } else {
                    WindowFuncFrameBound::Preceding(None)
                }
            }
            WindowFrameBound::Following(f) => {
                if let Some(box expr) = f {
                    WindowFuncFrameBound::Following(Some(self.resolve_rows_offset(&expr)?))
                } else {
                    WindowFuncFrameBound::Following(None)
                }
            }
        };

        Ok(WindowFuncFrame {
            units,
            start_bound: start,
            end_bound: end,
        })
    }

    fn resolve_range_offset(&mut self, bound: &WindowFrameBound) -> Result<Option<Scalar>> {
        match bound {
            WindowFrameBound::Following(Some(box expr))
            | WindowFrameBound::Preceding(Some(box expr)) => {
                let box (expr, _) = self.resolve(expr)?;
                let (expr, _) =
                    ConstantFolder::fold(&expr.as_expr()?, &self.func_ctx, &BUILTIN_FUNCTIONS);
                match expr.into_constant() {
                    Ok(expr::Constant { scalar, .. }) => Ok(Some(scalar)),
                    Err(expr) => Err(ErrorCode::SemanticError(
                        "Only constant is allowed in RANGE offset".to_string(),
                    )
                    .set_span(expr.span())),
                }
            }
            _ => Ok(None),
        }
    }

    fn resolve_window_range_frame(&mut self, frame: WindowFrame) -> Result<WindowFuncFrame> {
        let start_offset = self.resolve_range_offset(&frame.start_bound)?;
        let end_offset = self.resolve_range_offset(&frame.end_bound)?;

        let units = match frame.units {
            WindowFrameUnits::Rows => WindowFuncFrameUnits::Rows,
            WindowFrameUnits::Range => WindowFuncFrameUnits::Range,
        };
        let start = match frame.start_bound {
            WindowFrameBound::CurrentRow => WindowFuncFrameBound::CurrentRow,
            WindowFrameBound::Preceding(_) => WindowFuncFrameBound::Preceding(start_offset),
            WindowFrameBound::Following(_) => WindowFuncFrameBound::Following(start_offset),
        };
        let end = match frame.end_bound {
            WindowFrameBound::CurrentRow => WindowFuncFrameBound::CurrentRow,
            WindowFrameBound::Preceding(_) => WindowFuncFrameBound::Preceding(end_offset),
            WindowFrameBound::Following(_) => WindowFuncFrameBound::Following(end_offset),
        };

        Ok(WindowFuncFrame {
            units,
            start_bound: start,
            end_bound: end,
        })
    }

    fn resolve_window_frame(
        &mut self,
        span: Span,
        func: &WindowFuncType,
        order_by: &mut [WindowOrderBy],
        window_frame: Option<WindowFrame>,
    ) -> Result<WindowFuncFrame> {
        match func {
            WindowFuncType::PercentRank => {
                return Ok(WindowFuncFrame {
                    units: WindowFuncFrameUnits::Rows,
                    start_bound: WindowFuncFrameBound::Preceding(None),
                    end_bound: WindowFuncFrameBound::Following(None),
                });
            }
            WindowFuncType::LagLead(lag_lead) if lag_lead.is_lag => {
                return Ok(WindowFuncFrame {
                    units: WindowFuncFrameUnits::Rows,
                    start_bound: WindowFuncFrameBound::Preceding(Some(Scalar::Number(
                        NumberScalar::UInt64(lag_lead.offset),
                    ))),
                    end_bound: WindowFuncFrameBound::Preceding(Some(Scalar::Number(
                        NumberScalar::UInt64(lag_lead.offset),
                    ))),
                });
            }
            WindowFuncType::LagLead(lag_lead) => {
                return Ok(WindowFuncFrame {
                    units: WindowFuncFrameUnits::Rows,
                    start_bound: WindowFuncFrameBound::Following(Some(Scalar::Number(
                        NumberScalar::UInt64(lag_lead.offset),
                    ))),
                    end_bound: WindowFuncFrameBound::Following(Some(Scalar::Number(
                        NumberScalar::UInt64(lag_lead.offset),
                    ))),
                });
            }
            WindowFuncType::Ntile(_) => {
                return Ok(WindowFuncFrame {
                    units: WindowFuncFrameUnits::Rows,
                    start_bound: WindowFuncFrameBound::Preceding(None),
                    end_bound: WindowFuncFrameBound::Following(None),
                });
            }
            WindowFuncType::CumeDist => {
                return Ok(WindowFuncFrame {
                    units: WindowFuncFrameUnits::Range,
                    start_bound: WindowFuncFrameBound::Preceding(None),
                    end_bound: WindowFuncFrameBound::Following(None),
                });
            }
            _ => {}
        }
        if let Some(frame) = window_frame {
            if frame.units.is_range() {
                if order_by.len() != 1 {
                    return Err(ErrorCode::SemanticError(format!(
                        "The RANGE OFFSET window frame requires exactly one ORDER BY column, {} given.",
                        order_by.len()
                    )).set_span(span));
                }
                self.resolve_window_range_frame(frame)
            } else {
                self.resolve_window_rows_frame(frame)
            }
        } else if order_by.is_empty() {
            Ok(WindowFuncFrame {
                units: WindowFuncFrameUnits::Range,
                start_bound: WindowFuncFrameBound::Preceding(None),
                end_bound: WindowFuncFrameBound::Following(None),
            })
        } else {
            Ok(WindowFuncFrame {
                units: WindowFuncFrameUnits::Range,
                start_bound: WindowFuncFrameBound::Preceding(None),
                end_bound: WindowFuncFrameBound::CurrentRow,
            })
        }
    }

    /// Resolve general window function call.
    fn resolve_general_window_function(
        &mut self,
        span: Span,
        func_name: &str,
        args: &[&Expr],
        window_ignore_null: &Option<bool>,
    ) -> Result<WindowFuncType> {
        if matches!(
            self.bind_context.expr_context,
            ExprContext::InLambdaFunction
        ) {
            return Err(ErrorCode::SemanticError(
                "window functions can not be used in lambda function".to_string(),
            )
            .set_span(span));
        }
        if matches!(
            self.bind_context.expr_context,
            ExprContext::InSetReturningFunction
        ) {
            return Err(ErrorCode::SemanticError(
                "window functions can not be used in set-returning function".to_string(),
            )
            .set_span(span));
        }
        // try to resolve window function without arguments first
        if let Ok(window_func) = WindowFuncType::from_name(func_name) {
            return Ok(window_func);
        }

        if self.in_window_function {
            self.in_window_function = false;
            return Err(ErrorCode::SemanticError(
                "window function calls cannot be nested".to_string(),
            )
            .set_span(span));
        }

        self.in_window_function = true;
        let mut arguments = vec![];
        let mut arg_types = vec![];
        for arg in args.iter() {
            let box (argument, arg_type) = self.resolve(arg)?;
            arguments.push(argument);
            arg_types.push(arg_type);
        }
        self.in_window_function = false;

        // If { IGNORE | RESPECT } NULLS is not specified, the default is RESPECT NULLS
        // (i.e. a NULL value will be returned if the expression contains a NULL value, and it is the first value in the expression).
        let ignore_null = if let Some(ignore_null) = window_ignore_null {
            *ignore_null
        } else {
            false
        };

        match func_name {
            "lag" | "lead" => {
                self.resolve_lag_lead_window_function(func_name, &arguments, &arg_types)
            }
            "first_value" | "first" | "last_value" | "last" | "nth_value" => self
                .resolve_nth_value_window_function(func_name, &arguments, &arg_types, ignore_null),
            "ntile" => self.resolve_ntile_window_function(&arguments),
            _ => Err(ErrorCode::UnknownFunction(format!(
                "Unknown window function: {func_name}"
            ))),
        }
    }

    fn resolve_lag_lead_window_function(
        &mut self,
        func_name: &str,
        args: &[ScalarExpr],
        arg_types: &[DataType],
    ) -> Result<WindowFuncType> {
        if args.is_empty() || args.len() > 3 {
            return Err(ErrorCode::InvalidArgument(format!(
                "Function {:?} only support 1 to 3 arguments",
                func_name
            )));
        }

        let offset = if args.len() >= 2 {
            let off = args[1].as_expr()?;
            match off {
                EExpr::Constant(_) => Some(check_number::<i64, _>(
                    off.span(),
                    &self.func_ctx,
                    &off,
                    &BUILTIN_FUNCTIONS,
                )?),
                _ => {
                    return Err(ErrorCode::InvalidArgument(format!(
                        "The second argument to the function {:?} must be a constant",
                        func_name
                    )));
                }
            }
        } else {
            None
        };

        let offset = offset.unwrap_or(1);

        let is_lag = match func_name {
            "lag" if offset < 0 => false,
            "lead" if offset < 0 => true,
            "lag" => true,
            "lead" => false,
            _ => unreachable!(),
        };

        let (default, return_type) = if args.len() == 3 {
            (Some(args[2].clone()), arg_types[0].clone())
        } else {
            (None, arg_types[0].wrap_nullable())
        };

        let cast_default = default.map(|d| {
            Box::new(ScalarExpr::CastExpr(CastExpr {
                span: d.span(),
                is_try: false,
                argument: Box::new(d),
                target_type: Box::new(return_type.clone()),
            }))
        });

        Ok(WindowFuncType::LagLead(LagLeadFunction {
            is_lag,
            arg: Box::new(args[0].clone()),
            offset: offset.unsigned_abs(),
            default: cast_default,
            return_type: Box::new(return_type),
        }))
    }

    fn resolve_nth_value_window_function(
        &mut self,
        func_name: &str,
        args: &[ScalarExpr],
        arg_types: &[DataType],
        ignore_null: bool,
    ) -> Result<WindowFuncType> {
        Ok(match func_name {
            "first_value" | "first" => {
                if args.len() != 1 {
                    return Err(ErrorCode::InvalidArgument(format!(
                        "The function {:?} must take one argument",
                        func_name
                    )));
                }
                let return_type = arg_types[0].wrap_nullable();
                WindowFuncType::NthValue(NthValueFunction {
                    n: Some(1),
                    arg: Box::new(args[0].clone()),
                    return_type: Box::new(return_type),
                    ignore_null,
                })
            }
            "last_value" | "last" => {
                if args.len() != 1 {
                    return Err(ErrorCode::InvalidArgument(format!(
                        "The function {:?} must take one argument",
                        func_name
                    )));
                }
                let return_type = arg_types[0].wrap_nullable();
                WindowFuncType::NthValue(NthValueFunction {
                    n: None,
                    arg: Box::new(args[0].clone()),
                    return_type: Box::new(return_type),
                    ignore_null,
                })
            }
            _ => {
                // nth_value
                if args.len() != 2 {
                    return Err(ErrorCode::InvalidArgument(
                        "The function nth_value must take two arguments".to_string(),
                    ));
                }
                let return_type = arg_types[0].wrap_nullable();
                let n_expr = args[1].as_expr()?;
                let n = match n_expr {
                    EExpr::Constant(_) => check_number::<u64, _>(
                        n_expr.span(),
                        &self.func_ctx,
                        &n_expr,
                        &BUILTIN_FUNCTIONS,
                    )?,
                    _ => {
                        return Err(ErrorCode::InvalidArgument(
                            "The count of `nth_value` must be constant positive integer",
                        ));
                    }
                };
                if n == 0 {
                    return Err(ErrorCode::InvalidArgument(
                        "nth_value should count from 1".to_string(),
                    ));
                }

                WindowFuncType::NthValue(NthValueFunction {
                    n: Some(n),
                    arg: Box::new(args[0].clone()),
                    return_type: Box::new(return_type),
                    ignore_null,
                })
            }
        })
    }

    fn resolve_ntile_window_function(&mut self, args: &[ScalarExpr]) -> Result<WindowFuncType> {
        if args.len() != 1 {
            return Err(ErrorCode::InvalidArgument(
                "Function ntile can only take one argument".to_string(),
            ));
        }
        let n_expr = args[0].as_expr()?;
        let return_type = DataType::Number(NumberDataType::UInt64);
        let n = match n_expr {
            EExpr::Constant(_) => {
                check_number::<u64, _>(n_expr.span(), &self.func_ctx, &n_expr, &BUILTIN_FUNCTIONS)?
            }
            _ => {
                return Err(ErrorCode::InvalidArgument(
                    "The argument of `ntile` must be constant".to_string(),
                ));
            }
        };
        if n == 0 {
            return Err(ErrorCode::InvalidArgument(
                "ntile buckets must be greater than 0".to_string(),
            ));
        }

        Ok(WindowFuncType::Ntile(NtileFunction {
            n,
            return_type: Box::new(return_type),
        }))
    }

    /// Resolve aggregation function call.
    fn resolve_aggregate_function(
        &mut self,
        span: Span,
        func_name: &str,
        expr: &Expr,
        distinct: bool,
        params: Vec<Scalar>,
        args: &[&Expr],
        order_by: &[OrderByExpr],
    ) -> Result<(AggregateFunction, DataType)> {
        if matches!(
            self.bind_context.expr_context,
            ExprContext::InLambdaFunction
        ) {
            return Err(ErrorCode::SemanticError(
                "aggregate functions can not be used in lambda function".to_string(),
            )
            .set_span(span));
        }

        if self.in_aggregate_function {
            if self.in_window_function {
                // The aggregate function can be in window function call,
                // but it cannot be nested.
                // E.g. `select sum(sum(x)) over (partition by y) from t group by y;` is allowed.
                // But `select sum(sum(sum(x))) from t;` is not allowed.
                self.in_window_function = false;
            } else {
                // Reset the state
                self.in_aggregate_function = false;
                return Err(ErrorCode::SemanticError(
                    "aggregate function calls cannot be nested".to_string(),
                )
                .set_span(expr.span()));
            }
        }

        // Check aggregate function
        self.in_aggregate_function = true;
        let mut arguments = vec![];
        let mut arg_types = vec![];
        for arg in args.iter() {
            let box (argument, arg_type) = self.resolve(arg)?;
            arguments.push(argument);
            arg_types.push(arg_type);
        }
        self.in_aggregate_function = false;

        let sort_descs = order_by
            .iter()
            .map(
                |OrderByExpr {
                     expr,
                     asc,
                     nulls_first,
                 }| {
                    let box (scalar_expr, _) = self.resolve(expr)?;

                    Ok(AggregateFunctionScalarSortDesc {
                        expr: scalar_expr,
                        is_reuse_index: false,
                        nulls_first: nulls_first.unwrap_or(false),
                        asc: asc.unwrap_or(true),
                    })
                },
            )
            .collect::<Result<Vec<_>>>()?;

        // Convert the delimiter of string_agg to params
        let params = if (func_name.eq_ignore_ascii_case("string_agg")
            || func_name.eq_ignore_ascii_case("listagg")
            || func_name.eq_ignore_ascii_case("group_concat"))
            && arguments.len() == 2
            && params.is_empty()
        {
            let delimiter_value = ConstantExpr::try_from(arguments[1].clone());
            if arg_types[1] != DataType::String || delimiter_value.is_err() {
                return Err(ErrorCode::SemanticError(format!(
                    "The delimiter of `{func_name}` must be a constant string"
                )));
            }
            let _ = arguments.pop();
            let _ = arg_types.pop();
            let delimiter = delimiter_value.unwrap();
            vec![delimiter.value]
        } else {
            params
        };

        // Convert the num_buckets of histogram to params
        let params = if func_name.eq_ignore_ascii_case("histogram")
            && arguments.len() == 2
            && params.is_empty()
        {
            let max_num_buckets: u64 = check_number(
                None,
                &FunctionContext::default(),
                &arguments[1].as_expr()?,
                &BUILTIN_FUNCTIONS,
            )?;

            vec![Scalar::Number(NumberScalar::UInt64(max_num_buckets))]
        } else {
            params
        };

        // Rewrite `xxx(distinct)` to `xxx_distinct(...)`
        let (func_name, distinct) = if func_name.eq_ignore_ascii_case("count") && distinct {
            ("count_distinct", false)
        } else {
            (func_name, distinct)
        };

        let func_name = if distinct {
            format!("{}_distinct", func_name)
        } else {
            func_name.to_string()
        };

        let agg_func = AggregateFunctionFactory::instance()
            .get(&func_name, params.clone(), arg_types, vec![])
            .map_err(|e| e.set_span(span))?;

        let args = if optimize_remove_count_args(&func_name, distinct, args) {
            vec![]
        } else {
            arguments
        };

        let display_name = format!("{:#}", expr);
        let new_agg_func = AggregateFunction {
            span,
            display_name,
            func_name,
            distinct: false,
            params,
            args,
            return_type: Box::new(agg_func.return_type()?),
            sort_descs,
        };

        let data_type = agg_func.return_type()?;

        Ok((new_agg_func, data_type))
    }

    fn transform_to_max_type(&self, ty: &DataType) -> Result<DataType> {
        let max_ty = match ty.remove_nullable() {
            DataType::Number(s) => {
                if s.is_float() {
                    DataType::Number(NumberDataType::Float64)
                } else {
                    DataType::Number(NumberDataType::Int64)
                }
            }
            DataType::Decimal(s) if s.can_carried_by_128() => {
                let decimal_size = DecimalSize::new_unchecked(i128::MAX_PRECISION, s.scale());
                DataType::Decimal(decimal_size)
            }
            DataType::Decimal(s) => {
                let decimal_size = DecimalSize::new_unchecked(i256::MAX_PRECISION, s.scale());
                DataType::Decimal(decimal_size)
            }
            DataType::Null => DataType::Null,
            DataType::Binary => DataType::Binary,
            DataType::String => DataType::String,
            DataType::Variant => DataType::Variant,
            _ => {
                return Err(ErrorCode::BadDataValueType(format!(
                    "array_reduce does not support type '{:?}'",
                    ty
                )));
            }
        };

        if ty.is_nullable() {
            Ok(max_ty.wrap_nullable())
        } else {
            Ok(max_ty)
        }
    }

    fn resolve_lambda_function(
        &mut self,
        span: Span,
        func_name: &str,
        args: &[&Expr],
        lambda: &Lambda,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        if matches!(
            self.bind_context.expr_context,
            ExprContext::InLambdaFunction
        ) {
            return Err(ErrorCode::SemanticError(
                "lambda functions can not be used in lambda function".to_string(),
            )
            .set_span(span));
        }

        if args.len() != 1 {
            return Err(ErrorCode::SemanticError(format!(
                "invalid arguments for lambda function, {} expects 1 argument, but got {}",
                func_name,
                args.len()
            ))
            .set_span(span));
        }
        let box (mut arg, mut arg_type) = self.resolve(args[0])?;

        let mut func_name = func_name;
        let mut is_cast_variant = false;
        if arg_type.remove_nullable() == DataType::Variant {
            if func_name.starts_with("json_") {
                func_name = &func_name[5..];
            }
            // Try auto cast the Variant type to Array(Variant) or Map(String, Variant),
            // so that the lambda functions support variant type as argument.
            let mut target_type = if func_name.starts_with("array") {
                DataType::Array(Box::new(DataType::Nullable(Box::new(DataType::Variant))))
            } else {
                DataType::Map(Box::new(DataType::Tuple(vec![
                    DataType::String,
                    DataType::Nullable(Box::new(DataType::Variant)),
                ])))
            };
            if arg_type.is_nullable() {
                target_type = target_type.wrap_nullable();
            }

            arg = ScalarExpr::CastExpr(CastExpr {
                span: None,
                is_try: false,
                argument: Box::new(arg.clone()),
                target_type: Box::new(target_type.clone()),
            });
            arg_type = target_type;

            is_cast_variant = true;
        }

        let params = lambda
            .params
            .iter()
            .map(|param| param.name.to_lowercase())
            .collect::<Vec<_>>();

        self.check_lambda_param_count(func_name, params.len(), span)?;

        let inner_ty = match arg_type.remove_nullable() {
            DataType::Array(box inner_ty) => inner_ty.clone(),
            DataType::Map(box inner_ty) => inner_ty.clone(),
            DataType::Null | DataType::EmptyArray | DataType::EmptyMap => DataType::Null,
            _ => {
                return Err(ErrorCode::SemanticError(
                    "invalid arguments for lambda function, argument data type must be an array or map"
                        .to_string(),
                )
                .set_span(span));
            }
        };

        let inner_tys = if func_name == "array_reduce" {
            let max_ty = self.transform_to_max_type(&inner_ty)?;
            vec![max_ty.clone(), max_ty.clone()]
        } else if func_name == "map_filter"
            || func_name == "map_transform_keys"
            || func_name == "map_transform_values"
        {
            match &inner_ty {
                DataType::Null => {
                    vec![DataType::Null, DataType::Null]
                }
                DataType::Tuple(t) => t.clone(),
                _ => unreachable!(),
            }
        } else {
            vec![inner_ty.clone()]
        };

        let lambda_columns = params
            .iter()
            .zip(inner_tys.iter())
            .map(|(col, ty)| (col.clone(), ty.clone()))
            .collect::<Vec<_>>();

        let mut lambda_context = self.bind_context.clone();
        let box (lambda_expr, lambda_type) = parse_lambda_expr(
            self.ctx.clone(),
            &mut lambda_context,
            &lambda_columns,
            &lambda.expr,
            if LicenseManagerSwitch::instance()
                .check_enterprise_enabled(self.ctx.get_license_key(), Feature::DataMask)
                .is_ok()
            {
                Some(self.metadata.clone())
            } else {
                None
            },
        )?;

        let return_type = if func_name == "array_filter" || func_name == "map_filter" {
            if lambda_type.remove_nullable() == DataType::Boolean {
                arg_type.clone()
            } else {
                return Err(ErrorCode::SemanticError(
                    format!("invalid lambda function for `{}`, the result data type of lambda function must be boolean", func_name)
                )
                .set_span(span));
            }
        } else if func_name == "array_reduce" {
            // transform arg type
            let max_ty = inner_tys[0].clone();
            let target_type = if arg_type.is_nullable() {
                Box::new(DataType::Nullable(Box::new(DataType::Array(Box::new(
                    max_ty.clone(),
                )))))
            } else {
                Box::new(DataType::Array(Box::new(max_ty.clone())))
            };
            // we should convert arg to max_ty to avoid overflow in 'ADD'/'SUB',
            // so if arg_type(origin_type) != target_type(max_type), cast arg
            // for example, if arg = [1INT8, 2INT8, 3INT8], after cast it be [1INT64, 2INT64, 3INT64]
            if arg_type != *target_type {
                arg = ScalarExpr::CastExpr(CastExpr {
                    span: arg.span(),
                    is_try: false,
                    argument: Box::new(arg),
                    target_type,
                });
            }
            max_ty.wrap_nullable()
        } else if func_name == "map_transform_keys" {
            if arg_type.is_nullable() {
                DataType::Nullable(Box::new(DataType::Map(Box::new(DataType::Tuple(vec![
                    lambda_type.clone(),
                    inner_tys[1].clone(),
                ])))))
            } else {
                DataType::Map(Box::new(DataType::Tuple(vec![
                    lambda_type.clone(),
                    inner_tys[1].clone(),
                ])))
            }
        } else if func_name == "map_transform_values" {
            if arg_type.is_nullable() {
                DataType::Nullable(Box::new(DataType::Map(Box::new(DataType::Tuple(vec![
                    inner_tys[0].clone(),
                    lambda_type.clone(),
                ])))))
            } else {
                DataType::Map(Box::new(DataType::Tuple(vec![
                    inner_tys[0].clone(),
                    lambda_type.clone(),
                ])))
            }
        } else if arg_type.is_nullable() {
            DataType::Nullable(Box::new(DataType::Array(Box::new(lambda_type.clone()))))
        } else {
            DataType::Array(Box::new(lambda_type.clone()))
        };

        let (lambda_func, data_type) = match arg_type.remove_nullable() {
            // Null and Empty array can convert to ConstantExpr
            DataType::Null => (
                ConstantExpr {
                    span,
                    value: Scalar::Null,
                }
                .into(),
                DataType::Null,
            ),
            DataType::EmptyArray => (
                ConstantExpr {
                    span,
                    value: Scalar::EmptyArray,
                }
                .into(),
                DataType::EmptyArray,
            ),
            DataType::EmptyMap => (
                ConstantExpr {
                    span,
                    value: Scalar::EmptyMap,
                }
                .into(),
                DataType::EmptyMap,
            ),
            _ => {
                struct LambdaVisitor<'a> {
                    bind_context: &'a BindContext,
                    arg_index: HashSet<IndexType>,
                    args: Vec<ScalarExpr>,
                    fields: Vec<DataField>,
                }

                impl<'a> ScalarVisitor<'a> for LambdaVisitor<'a> {
                    fn visit_bound_column_ref(&mut self, col: &'a BoundColumnRef) -> Result<()> {
                        if self.arg_index.contains(&col.column.index) {
                            return Ok(());
                        }
                        self.arg_index.insert(col.column.index);
                        let is_outer_column = self
                            .bind_context
                            .all_column_bindings()
                            .iter()
                            .map(|c| c.index)
                            .contains(&col.column.index);
                        if is_outer_column {
                            let arg = ScalarExpr::BoundColumnRef(col.clone());
                            self.args.push(arg);
                            let field = DataField::new(
                                &format!("{}", col.column.index),
                                *col.column.data_type.clone(),
                            );
                            self.fields.push(field);
                        }
                        Ok(())
                    }
                }

                // Collect outer scope columns as arguments first.
                let mut lambda_visitor = LambdaVisitor {
                    bind_context: self.bind_context,
                    arg_index: HashSet::new(),
                    args: Vec::new(),
                    fields: Vec::new(),
                };
                lambda_visitor.visit(&lambda_expr)?;

                let mut lambda_args = mem::take(&mut lambda_visitor.args);
                lambda_args.push(arg);
                let mut lambda_fields = mem::take(&mut lambda_visitor.fields);
                // Add lambda columns as arguments at end.
                for (lambda_column_name, lambda_column_type) in lambda_columns.into_iter() {
                    for column in lambda_context.all_column_bindings().iter().rev() {
                        if column.column_name == lambda_column_name {
                            let lambda_field =
                                DataField::new(&format!("{}", column.index), lambda_column_type);
                            lambda_fields.push(lambda_field);
                            break;
                        }
                    }
                }
                let lambda_schema = DataSchema::new(lambda_fields);
                let expr = lambda_expr
                    .type_check(&lambda_schema)?
                    .project_column_ref(|index| lambda_schema.index_of(&index.to_string()))?;
                let (expr, _) = ConstantFolder::fold(&expr, &self.func_ctx, &BUILTIN_FUNCTIONS);
                let remote_lambda_expr = expr.as_remote_expr();
                let lambda_display = format!("{:?} -> {}", params, expr.sql_display());

                (
                    LambdaFunc {
                        span,
                        func_name: func_name.to_string(),
                        args: lambda_args,
                        lambda_expr: Box::new(remote_lambda_expr),
                        lambda_display,
                        return_type: Box::new(return_type.clone()),
                    }
                    .into(),
                    return_type,
                )
            }
        };

        if is_cast_variant {
            let result_target_type = if data_type.is_nullable() {
                DataType::Nullable(Box::new(DataType::Variant))
            } else {
                DataType::Variant
            };
            let result_target_scalar = ScalarExpr::CastExpr(CastExpr {
                span: None,
                is_try: false,
                argument: Box::new(lambda_func),
                target_type: Box::new(result_target_type.clone()),
            });
            Ok(Box::new((result_target_scalar, result_target_type)))
        } else {
            Ok(Box::new((lambda_func, data_type)))
        }
    }

    fn check_lambda_param_count(
        &mut self,
        func_name: &str,
        param_count: usize,
        span: Span,
    ) -> Result<()> {
        // json lambda functions are cast to array or map, ignored here.
        let expected_count = if func_name == "array_reduce" {
            2
        } else if func_name.starts_with("array") {
            1
        } else if func_name.starts_with("map") {
            2
        } else {
            unreachable!()
        };

        if param_count != expected_count {
            return Err(ErrorCode::SemanticError(format!(
                "incorrect number of parameters in lambda function, {} expects {} parameter(s), but got {}",
                func_name, expected_count, param_count
            ))
            .set_span(span));
        }
        Ok(())
    }

    fn resolve_score_search_function(
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
    fn resolve_match_search_function(
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
    fn resolve_query_search_function(
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
        for field in fields.into_iter() {
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
            let option_strs: Vec<&str> = option_text.split(';').collect();
            for option_str in option_strs {
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

    /// Resolve set returning function.
    pub fn resolve_set_returning_function(
        &mut self,
        span: Span,
        func_name: &str,
        args: &[&Expr],
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        match self.bind_context.expr_context {
            ExprContext::InSetReturningFunction => {
                return Err(ErrorCode::SemanticError(
                    "set-returning functions cannot be nested".to_string(),
                )
                .set_span(span));
            }
            ExprContext::WhereClause => {
                return Err(ErrorCode::SemanticError(
                    "set-returning functions are not allowed in WHERE clause".to_string(),
                )
                .set_span(span));
            }
            ExprContext::HavingClause => {
                return Err(ErrorCode::SemanticError(
                    "set-returning functions cannot be used in HAVING clause".to_string(),
                )
                .set_span(span));
            }
            ExprContext::QualifyClause => {
                return Err(ErrorCode::SemanticError(
                    "set-returning functions cannot be used in QUALIFY clause".to_string(),
                )
                .set_span(span));
            }
            _ => {}
        }

        if self.in_window_function {
            return Err(ErrorCode::SemanticError(
                "set-returning functions cannot be used in window spec",
            )
            .set_span(span));
        }

        let original_context = self.bind_context.expr_context.clone();
        self.bind_context
            .set_expr_context(ExprContext::InSetReturningFunction);

        let mut arguments = Vec::with_capacity(args.len());
        for arg in args.iter() {
            let box (scalar, _) = self.resolve(arg)?;
            arguments.push(scalar);
        }

        // Restore the original context
        self.bind_context.set_expr_context(original_context);

        let srf_scalar = ScalarExpr::FunctionCall(FunctionCall {
            span,
            func_name: func_name.to_string(),
            params: vec![],
            arguments,
        });
        let srf_expr = srf_scalar.as_expr()?;
        let srf_tuple_types = srf_expr.data_type().as_tuple().ok_or_else(|| {
            ErrorCode::Internal(format!(
                "The return type of srf should be tuple, but got {}",
                srf_expr.data_type()
            ))
        })?;

        // If tuple has more than one field, return the tuple column,
        // otherwise, extract the tuple field to top level column.
        let (return_scalar, return_type) = if srf_tuple_types.len() > 1 {
            (srf_scalar, srf_expr.data_type().clone())
        } else {
            let child_scalar = ScalarExpr::FunctionCall(FunctionCall {
                span,
                func_name: "get".to_string(),
                params: vec![Scalar::Number(NumberScalar::Int64(1))],
                arguments: vec![srf_scalar],
            });
            (child_scalar, srf_tuple_types[0].clone())
        };

        Ok(Box::new((return_scalar, return_type)))
    }

    /// Resolve function call.
    pub fn resolve_function(
        &mut self,
        span: Span,
        func_name: &str,
        params: Vec<Scalar>,
        arguments: &[&Expr],
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        // Check if current function is a virtual function, e.g. `database`, `version`
        if let Some(rewritten_func_result) = databend_common_base::runtime::block_on(
            self.try_rewrite_sugar_function(span, func_name, arguments),
        ) {
            return rewritten_func_result;
        }

        let mut args = vec![];
        let mut arg_types = vec![];

        for argument in arguments {
            let box (arg, mut arg_type) = self.resolve(argument)?;
            if let ScalarExpr::SubqueryExpr(subquery) = &arg {
                if subquery.typ == SubqueryType::Scalar && !arg.data_type()?.is_nullable() {
                    arg_type = arg_type.wrap_nullable();
                }
            }
            args.push(arg);
            arg_types.push(arg_type);
        }

        if let Some(rewritten_variant_expr) =
            self.try_rewrite_variant_function(span, func_name, &args, &arg_types)
        {
            return rewritten_variant_expr;
        }
        if let Some(rewritten_vector_expr) =
            self.try_rewrite_vector_function(span, func_name, &args)
        {
            return rewritten_vector_expr;
        }

        self.resolve_scalar_function_call(span, func_name, params, args)
    }

    pub fn resolve_scalar_function_call(
        &self,
        span: Span,
        func_name: &str,
        mut params: Vec<Scalar>,
        mut args: Vec<ScalarExpr>,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        // rewrite substr('xx', 0, xx) -> substr('xx', 1, xx)
        if (func_name == "substr" || func_name == "substring")
            && self
                .ctx
                .get_settings()
                .get_sql_dialect()
                .unwrap()
                .substr_index_zero_literal_as_one()
        {
            Self::rewrite_substring(&mut args);
        }

        self.adjust_date_interval_function_args(func_name, &mut args)?;

        // Type check
        let mut arguments = args.iter().map(|v| v.as_raw_expr()).collect::<Vec<_>>();
        // inject the params
        if ["round", "truncate"].contains(&func_name)
            && !args.is_empty()
            && params.is_empty()
            && args[0].data_type()?.remove_nullable().is_decimal()
        {
            let scale = if args.len() == 2 {
                let scalar_expr = &arguments[1];
                let expr = type_check::check(scalar_expr, &BUILTIN_FUNCTIONS)?;

                let scale: i64 = check_number(
                    expr.span(),
                    &FunctionContext::default(),
                    &expr,
                    &BUILTIN_FUNCTIONS,
                )?;
                scale.clamp(-76, 76)
            } else {
                0
            };
            params.push(Scalar::Number(NumberScalar::Int64(scale)));
        } else if func_name.eq_ignore_ascii_case("as_decimal") {
            // Convert the precision and scale argument of `as_decimal` to params
            if !params.is_empty() {
                if params.len() > 2 || arguments.len() != 1 {
                    return Err(ErrorCode::SemanticError(format!(
                        "Invalid arguments for `{func_name}`, get {} params and {} arguments",
                        params.len(),
                        arguments.len()
                    )));
                }
            } else {
                if arguments.is_empty() || arguments.len() > 3 {
                    return Err(ErrorCode::SemanticError(format!(
                        "Invalid arguments for `{func_name}` require 1, 2 or 3 arguments, but got {} arguments",
                        arguments.len()
                    )));
                }
                let param_args = arguments.split_off(1);
                for arg in param_args.into_iter() {
                    let expr = type_check::check(&arg, &BUILTIN_FUNCTIONS)?;
                    let param: u8 = check_number(
                        expr.span(),
                        &FunctionContext::default(),
                        &expr,
                        &BUILTIN_FUNCTIONS,
                    )?;
                    params.push(Scalar::Number(NumberScalar::UInt8(param)));
                }
            }
            if !params.is_empty() {
                let Some(precision) = params[0].get_i64() else {
                    return Err(ErrorCode::SemanticError(format!(
                        "Invalid value `{}` for `{func_name}` precision parameter",
                        params[0]
                    )));
                };
                if precision < 0 || precision > i256::MAX_PRECISION as i64 {
                    return Err(ErrorCode::SemanticError(format!(
                        "Invalid value `{precision}` for `{func_name}` precision parameter"
                    )));
                }
                if params.len() == 2 {
                    let Some(scale) = params[1].get_i64() else {
                        return Err(ErrorCode::SemanticError(format!(
                            "Invalid value `{}` for `{func_name}` scale parameter",
                            params[1]
                        )));
                    };
                    if scale < 0 || scale > precision {
                        return Err(ErrorCode::SemanticError(format!(
                            "Invalid value `{scale}` for `{func_name}` scale parameter"
                        )));
                    }
                }
            }
        } else if (func_name.eq_ignore_ascii_case("to_number")
            || func_name.eq_ignore_ascii_case("to_numeric")
            || func_name.eq_ignore_ascii_case("to_decimal")
            || func_name.eq_ignore_ascii_case("try_to_number")
            || func_name.eq_ignore_ascii_case("try_to_numeric")
            || func_name.eq_ignore_ascii_case("try_to_decimal"))
            && params.is_empty()
        {
            if args.is_empty() || args.len() > 4 {
                return Err(ErrorCode::SemanticError(format!(
                    "Invalid arguments for `{func_name}`, get {} params and {} arguments",
                    params.len(),
                    arguments.len()
                )));
            }
            let func_ctx = self.ctx.get_function_context()?;
            let arg_fn = |args: &[ScalarExpr],
                          index: usize,
                          arg_name: &str,
                          default: i64|
             -> Result<i64> {
                Ok(args.get(index).map(|arg| {
                    match ConstantFolder::fold(&arg.as_expr()?, &func_ctx, &BUILTIN_FUNCTIONS).0 {
                        databend_common_expression::Expr::Constant(Constant {
                                                                       scalar,
                                                                       ..
                                                                   }) => Ok(scalar.get_i64()),
                        _ => Err(ErrorCode::SemanticError(format!("Invalid arguments for `{func_name}`, {arg_name} is only allowed to be a constant"))),
                    }
                }).transpose()?.flatten().unwrap_or(default))
            };

            let (precision_index, scale_index) =
                if args.len() > 1 && args[1].data_type()?.remove_nullable().is_string() {
                    (2, 3)
                } else {
                    (1, 2)
                };
            let precision = arg_fn(
                &args,
                precision_index,
                "precision",
                DEFAULT_DECIMAL_PRECISION,
            )?;
            let scale = arg_fn(&args, scale_index, "scale", DEFAULT_DECIMAL_SCALE)?;

            if let Err(err) = DecimalSize::new(precision as u8, scale as u8) {
                return Err(ErrorCode::SemanticError(format!(
                    "Invalid arguments for `{func_name}`, {}",
                    err,
                )));
            }

            params.push(Scalar::Number(NumberScalar::Int64(precision as _)));
            params.push(Scalar::Number(NumberScalar::Int64(scale as _)));
        }

        let raw_expr = RawExpr::FunctionCall {
            span,
            name: func_name.to_string(),
            params: params.clone(),
            args: arguments,
        };

        let expr = type_check::check(&raw_expr, &BUILTIN_FUNCTIONS)?;
        let expr = type_check::rewrite_function_to_cast(expr);

        // Run constant folding for arguments of the scalar function.
        // This will be helpful to simplify some constant expressions, especially
        // the implicitly casted literal values, e.g. `timestamp > '2001-01-01'`
        // will be folded from `timestamp > to_timestamp('2001-01-01')` to `timestamp > 978307200000000`
        // Note: check function may reorder the args

        let mut folded_args = match &expr {
            expr::Expr::FunctionCall(expr::FunctionCall {
                function,
                args: checked_args,
                ..
            }) => checked_args
                .iter()
                .zip(
                    function
                        .signature
                        .args_type
                        .iter()
                        .map(DataType::is_generic),
                )
                .map(|(checked_arg, is_generic)| self.try_fold_constant(checked_arg, !is_generic))
                .zip(args)
                .map(|(folded, arg)| match folded {
                    Some(box (constant, _)) if arg.evaluable() => constant,
                    _ => arg,
                })
                .collect(),
            _ => args,
        };

        if !expr.is_deterministic(&BUILTIN_FUNCTIONS) {
            self.ctx.set_cacheable(false);
        }

        if let Some(constant) = self.try_fold_constant(&expr, true) {
            return Ok(constant);
        }

        if let expr::Expr::Cast(expr::Cast {
            span,
            is_try,
            dest_type,
            ..
        }) = expr
        {
            assert_eq!(folded_args.len(), 1);
            return Ok(Box::new((
                CastExpr {
                    span,
                    is_try,
                    argument: Box::new(folded_args.pop().unwrap()),
                    target_type: Box::new(dest_type.clone()),
                }
                .into(),
                dest_type,
            )));
        }

        // reorder
        if func_name == "eq"
            && folded_args.len() == 2
            && matches!(folded_args[0], ScalarExpr::ConstantExpr(_))
            && !matches!(folded_args[1], ScalarExpr::ConstantExpr(_))
        {
            folded_args.swap(0, 1);
        }

        Ok(Box::new((
            FunctionCall {
                span,
                params,
                arguments: folded_args,
                func_name: func_name.to_string(),
            }
            .into(),
            expr.data_type().clone(),
        )))
    }

    /// Resolve binary expressions. Most of the binary expressions
    /// would be transformed into `FunctionCall`, except comparison
    /// expressions, conjunction(`AND`) and disjunction(`OR`).
    pub fn resolve_binary_op(
        &mut self,
        span: Span,
        op: &BinaryOperator,
        left: &Expr,
        right: &Expr,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        match op {
            BinaryOperator::NotLike(_) | BinaryOperator::NotRegexp | BinaryOperator::NotRLike => {
                let positive_op = match op {
                    BinaryOperator::NotLike(escape) => BinaryOperator::Like(escape.clone()),
                    BinaryOperator::NotRegexp => BinaryOperator::Regexp,
                    BinaryOperator::NotRLike => BinaryOperator::RLike,
                    _ => unreachable!(),
                };
                let (positive, _) = *self.resolve_binary_op(span, &positive_op, left, right)?;
                self.resolve_scalar_function_call(span, "not", vec![], vec![positive])
            }
            BinaryOperator::SoundsLike => {
                // rewrite "expr1 SOUNDS LIKE expr2" to "SOUNDEX(expr1) = SOUNDEX(expr2)"
                let box (left, _) = self.resolve(left)?;
                let box (right, _) = self.resolve(right)?;

                let (left, _) =
                    *self.resolve_scalar_function_call(span, "soundex", vec![], vec![left])?;
                let (right, _) =
                    *self.resolve_scalar_function_call(span, "soundex", vec![], vec![right])?;

                self.resolve_scalar_function_call(
                    span,
                    &BinaryOperator::Eq.to_func_name(),
                    vec![],
                    vec![left, right],
                )
            }
            BinaryOperator::Like(escape) => {
                // Convert `Like` to compare function , such as `p_type like PROMO%` will be converted to `p_type >= PROMO and p_type < PROMP`
                if let Expr::Literal {
                    value: Literal::String(str),
                    ..
                } = right
                {
                    return self.resolve_like(op, span, left, right, str, escape);
                }
                self.resolve_like_escape(op, span, left, right, escape)
            }
            BinaryOperator::LikeAny(escape) => {
                self.resolve_like_escape(op, span, left, right, escape)
            }
            BinaryOperator::Eq | BinaryOperator::NotEq => {
                let name = op.to_func_name();
                let box (res, ty) =
                    self.resolve_function(span, name.as_str(), vec![], &[left, right])?;
                // When a variant type column is compared with a scalar string value,
                // we try to cast the scalar string value to variant type,
                // because casting variant column data is a time-consuming operation.
                if let ScalarExpr::FunctionCall(ref func) = res {
                    if func.arguments.len() != 2 {
                        return Ok(Box::new((res, ty)));
                    }
                    let arg0 = &func.arguments[0];
                    let arg1 = &func.arguments[1];
                    let (constant_arg_index, constant_arg) = match (arg0, arg1) {
                        (ScalarExpr::ConstantExpr(_), _)
                            if arg1.data_type()?.remove_nullable() == DataType::Variant
                                && !arg1.used_columns().is_empty()
                                && arg0.data_type()? == DataType::String =>
                        {
                            (0, arg0)
                        }
                        (_, ScalarExpr::ConstantExpr(_))
                            if arg0.data_type()?.remove_nullable() == DataType::Variant
                                && !arg0.used_columns().is_empty()
                                && arg1.data_type()? == DataType::String =>
                        {
                            (1, arg1)
                        }
                        _ => {
                            return Ok(Box::new((res, ty)));
                        }
                    };

                    let wrap_new_arg = ScalarExpr::FunctionCall(FunctionCall {
                        span: func.span,
                        func_name: "to_variant".to_string(),
                        params: vec![],
                        arguments: vec![constant_arg.clone()],
                    });
                    let mut new_arguments = func.arguments.clone();
                    new_arguments[constant_arg_index] = wrap_new_arg;

                    let new_func = ScalarExpr::FunctionCall(FunctionCall {
                        span: func.span,
                        func_name: func.func_name.clone(),
                        params: func.params.clone(),
                        arguments: new_arguments,
                    });

                    return Ok(Box::new((new_func, ty)));
                }
                Ok(Box::new((res, ty)))
            }
            BinaryOperator::Plus | BinaryOperator::Minus => {
                let name = op.to_func_name();
                let (mut left_expr, left_type) = *self.resolve(left)?;
                let (mut right_expr, right_type) = *self.resolve(right)?;
                self.adjust_date_interval_operands(
                    op,
                    &mut left_expr,
                    &left_type,
                    &mut right_expr,
                    &right_type,
                )?;
                self.resolve_scalar_function_call(span, name.as_str(), vec![], vec![
                    left_expr, right_expr,
                ])
            }
            other => {
                let name = other.to_func_name();
                self.resolve_function(span, name.as_str(), vec![], &[left, right])
            }
        }
    }

    fn adjust_date_interval_operands(
        &self,
        op: &BinaryOperator,
        left_expr: &mut ScalarExpr,
        left_type: &DataType,
        right_expr: &mut ScalarExpr,
        right_type: &DataType,
    ) -> Result<()> {
        match op {
            BinaryOperator::Plus => {
                self.adjust_single_date_interval_operand(
                    left_expr, left_type, right_expr, right_type,
                )?;
                self.adjust_single_date_interval_operand(
                    right_expr, right_type, left_expr, left_type,
                )?;
            }
            BinaryOperator::Minus => {
                self.adjust_single_date_interval_operand(
                    left_expr, left_type, right_expr, right_type,
                )?;
            }
            _ => {}
        }
        Ok(())
    }

    fn adjust_date_interval_function_args(
        &self,
        func_name: &str,
        args: &mut [ScalarExpr],
    ) -> Result<()> {
        if args.len() != 2 {
            return Ok(());
        }
        let op = if func_name.eq_ignore_ascii_case("plus") {
            BinaryOperator::Plus
        } else if func_name.eq_ignore_ascii_case("minus") {
            BinaryOperator::Minus
        } else {
            return Ok(());
        };
        let (left_slice, right_slice) = args.split_at_mut(1);
        let left_expr = &mut left_slice[0];
        let right_expr = &mut right_slice[0];
        let left_type = left_expr.data_type()?;
        let right_type = right_expr.data_type()?;
        self.adjust_date_interval_operands(&op, left_expr, &left_type, right_expr, &right_type)
    }

    fn adjust_single_date_interval_operand(
        &self,
        date_expr: &mut ScalarExpr,
        date_type: &DataType,
        interval_expr: &ScalarExpr,
        interval_type: &DataType,
    ) -> Result<()> {
        if date_type.remove_nullable() != DataType::Date
            || interval_type.remove_nullable() != DataType::Interval
        {
            return Ok(());
        }

        if self.interval_contains_only_date_parts(interval_expr)? {
            return Ok(());
        }

        // Preserve nullability when casting DATE to TIMESTAMP
        let target_type = if date_type.is_nullable_or_null() {
            DataType::Timestamp.wrap_nullable()
        } else {
            DataType::Timestamp
        };
        *date_expr = wrap_cast(date_expr, &target_type);
        Ok(())
    }

    fn interval_contains_only_date_parts(&self, interval_expr: &ScalarExpr) -> Result<bool> {
        let expr = interval_expr.as_expr()?;
        let (folded, _) = ConstantFolder::fold(&expr, &self.func_ctx, &BUILTIN_FUNCTIONS);
        if let EExpr::Constant(Constant {
            scalar: Scalar::Interval(value),
            ..
        }) = folded
        {
            return Ok(value.microseconds() == 0);
        }
        Ok(false)
    }

    /// Resolve unary expressions.
    pub fn resolve_unary_op(
        &mut self,
        span: Span,
        op: &UnaryOperator,
        child: &Expr,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        match op {
            UnaryOperator::Plus => {
                // Omit unary + operator
                self.resolve(child)
            }
            UnaryOperator::Minus => {
                if let Expr::Literal { value, .. } = child {
                    let box (value, data_type) = self.resolve_minus_literal_scalar(span, value)?;
                    let scalar_expr = ScalarExpr::ConstantExpr(ConstantExpr { span, value });
                    return Ok(Box::new((scalar_expr, data_type)));
                }
                let name = op.to_func_name();
                self.resolve_function(span, name.as_str(), vec![], &[child])
            }
            other => {
                let name = other.to_func_name();
                self.resolve_function(span, name.as_str(), vec![], &[child])
            }
        }
    }

    pub fn resolve_extract_expr(
        &mut self,
        span: Span,
        interval_kind: &ASTIntervalKind,
        arg: &Expr,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        match interval_kind {
            ASTIntervalKind::ISOYear => self.resolve_function(span, "to_iso_year", vec![], &[arg]),
            ASTIntervalKind::Year => self.resolve_function(span, "to_year", vec![], &[arg]),
            ASTIntervalKind::Quarter => self.resolve_function(span, "to_quarter", vec![], &[arg]),
            ASTIntervalKind::Month => self.resolve_function(span, "to_month", vec![], &[arg]),
            ASTIntervalKind::Day => self.resolve_function(span, "to_day_of_month", vec![], &[arg]),
            ASTIntervalKind::Hour => self.resolve_function(span, "to_hour", vec![], &[arg]),
            ASTIntervalKind::Minute => self.resolve_function(span, "to_minute", vec![], &[arg]),
            ASTIntervalKind::Second => self.resolve_function(span, "to_second", vec![], &[arg]),
            ASTIntervalKind::Doy => self.resolve_function(span, "to_day_of_year", vec![], &[arg]),
            // Day of the week (Sunday = 0, Saturday = 6)
            ASTIntervalKind::Dow => self.resolve_function(span, "dayofweek", vec![], &[arg]),
            ASTIntervalKind::Week => self.resolve_function(span, "to_week_of_year", vec![], &[arg]),
            ASTIntervalKind::Epoch => self.resolve_function(span, "epoch", vec![], &[arg]),
            ASTIntervalKind::MicroSecond => {
                self.resolve_function(span, "to_microsecond", vec![], &[arg])
            }
            // ISO day of the week (Monday = 1, Sunday = 7)
            ASTIntervalKind::ISODow => {
                self.resolve_function(span, "to_day_of_week", vec![], &[arg])
            }
            ASTIntervalKind::YearWeek => self.resolve_function(span, "yearweek", vec![], &[arg]),
            ASTIntervalKind::Millennium => {
                self.resolve_function(span, "millennium", vec![], &[arg])
            }
            _ => Err(ErrorCode::SemanticError(
                "Only support interval type [ISOYear, Year, Quarter, Month, Day, Hour, Minute, Second, Doy, Dow, Week, Epoch, MicroSecond, ISODow, YearWeek, Millennium]".to_string(),
            )
            .set_span(span)),
        }
    }

    pub fn resolve_date_arith(
        &mut self,
        span: Span,
        interval_kind: &ASTIntervalKind,
        date_rhs: &Expr,
        date_lhs: &Expr,
        is_diff: &Expr,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let func_name = match is_diff {
            Expr::DateDiff { .. } => format!("diff_{}s", interval_kind.to_string().to_lowercase()),
            Expr::DateSub { .. } | Expr::DateAdd { .. } => {
                let interval_kind = interval_kind.to_string().to_lowercase();
                if interval_kind == "month" {
                    format!("date_add_{}s", interval_kind.to_string().to_lowercase())
                } else {
                    format!("add_{}s", interval_kind.to_string().to_lowercase())
                }
            }
            Expr::DateBetween { .. } => {
                format!("between_{}s", interval_kind.to_string().to_lowercase())
            }
            _ => {
                return Err(ErrorCode::Internal(
                    "Only support resolve datesub, date_sub, date_diff, date_add",
                ));
            }
        };
        let mut args = vec![];
        let mut arg_types = vec![];

        let (date_lhs, date_lhs_type) = *self.resolve(date_lhs)?;
        args.push(date_lhs);
        arg_types.push(date_lhs_type);

        let (date_rhs, date_rhs_type) = *self.resolve(date_rhs)?;

        args.push(date_rhs);
        arg_types.push(date_rhs_type);

        self.resolve_scalar_function_call(span, &func_name, vec![], args)
    }

    pub fn resolve_date_trunc(
        &mut self,
        span: Span,
        date: &Expr,
        kind: &ASTIntervalKind,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        match kind {
            ASTIntervalKind::Year => {
                self.resolve_function(
                    span,
                    "to_start_of_year", vec![],
                    &[date],
                )
            }
            ASTIntervalKind::ISOYear => {
                self.resolve_function(
                    span,
                    "to_start_of_iso_year", vec![],
                    &[date],
                )
            }
            ASTIntervalKind::Quarter => {
                self.resolve_function(
                    span,
                    "to_start_of_quarter", vec![],
                    &[date],
                )
            }
            ASTIntervalKind::Month => {
                self.resolve_function(
                    span,
                    "to_start_of_month", vec![],
                    &[date],
                )
            }
            ASTIntervalKind::Week => {
                let week_start = self.func_ctx.week_start;
                self.resolve_function(
                    span,
                    "to_start_of_week", vec![],
                    &[date, &Expr::Literal {
                        span: None,
                        value: Literal::UInt64(week_start as u64)
                    }],
                )
            }
            ASTIntervalKind::ISOWeek => {
                self.resolve_function(
                    span,
                    "to_start_of_iso_week", vec![],
                    &[date],
                )
            }
            ASTIntervalKind::Day => {
                self.resolve_function(
                    span,
                    "to_start_of_day", vec![],
                    &[date],
                )
            }
            ASTIntervalKind::Hour => {
                self.resolve_function(
                    span,
                    "to_start_of_hour", vec![],
                    &[date],
                )
            }
            ASTIntervalKind::Minute => {
                self.resolve_function(
                    span,
                    "to_start_of_minute", vec![],
                    &[date],
                )
            }
            ASTIntervalKind::Second => {
                self.resolve_function(
                    span,
                    "to_start_of_second", vec![],
                    &[date],
                )
            }
            _ => Err(ErrorCode::SemanticError("Only these interval types are currently supported: [year, quarter, month, day, hour, minute, second, week]".to_string()).set_span(span)),
        }
    }

    pub fn resolve_time_slice(
        &mut self,
        span: Span,
        date: &Expr,
        slice_length: u64,
        kind: &ASTIntervalKind,
        start_or_end: String,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        if slice_length < 1 {
            return Err(ErrorCode::BadArguments(
                "slice_length must be greater than or equal to 1",
            ));
        }
        let slice_length = &Expr::Literal {
            span: None,
            value: Literal::UInt64(slice_length),
        };
        let start_or_end = if start_or_end.eq_ignore_ascii_case("start")
            || start_or_end.eq_ignore_ascii_case("end")
        {
            &Expr::Literal {
                span: None,
                value: Literal::String(start_or_end),
            }
        } else {
            return Err(ErrorCode::BadArguments(
                "time_slice only support start or end",
            ));
        };

        let kind = match kind {
            ASTIntervalKind::Year |
            ASTIntervalKind::Quarter |
            ASTIntervalKind::Month|
            ASTIntervalKind::Week| ASTIntervalKind::ISOWeek | ASTIntervalKind::Day | ASTIntervalKind::Hour | ASTIntervalKind::Minute | ASTIntervalKind::Second => {
                    &Expr::Literal {
                    span: None,
                    value: Literal::String(kind.to_string())
                }
            }
            _ => return Err(ErrorCode::SemanticError("Only these interval types are currently supported: [year, quarter, month, day, hour, minute, second, week]".to_string()).set_span(span)),
        };
        self.resolve_function(span, "time_slice", vec![], &[
            date,
            slice_length,
            start_or_end,
            kind,
        ])
    }

    pub fn resolve_last_day(
        &mut self,
        span: Span,
        date: &Expr,
        kind: &ASTIntervalKind,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        match kind {
            ASTIntervalKind::Year => {
                self.resolve_function(span, "to_last_of_year", vec![], &[date])
            }
            ASTIntervalKind::Quarter => {
                self.resolve_function(span, "to_last_of_quarter", vec![], &[date])
            }
            ASTIntervalKind::Month => {
                self.resolve_function(span, "to_last_of_month", vec![], &[date])
            }
            ASTIntervalKind::Week => {
                self.resolve_function(span, "to_last_of_week", vec![], &[date])
            }
            _ => Err(ErrorCode::SemanticError(
                "Only these interval types are currently supported: [year, quarter, month, week]"
                    .to_string(),
            )
            .set_span(span)),
        }
    }

    pub fn resolve_previous_or_next_day(
        &mut self,
        span: Span,
        date: &Expr,
        weekday: &ASTWeekday,
        is_previous: bool,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let prefix = if is_previous {
            "to_previous_"
        } else {
            "to_next_"
        };

        let func_name = match weekday {
            ASTWeekday::Monday => format!("{}monday", prefix),
            ASTWeekday::Tuesday => format!("{}tuesday", prefix),
            ASTWeekday::Wednesday => format!("{}wednesday", prefix),
            ASTWeekday::Thursday => format!("{}thursday", prefix),
            ASTWeekday::Friday => format!("{}friday", prefix),
            ASTWeekday::Saturday => format!("{}saturday", prefix),
            ASTWeekday::Sunday => format!("{}sunday", prefix),
        };

        self.resolve_function(span, &func_name, vec![], &[date])
    }

    pub fn resolve_subquery(
        &mut self,
        typ: SubqueryType,
        subquery: &Query,
        child_expr: Option<Expr>,
        compare_op: Option<SubqueryComparisonOp>,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let mut binder = Binder::new(
            self.ctx.clone(),
            CatalogManager::instance(),
            self.name_resolution_ctx.clone(),
            self.metadata.clone(),
        );

        // Create new `BindContext` with current `bind_context` as its parent, so we can resolve outer columns.
        let mut bind_context = BindContext::with_parent(self.bind_context.clone())?;
        let (s_expr, output_context) = binder.bind_query(&mut bind_context, subquery)?;
        self.bind_context
            .cte_context
            .set_cte_context_and_name(output_context.cte_context);

        if (typ == SubqueryType::Scalar || typ == SubqueryType::Any)
            && output_context.columns.len() > 1
        {
            return Err(ErrorCode::SemanticError(format!(
                "Subquery must return only one column, but got {} columns",
                output_context.columns.len()
            )));
        }

        let mut contain_agg = None;
        if let SetExpr::Select(select_stmt) = &subquery.body {
            if typ == SubqueryType::Scalar {
                let select = &select_stmt.select_list[0];
                if matches!(select, SelectTarget::AliasedExpr { .. }) {
                    // Check if contain aggregation function
                    #[derive(Visitor)]
                    #[visitor(Expr(enter), ASTFunctionCall(enter))]
                    struct AggFuncVisitor {
                        contain_agg: bool,
                    }
                    impl AggFuncVisitor {
                        fn enter_ast_function_call(&mut self, func: &ASTFunctionCall) {
                            self.contain_agg = self.contain_agg
                                || AggregateFunctionFactory::instance()
                                    .contains(func.name.to_string());
                        }
                        fn enter_expr(&mut self, expr: &Expr) {
                            self.contain_agg = self.contain_agg
                                || matches!(expr, Expr::CountAll { window: None, .. });
                        }
                    }
                    let mut visitor = AggFuncVisitor { contain_agg: false };
                    select.drive(&mut visitor);
                    contain_agg = Some(visitor.contain_agg);
                }
            }
        }

        let box mut data_type = output_context.columns[0].data_type.clone();

        let rel_expr = RelExpr::with_s_expr(&s_expr);
        let rel_prop = rel_expr.derive_relational_prop()?;

        let mut child_scalar = None;
        if let Some(expr) = child_expr {
            assert_eq!(output_context.columns.len(), 1);
            let box (scalar, expr_ty) = self.resolve(&expr)?;
            child_scalar = Some(Box::new(scalar));
            // wrap nullable to make sure expr and list values have common type.
            if expr_ty.is_nullable() {
                data_type = data_type.wrap_nullable();
            }
        }

        if typ.eq(&SubqueryType::Scalar) {
            data_type = data_type.wrap_nullable();
        }
        let subquery_expr = SubqueryExpr {
            span: subquery.span,
            subquery: Box::new(s_expr),
            child_expr: child_scalar,
            compare_op,
            output_column: output_context.columns[0].clone(),
            projection_index: None,
            data_type: Box::new(data_type),
            typ,
            outer_columns: rel_prop.outer_columns.clone(),
            contain_agg,
        };
        let data_type = subquery_expr.output_data_type();
        Ok(Box::new((subquery_expr.into(), data_type)))
    }

    pub fn all_sugar_functions() -> &'static [Ascii<&'static str>] {
        static FUNCTIONS: &[Ascii<&'static str>] = &[
            Ascii::new("current_catalog"),
            Ascii::new("database"),
            Ascii::new("currentdatabase"),
            Ascii::new("current_database"),
            Ascii::new("version"),
            Ascii::new("user"),
            Ascii::new("currentuser"),
            Ascii::new("current_user"),
            Ascii::new("current_role"),
            Ascii::new("current_secondary_roles"),
            Ascii::new("current_available_roles"),
            Ascii::new("connection_id"),
            Ascii::new("client_session_id"),
            Ascii::new("timezone"),
            Ascii::new("nullif"),
            Ascii::new("iff"),
            Ascii::new("ifnull"),
            Ascii::new("nvl"),
            Ascii::new("nvl2"),
            Ascii::new("is_null"),
            Ascii::new("isnull"),
            Ascii::new("is_error"),
            Ascii::new("error_or"),
            Ascii::new("coalesce"),
            Ascii::new("decode"),
            Ascii::new("last_query_id"),
            Ascii::new("array_sort"),
            Ascii::new("array_aggregate"),
            Ascii::new("to_variant"),
            Ascii::new("try_to_variant"),
            Ascii::new("greatest"),
            Ascii::new("least"),
            Ascii::new("greatest_ignore_nulls"),
            Ascii::new("least_ignore_nulls"),
            Ascii::new("stream_has_data"),
            Ascii::new("getvariable"),
            Ascii::new("equal_null"),
            Ascii::new("hex_decode_string"),
            Ascii::new("base64_decode_string"),
            Ascii::new("try_hex_decode_string"),
            Ascii::new("try_base64_decode_string"),
        ];
        FUNCTIONS
    }

    async fn try_rewrite_sugar_function(
        &mut self,
        span: Span,
        func_name: &str,
        args: &[&Expr],
    ) -> Option<Result<Box<(ScalarExpr, DataType)>>> {
        match (func_name.to_lowercase().as_str(), args) {
            ("current_catalog", &[]) => Some(self.resolve(&Expr::Literal {
                span,
                value: Literal::String(self.ctx.get_current_catalog()),
            })),
            ("database" | "currentdatabase" | "current_database", &[]) => {
                Some(self.resolve(&Expr::Literal {
                    span,
                    value: Literal::String(self.ctx.get_current_database()),
                }))
            }
            ("version", &[]) => Some(self.resolve(&Expr::Literal {
                span,
                value: Literal::String(self.ctx.get_fuse_version()),
            })),
            ("user" | "currentuser" | "current_user", &[]) => match self.ctx.get_current_user() {
                Ok(user) => Some(self.resolve(&Expr::Literal {
                    span,
                    value: Literal::String(user.identity().display().to_string()),
                })),
                Err(e) => Some(Err(e)),
            },
            ("current_role", &[]) => Some(
                self.resolve(&Expr::Literal {
                    span,
                    value: Literal::String(
                        self.ctx
                            .get_current_role()
                            .map(|r| r.name)
                            .unwrap_or_default(),
                    ),
                }),
            ),
            ("current_secondary_roles", &[]) => {
                let mut res = self
                    .ctx
                    .get_all_effective_roles()
                    .await
                    .unwrap_or_default()
                    .iter()
                    .map(|r| r.name.clone())
                    .collect::<Vec<String>>();
                res.sort();
                let roles_comma_separated_string = res.iter().join(",");
                let res = if self.ctx.get_secondary_roles().is_none() {
                    json!({
                        "roles": roles_comma_separated_string,
                        "value": "ALL"
                    })
                } else {
                    json!({
                        "roles": roles_comma_separated_string,
                        "value": "None"
                    })
                };
                match to_string(&res) {
                    Ok(res) => Some(self.resolve(&Expr::Literal {
                        span,
                        value: Literal::String(res),
                    })),
                    Err(e) => Some(Err(ErrorCode::IllegalRole(format!(
                        "Failed to serialize secondary roles into JSON string: {}",
                        e
                    )))),
                }
            }
            ("current_available_roles", &[]) => {
                let mut res = self
                    .ctx
                    .get_all_available_roles()
                    .await
                    .unwrap_or_default()
                    .iter()
                    .map(|r| r.name.clone())
                    .collect::<Vec<String>>();
                res.sort();
                match to_string(&res) {
                    Ok(res) => Some(self.resolve(&Expr::Literal {
                        span,
                        value: Literal::String(res),
                    })),
                    Err(e) => Some(Err(ErrorCode::IllegalRole(format!(
                        "Failed to serialize available roles into JSON string: {}",
                        e
                    )))),
                }
            }
            ("connection_id", &[]) => Some(self.resolve(&Expr::Literal {
                span,
                value: Literal::String(self.ctx.get_connection_id()),
            })),
            ("client_session_id", &[]) => Some(self.resolve(&Expr::Literal {
                span,
                value: Literal::String(
                    self.ctx.get_current_client_session_id().unwrap_or_default(),
                ),
            })),
            ("timezone", &[]) => {
                let tz = self.ctx.get_settings().get_timezone().unwrap();
                Some(self.resolve(&Expr::Literal {
                    span,
                    value: Literal::String(tz),
                }))
            }
            ("nullif", &[arg_x, arg_y]) => {
                // Rewrite nullif(x, y) to if(x = y, null, x)
                Some(self.resolve_function(span, "if", vec![], &[
                    &Expr::BinaryOp {
                        span,
                        op: BinaryOperator::Eq,
                        left: Box::new(arg_x.clone()),
                        right: Box::new(arg_y.clone()),
                    },
                    &Expr::Literal {
                        span,
                        value: Literal::Null,
                    },
                    arg_x,
                ]))
            }
            ("equal_null", &[arg_x, arg_y]) => {
                // Rewrite equal_null(x, y) to if(is_not_null( x = y ), is_true( x = y ), x is null and y is null)
                let eq_expr = Expr::BinaryOp {
                    span,
                    op: BinaryOperator::Eq,
                    left: Box::new(arg_x.clone()),
                    right: Box::new(arg_y.clone()),
                };

                let is_null_x = Expr::IsNull {
                    span,
                    expr: Box::new(arg_x.clone()),
                    not: false,
                };
                let is_null_y = Expr::IsNull {
                    span,
                    expr: Box::new(arg_y.clone()),
                    not: false,
                };

                Some(self.resolve_function(span, "if", vec![], &[
                    &Expr::IsNull {
                        span,
                        expr: Box::new(eq_expr.clone()),
                        not: true,
                    },
                    &Expr::FunctionCall {
                        span,
                        func: ASTFunctionCall {
                            name: Identifier::from_name(span, "is_true"),
                            args: vec![eq_expr],
                            ..Default::default()
                        },
                    },
                    &Expr::FunctionCall {
                        span,
                        func: ASTFunctionCall {
                            name: Identifier::from_name(span, "and_filters"),
                            args: vec![is_null_x, is_null_y],
                            ..Default::default()
                        },
                    },
                ]))
            }
            ("iff", args) => Some(self.resolve_function(span, "if", vec![], args)),
            ("ifnull" | "nvl", args) => {
                if args.len() == 2 {
                    // Rewrite ifnull(x, y) | nvl(x, y) to if(is_null(x), y, x)
                    Some(self.resolve_function(span, "if", vec![], &[
                        &Expr::IsNull {
                            span,
                            expr: Box::new(args[0].clone()),
                            not: false,
                        },
                        args[1],
                        args[0],
                    ]))
                } else {
                    // Rewrite ifnull(args) to coalesce(x, y)
                    // Rewrite nvl(args) to coalesce(args)
                    // nvl is essentially an alias for ifnull.
                    Some(self.resolve_function(span, "coalesce", vec![], args))
                }
            }
            ("nvl2", &[arg_x, arg_y, arg_z]) => {
                // Rewrite nvl2(x, y, z) to if(is_not_null(x), y, z)
                Some(self.resolve_function(span, "if", vec![], &[
                    &Expr::IsNull {
                        span,
                        expr: Box::new(arg_x.clone()),
                        not: true,
                    },
                    arg_y,
                    arg_z,
                ]))
            }
            ("is_null", &[arg_x]) | ("isnull", &[arg_x]) => {
                // Rewrite is_null(x) to not(is_not_null(x))
                Some(
                    self.resolve_unary_op(span, &UnaryOperator::Not, &Expr::FunctionCall {
                        span,
                        func: ASTFunctionCall {
                            distinct: false,
                            name: Identifier::from_name(span, "is_not_null"),
                            args: vec![arg_x.clone()],
                            params: vec![],
                            order_by: vec![],
                            window: None,
                            lambda: None,
                        },
                    }),
                )
            }
            ("is_error", &[arg_x]) => {
                // Rewrite is_error(x) to not(is_not_error(x))
                Some(
                    self.resolve_unary_op(span, &UnaryOperator::Not, &Expr::FunctionCall {
                        span,
                        func: ASTFunctionCall {
                            distinct: false,
                            name: Identifier::from_name(span, "is_not_error"),
                            args: vec![arg_x.clone()],
                            params: vec![],
                            order_by: vec![],
                            window: None,
                            lambda: None,
                        },
                    }),
                )
            }
            ("error_or", args) => {
                // error_or(arg0, arg1, ..., argN) is essentially
                // if(is_not_error(arg0), arg0, is_not_error(arg1), arg1, ..., argN)
                let mut new_args = Vec::with_capacity(args.len() * 2 + 1);

                for arg in args.iter() {
                    let is_not_error = Expr::FunctionCall {
                        span,
                        func: ASTFunctionCall {
                            distinct: false,
                            name: Identifier::from_name(span, "is_not_error"),
                            args: vec![(*arg).clone()],
                            params: vec![],
                            order_by: vec![],
                            window: None,
                            lambda: None,
                        },
                    };
                    new_args.push(is_not_error);
                    new_args.push((*arg).clone());
                }
                new_args.push(Expr::Literal {
                    span,
                    value: Literal::Null,
                });

                let args_ref: Vec<&Expr> = new_args.iter().collect();
                Some(self.resolve_function(span, "if", vec![], &args_ref))
            }
            ("coalesce", args) => {
                // coalesce(arg0, arg1, ..., argN) is essentially
                // if(is_not_null(arg0), assume_not_null(arg0), is_not_null(arg1), assume_not_null(arg1), ..., argN)
                // with constant Literal::Null arguments removed.
                let mut new_args = Vec::with_capacity(args.len() * 2 + 1);
                for arg in args.iter() {
                    if let Expr::Literal {
                        span: _,
                        value: Literal::Null,
                    } = arg
                    {
                        continue;
                    }

                    let is_not_null_expr = Expr::IsNull {
                        span,
                        expr: Box::new((*arg).clone()),
                        not: true,
                    };
                    if let Ok(res) = self.resolve(&is_not_null_expr) {
                        if let ScalarExpr::ConstantExpr(c) = res.0 {
                            if Scalar::Boolean(false) == c.value {
                                continue;
                            }
                        }
                    }

                    let assume_not_null_expr = Expr::FunctionCall {
                        span,
                        func: ASTFunctionCall {
                            distinct: false,
                            name: Identifier::from_name(span, "assume_not_null"),
                            args: vec![(*arg).clone()],
                            params: vec![],
                            order_by: vec![],
                            window: None,
                            lambda: None,
                        },
                    };

                    new_args.push(is_not_null_expr);
                    new_args.push(assume_not_null_expr);
                }
                new_args.push(Expr::Literal {
                    span,
                    value: Literal::Null,
                });

                // coalesce(all_null) => null
                if new_args.len() == 1 {
                    new_args.push(Expr::Literal {
                        span,
                        value: Literal::Null,
                    });
                    new_args.push(Expr::Literal {
                        span,
                        value: Literal::Null,
                    });
                }

                let args_ref: Vec<&Expr> = new_args.iter().collect();
                Some(self.resolve_function(span, "if", vec![], &args_ref))
            }
            ("decode", args) => {
                // DECODE( <expr> , <search1> , <result1> [ , <search2> , <result2> ... ] [ , <default> ] )
                // Note that, contrary to CASE, a NULL value in the select expression matches a NULL value in the search expressions.
                if args.len() < 3 {
                    return Some(Err(ErrorCode::BadArguments(
                        "DECODE requires at least 3 arguments",
                    )
                    .set_span(span)));
                }

                let mut new_args = Vec::with_capacity(args.len() * 2 + 1);
                let search_expr = args[0].clone();
                let mut i = 1;

                while i < args.len() {
                    let search = args[i].clone();
                    let result = if i + 1 < args.len() {
                        args[i + 1].clone()
                    } else {
                        // If we're at the last argument and it's odd, it's the default value
                        break;
                    };

                    // (a = b) or (a is null and b is null)
                    let is_null_a = Expr::IsNull {
                        span,
                        expr: Box::new(search_expr.clone()),
                        not: false,
                    };
                    let is_null_b = Expr::IsNull {
                        span,
                        expr: Box::new(search.clone()),
                        not: false,
                    };
                    let and_expr = Expr::BinaryOp {
                        span,
                        op: BinaryOperator::And,
                        left: Box::new(is_null_a),
                        right: Box::new(is_null_b),
                    };

                    let eq_expr = Expr::BinaryOp {
                        span,
                        op: BinaryOperator::Eq,
                        left: Box::new(search_expr.clone()),
                        right: Box::new(search),
                    };

                    let or_expr = Expr::BinaryOp {
                        span,
                        op: BinaryOperator::Or,
                        left: Box::new(eq_expr),
                        right: Box::new(and_expr),
                    };

                    new_args.push(or_expr);
                    new_args.push(result);
                    i += 2;
                }

                // Add default value if it exists
                if i + 1 == args.len() {
                    new_args.push(args[i].clone());
                } else {
                    new_args.push(Expr::Literal {
                        span,
                        value: Literal::Null,
                    });
                }

                let args_ref: Vec<&Expr> = new_args.iter().collect();
                Some(self.resolve_function(span, "if", vec![], &args_ref))
            }
            ("last_query_id", args) => {
                // last_query_id(index) returns query_id in current session by index
                let res: Result<i64> = try {
                    if args.len() > 1 {
                        return Some(Err(ErrorCode::BadArguments(
                            "last_query_id needs at most one integer argument",
                        )
                        .set_span(span)));
                    }
                    if args.is_empty() {
                        -1
                    } else {
                        let box (scalar, _) = self.resolve(args[0])?;

                        let expr = scalar.as_expr()?;
                        match expr.as_constant() {
                            Some(_) => {
                                check_number(span, &self.func_ctx, &expr, &BUILTIN_FUNCTIONS)?
                            }
                            None => {
                                return Some(Err(ErrorCode::BadArguments(
                                    "last_query_id argument only support constant argument",
                                )
                                .set_span(span)));
                            }
                        }
                    }
                };

                Some(match res {
                    Ok(index) => {
                        if let Some(query_id) = self.ctx.get_last_query_id(index as i32) {
                            self.resolve(&Expr::Literal {
                                span,
                                value: Literal::String(query_id),
                            })
                        } else {
                            self.resolve(&Expr::Literal {
                                span,
                                value: Literal::Null,
                            })
                        }
                    }
                    Err(e) => Err(e),
                })
            }
            ("array_sort", args) => {
                if args.is_empty() || args.len() > 3 {
                    return None;
                }
                let mut asc = true;
                let mut nulls_first = None;
                if args.len() >= 2 {
                    let box (arg, _) = self.resolve(args[1]).ok()?;
                    if let Ok(arg) = ConstantExpr::try_from(arg) {
                        if let Scalar::String(sort_order) = arg.value {
                            if sort_order.eq_ignore_ascii_case("asc") {
                                asc = true;
                            } else if sort_order.eq_ignore_ascii_case("desc") {
                                asc = false;
                            } else {
                                return Some(Err(ErrorCode::SemanticError(
                                    "Sorting order must be either ASC or DESC",
                                )));
                            }
                        } else {
                            return Some(Err(ErrorCode::SemanticError(
                                "Sorting order must be either ASC or DESC",
                            )));
                        }
                    } else {
                        return Some(Err(ErrorCode::SemanticError(
                            "Sorting order must be a constant string",
                        )));
                    }
                }
                if args.len() == 3 {
                    let box (arg, _) = self.resolve(args[2]).ok()?;
                    if let Ok(arg) = ConstantExpr::try_from(arg) {
                        if let Scalar::String(nulls_order) = arg.value {
                            if nulls_order.eq_ignore_ascii_case("nulls first") {
                                nulls_first = Some(true);
                            } else if nulls_order.eq_ignore_ascii_case("nulls last") {
                                nulls_first = Some(false);
                            } else {
                                return Some(Err(ErrorCode::SemanticError(
                                    "Null sorting order must be either NULLS FIRST or NULLS LAST",
                                )));
                            }
                        } else {
                            return Some(Err(ErrorCode::SemanticError(
                                "Null sorting order must be either NULLS FIRST or NULLS LAST",
                            )));
                        }
                    } else {
                        return Some(Err(ErrorCode::SemanticError(
                            "Null sorting order must be a constant string",
                        )));
                    }
                }

                let nulls_first = nulls_first.unwrap_or_else(|| {
                    let settings = self.ctx.get_settings();
                    settings.get_nulls_first()(asc)
                });

                let func_name = match (asc, nulls_first) {
                    (true, true) => "array_sort_asc_null_first",
                    (false, true) => "array_sort_desc_null_first",
                    (true, false) => "array_sort_asc_null_last",
                    (false, false) => "array_sort_desc_null_last",
                };
                let args_ref: Vec<&Expr> = vec![args[0]];
                Some(self.resolve_function(span, func_name, vec![], &args_ref))
            }
            ("array_aggregate", args) => {
                if args.len() != 2 {
                    return None;
                }
                let box (arg, _) = self.resolve(args[1]).ok()?;
                if let Ok(arg) = ConstantExpr::try_from(arg) {
                    if let Scalar::String(aggr_func_name) = arg.value {
                        let func_name = format!("array_{}", aggr_func_name);
                        let args_ref: Vec<&Expr> = vec![args[0]];
                        return Some(self.resolve_function(span, &func_name, vec![], &args_ref));
                    }
                }
                Some(Err(ErrorCode::SemanticError(
                    "Array aggregate function name be must a constant string",
                )))
            }
            ("to_variant", args) => {
                if args.len() != 1 {
                    return None;
                }
                let box (scalar, data_type) = self.resolve(args[0]).ok()?;
                self.resolve_cast_to_variant(span, &data_type, &scalar, false)
            }
            ("try_to_variant", args) => {
                if args.len() != 1 {
                    return None;
                }
                let box (scalar, data_type) = self.resolve(args[0]).ok()?;
                self.resolve_cast_to_variant(span, &data_type, &scalar, true)
            }
            (name @ ("greatest" | "least"), args) => {
                let array_func = if name == "greatest" {
                    "array_max"
                } else {
                    "array_min"
                };
                let (array, _) = *self.resolve_function(span, "array", vec![], args).ok()?;
                let null_scalar = ScalarExpr::ConstantExpr(ConstantExpr {
                    span: None,
                    value: Scalar::Null,
                });

                let contains_null = self
                    .resolve_scalar_function_call(span, "array_contains", vec![], vec![
                        array.clone(),
                        null_scalar.clone(),
                    ])
                    .ok()?;

                let max = self
                    .resolve_scalar_function_call(span, array_func, vec![], vec![array])
                    .ok()?;

                Some(self.resolve_scalar_function_call(span, "if", vec![], vec![
                    contains_null.0.clone(),
                    null_scalar.clone(),
                    max.0.clone(),
                ]))
            }
            ("greatest_ignore_nulls", args) => {
                let (array, _) = *self.resolve_function(span, "array", vec![], args).ok()?;
                Some(self.resolve_scalar_function_call(span, "array_max", vec![], vec![array]))
            }
            ("least_ignore_nulls", args) => {
                let (array, _) = *self.resolve_function(span, "array", vec![], args).ok()?;
                Some(self.resolve_scalar_function_call(span, "array_min", vec![], vec![array]))
            }
            ("getvariable", args) => {
                if args.len() != 1 {
                    return None;
                }
                let box (scalar, _) = self.resolve(args[0]).ok()?;

                if let Ok(arg) = ConstantExpr::try_from(scalar) {
                    if let Scalar::String(var_name) = arg.value {
                        let var_value = self.ctx.get_variable(&var_name).unwrap_or(Scalar::Null);
                        let var_value = shrink_scalar(var_value);
                        let data_type = var_value.as_ref().infer_data_type();
                        return Some(Ok(Box::new((
                            ScalarExpr::ConstantExpr(ConstantExpr {
                                span,
                                value: var_value,
                            }),
                            data_type,
                        ))));
                    }
                }
                Some(Err(ErrorCode::SemanticError(
                    "Variable name must be a constant string",
                )))
            }
            ("get" | "get_string", &[arg_x, arg_y]) => {
                if !self.bind_context.allow_virtual_column {
                    return None;
                }

                let mut expr = arg_x;
                let mut path_exprs = VecDeque::new();
                path_exprs.push_back(arg_y);
                while let Expr::FunctionCall { func, .. } = expr {
                    let func_name =
                        normalize_identifier(&func.name, self.name_resolution_ctx).to_string();
                    let func_name = func_name.as_str();
                    if func_name == "get" && func.args.len() == 2 {
                        expr = &func.args[0];
                        path_exprs.push_back(&func.args[1]);
                    } else {
                        return None;
                    }
                }
                let mut paths = VecDeque::with_capacity(path_exprs.len());
                while let Some(path_expr) = path_exprs.pop_back() {
                    if let Expr::Literal { span, value } = path_expr {
                        if matches!(value, Literal::UInt64(_) | Literal::String(_)) {
                            paths.push_back((*span, value.clone()));
                        } else {
                            return Some(Err(ErrorCode::SemanticError(format!(
                                "Unsupported argument: {:?}",
                                value
                            ))
                            .set_span(*span)));
                        }
                    } else {
                        return None;
                    }
                }
                if func_name == "get_string" {
                    if let Ok(box (scalar, data_type)) = self.resolve_map_access(span, expr, paths)
                    {
                        if data_type.remove_nullable() == DataType::Variant {
                            let target_type = DataType::Nullable(Box::new(DataType::String));
                            let new_scalar = ScalarExpr::CastExpr(CastExpr {
                                span: scalar.span(),
                                is_try: false,
                                argument: Box::new(scalar),
                                target_type: Box::new(target_type.clone()),
                            });
                            return Some(Ok(Box::new((new_scalar, target_type))));
                        }
                    }
                    None
                } else {
                    Some(self.resolve_map_access(span, expr, paths))
                }
            }
            (func_name, &[expr])
                if matches!(
                    func_name,
                    "hex_decode_string"
                        | "try_hex_decode_string"
                        | "base64_decode_string"
                        | "try_base64_decode_string"
                ) =>
            {
                Some(self.resolve(&Expr::Cast {
                    span,
                    expr: Box::new(Expr::FunctionCall {
                        span,
                        func: ASTFunctionCall {
                            distinct: false,
                            name: Identifier::from_name(
                                span,
                                func_name.replace("_string", "_binary"),
                            ),
                            args: vec![expr.clone()],
                            params: vec![],
                            order_by: vec![],
                            window: None,
                            lambda: None,
                        },
                    }),
                    target_type: TypeName::String,
                    pg_style: false,
                }))
            }
            _ => None,
        }
    }

    fn rewritable_variant_functions() -> &'static [Ascii<&'static str>] {
        static VARIANT_FUNCTIONS: &[Ascii<&'static str>] = &[
            Ascii::new("get_by_keypath"),
            Ascii::new("get_by_keypath_string"),
        ];
        VARIANT_FUNCTIONS
    }

    fn try_rewrite_variant_function(
        &mut self,
        span: Span,
        func_name: &str,
        args: &[ScalarExpr],
        arg_types: &[DataType],
    ) -> Option<Result<Box<(ScalarExpr, DataType)>>> {
        if !self.bind_context.allow_virtual_column
            || !Self::rewritable_variant_functions().contains(&Ascii::new(func_name))
            || arg_types.is_empty()
            || arg_types[0].remove_nullable() != DataType::Variant
        {
            return None;
        }
        let ScalarExpr::BoundColumnRef(BoundColumnRef { column, .. }) = &args[0] else {
            return None;
        };
        if column.index >= self.metadata.read().columns().len() {
            return None;
        }
        // only rewrite when arg[1] is path
        let ScalarExpr::ConstantExpr(ConstantExpr {
            value: Scalar::String(path),
            ..
        }) = &args[1]
        else {
            return None;
        };
        let Ok(keypaths) = parse_key_paths(path.as_bytes()) else {
            return None;
        };

        // try rewrite as virtual column and pushdown to storage layer.
        let column_entry = self.metadata.read().column(column.index).clone();
        if let ColumnEntry::BaseTableColumn(base_column) = column_entry {
            if let Some(box (scalar, data_type)) = self.try_rewrite_virtual_column(
                span,
                base_column.table_index,
                base_column.column_id,
                &base_column.column_name,
                &keypaths,
            ) {
                if func_name == "get_by_keypath_string" {
                    let target_type = DataType::Nullable(Box::new(DataType::String));
                    let new_scalar = ScalarExpr::CastExpr(CastExpr {
                        span: scalar.span(),
                        is_try: false,
                        argument: Box::new(scalar),
                        target_type: Box::new(target_type.clone()),
                    });
                    return Some(Ok(Box::new((new_scalar, target_type))));
                } else {
                    return Some(Ok(Box::new((scalar, data_type))));
                }
            }
        }
        None
    }

    fn vector_functions() -> &'static [Ascii<&'static str>] {
        static VECTOR_FUNCTIONS: &[Ascii<&'static str>] = &[
            Ascii::new("cosine_distance"),
            Ascii::new("l1_distance"),
            Ascii::new("l2_distance"),
        ];
        VECTOR_FUNCTIONS
    }

    fn try_rewrite_vector_function(
        &mut self,
        span: Span,
        func_name: &str,
        args: &[ScalarExpr],
    ) -> Option<Result<Box<(ScalarExpr, DataType)>>> {
        // Try rewrite vector distance function to vector score internal column,
        // so that the vector index can be used to accelerate the query.
        let uni_case_func_name = Ascii::new(func_name);
        if Self::vector_functions().contains(&uni_case_func_name) {
            match args {
                [
                    ScalarExpr::BoundColumnRef(BoundColumnRef {
                        column:
                            ColumnBinding {
                                table_index,
                                database_name,
                                table_name,
                                column_name,
                                data_type,
                                ..
                            },
                        ..
                    }),
                    ScalarExpr::CastExpr(CastExpr {
                        argument,
                        target_type,
                        ..
                    }),
                ]
                | [
                    ScalarExpr::CastExpr(CastExpr {
                        argument,
                        target_type,
                        ..
                    }),
                    ScalarExpr::BoundColumnRef(BoundColumnRef {
                        column:
                            ColumnBinding {
                                table_index,
                                database_name,
                                table_name,
                                column_name,
                                data_type,
                                ..
                            },
                        ..
                    }),
                ] => {
                    let col_data_type = data_type.remove_nullable();
                    let target_type = target_type.remove_nullable();
                    if table_index.is_some()
                        && matches!(col_data_type, DataType::Vector(_))
                        && matches!(&**argument, ScalarExpr::ConstantExpr(_))
                        && matches!(&target_type, DataType::Vector(_))
                    {
                        let table_index = table_index.unwrap();
                        let table_entry = self.metadata.read().table(table_index).clone();
                        let table = table_entry.table();
                        let table_info = table.get_table_info();
                        let table_schema = table.schema();
                        let table_indexes = &table_info.meta.indexes;
                        if self
                            .bind_context
                            .vector_index_map
                            .contains_key(&table_index)
                        {
                            return None;
                        }
                        let Ok(column_id) = table_schema.column_id_of(column_name) else {
                            return None;
                        };
                        for vector_index in table_indexes.values() {
                            if vector_index.index_type != TableIndexType::Vector {
                                continue;
                            }
                            let Some(distances) = vector_index.options.get("distance") else {
                                continue;
                            };
                            // distance_type must match function name
                            let mut matched_distance = false;
                            let distance_types: Vec<&str> = distances.split(',').collect();
                            for distance_type in distance_types {
                                if func_name.starts_with(distance_type) {
                                    matched_distance = true;
                                    break;
                                }
                            }
                            if !matched_distance {
                                continue;
                            }
                            if vector_index.column_ids.contains(&column_id) {
                                let internal_column = InternalColumn::new(
                                    VECTOR_SCORE_COL_NAME,
                                    InternalColumnType::VectorScore,
                                );
                                let internal_column_binding = InternalColumnBinding {
                                    database_name: database_name.clone(),
                                    table_name: table_name.clone(),
                                    internal_column,
                                };
                                let Ok(column_binding) =
                                    self.bind_context.add_internal_column_binding(
                                        &internal_column_binding,
                                        self.metadata.clone(),
                                        Some(table_index),
                                        false,
                                    )
                                else {
                                    return None;
                                };

                                let new_column = ScalarExpr::BoundColumnRef(BoundColumnRef {
                                    span,
                                    column: column_binding,
                                });

                                let arg = ConstantExpr::try_from(*argument.clone()).unwrap();
                                let Scalar::Array(arg_col) = arg.value else {
                                    return None;
                                };
                                let arg_col = arg_col.remove_nullable();

                                let col_vector_type = col_data_type.as_vector().unwrap();
                                let col_dimension = col_vector_type.dimension() as usize;
                                let arg_vector_type = target_type.as_vector().unwrap();
                                let arg_dimension = arg_vector_type.dimension() as usize;
                                if col_dimension != arg_dimension || arg_col.len() != col_dimension
                                {
                                    return None;
                                }
                                let mut query_values = Vec::with_capacity(arg_col.len());
                                match arg_col {
                                    Column::Number(num_col) => {
                                        for i in 0..num_col.len() {
                                            let num = unsafe { num_col.index_unchecked(i) };
                                            query_values.push(num.to_f32());
                                        }
                                    }
                                    Column::Decimal(dec_col) => {
                                        for i in 0..dec_col.len() {
                                            let dec = unsafe { dec_col.index_unchecked(i) };
                                            query_values.push(F32::from(dec.to_float32()));
                                        }
                                    }
                                    _ => {
                                        return None;
                                    }
                                }

                                let index_info = VectorIndexInfo {
                                    index_name: vector_index.name.clone(),
                                    index_version: vector_index.version.clone(),
                                    index_options: vector_index.options.clone(),
                                    column_id,
                                    func_name: func_name.to_string(),
                                    query_values,
                                };
                                self.bind_context
                                    .vector_index_map
                                    .insert(table_index, index_info);

                                return Some(Ok(Box::new((
                                    new_column,
                                    DataType::Number(NumberDataType::Float32),
                                ))));
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        None
    }

    fn resolve_trim_function(
        &mut self,
        span: Span,
        expr: &Expr,
        trim_where: &Option<(TrimWhere, Box<Expr>)>,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let (func_name, trim_scalar, _trim_type) = if let Some((trim_type, trim_expr)) = trim_where
        {
            let func_name = match trim_type {
                TrimWhere::Leading => "trim_leading",
                TrimWhere::Trailing => "trim_trailing",
                TrimWhere::Both => "trim_both",
            };

            let box (trim_scalar, trim_type) = self.resolve(trim_expr)?;
            (func_name, trim_scalar, trim_type)
        } else {
            let trim_scalar = ConstantExpr {
                span,
                value: Scalar::String(" ".to_string()),
            }
            .into();
            ("trim_both", trim_scalar, DataType::String)
        };

        let box (trim_source, _source_type) = self.resolve(expr)?;
        let args = vec![trim_source, trim_scalar];

        self.resolve_scalar_function_call(span, func_name, vec![], args)
    }

    /// Resolve literal values.
    pub fn resolve_literal_scalar(
        &self,
        literal: &databend_common_ast::ast::Literal,
    ) -> Result<Box<(Scalar, DataType)>> {
        let value = match literal {
            Literal::UInt64(value) => Scalar::Number(NumberScalar::UInt64(*value)),
            Literal::Decimal256 {
                value,
                precision,
                scale,
            } => Scalar::Decimal(DecimalScalar::Decimal256(
                i256(*value),
                DecimalSize::new_unchecked(*precision, *scale),
            )),
            Literal::Float64(float) => Scalar::Number(NumberScalar::Float64((*float).into())),
            Literal::String(string) => Scalar::String(string.clone()),
            Literal::Boolean(boolean) => Scalar::Boolean(*boolean),
            Literal::Null => Scalar::Null,
        };
        let value = shrink_scalar(value);
        let data_type = value.as_ref().infer_data_type();
        Ok(Box::new((value, data_type)))
    }

    pub fn resolve_minus_literal_scalar(
        &self,
        span: Span,
        literal: &databend_common_ast::ast::Literal,
    ) -> Result<Box<(Scalar, DataType)>> {
        let value = match literal {
            Literal::UInt64(v) => {
                if *v <= i64::MAX as u64 {
                    Scalar::Number(NumberScalar::Int64(-(*v as i64)))
                } else {
                    Scalar::Decimal(DecimalScalar::Decimal128(
                        -(*v as i128),
                        DecimalSize::new_unchecked(i128::MAX_PRECISION, 0),
                    ))
                }
            }
            Literal::Decimal256 {
                value,
                precision,
                scale,
            } => Scalar::Decimal(DecimalScalar::Decimal256(
                i256(*value).checked_mul(i256::minus_one()).unwrap(),
                DecimalSize::new_unchecked(*precision, *scale),
            )),
            Literal::Float64(v) => Scalar::Number(NumberScalar::Float64((-*v).into())),
            Literal::Null => Scalar::Null,
            Literal::String(_) | Literal::Boolean(_) => {
                return Err(ErrorCode::InvalidArgument(format!(
                    "Invalid minus operator for {}",
                    literal
                ))
                .set_span(span));
            }
        };
        let value = shrink_scalar(value);
        let data_type = value.as_ref().infer_data_type();
        Ok(Box::new((value, data_type)))
    }

    // Fast path for constant arrays so we don't need to go through the scalar `array()` function
    // (which performs full type-checking and constant-folding). Non-constant elements still use
    // the generic resolver to preserve the previous behaviour.
    fn resolve_array(&mut self, span: Span, exprs: &[Expr]) -> Result<Box<(ScalarExpr, DataType)>> {
        let mut elems = Vec::with_capacity(exprs.len());
        let mut constant_values: Option<Vec<(Scalar, DataType)>> =
            Some(Vec::with_capacity(exprs.len()));
        let mut element_type: Option<DataType> = None;

        let mut data_type_set = HashSet::with_capacity(2);
        for expr in exprs {
            let box (arg, data_type) = self.resolve(expr)?;
            if let Some(values) = constant_values.as_mut() {
                let maybe_constant = match &arg {
                    ScalarExpr::ConstantExpr(constant) => Some(constant.value.clone()),
                    ScalarExpr::TypedConstantExpr(constant, _) => Some(constant.value.clone()),
                    _ => None,
                };
                if let Some(value) = maybe_constant {
                    // If the data type has already been computed,
                    // we don't need to compute the common type again.
                    if data_type_set.contains(&data_type) {
                        elems.push(arg);
                        values.push((value, data_type));
                        continue;
                    }
                    element_type = if let Some(current_ty) = element_type.clone() {
                        common_super_type(
                            current_ty.clone(),
                            data_type.clone(),
                            &BUILTIN_FUNCTIONS.default_cast_rules,
                        )
                    } else {
                        Some(data_type.clone())
                    };

                    if element_type.is_some() {
                        data_type_set.insert(data_type.clone());
                        values.push((value, data_type));
                    } else {
                        constant_values = None;
                        element_type = None;
                    }
                } else {
                    constant_values = None;
                    element_type = None;
                }
            }
            elems.push(arg);
        }

        if let (Some(values), Some(element_ty)) = (constant_values, element_type) {
            let mut casted = Vec::with_capacity(values.len());
            for (value, ty) in values {
                if ty == element_ty {
                    casted.push(value);
                } else {
                    casted.push(cast_scalar(span, value, &element_ty, &BUILTIN_FUNCTIONS)?);
                }
            }
            return Ok(Self::build_constant_array(span, element_ty, casted));
        }

        self.resolve_scalar_function_call(span, "array", vec![], elems)
    }

    fn build_constant_array(
        span: Span,
        element_ty: DataType,
        values: Vec<Scalar>,
    ) -> Box<(ScalarExpr, DataType)> {
        let mut builder = ColumnBuilder::with_capacity(&element_ty, values.len());
        for value in &values {
            builder.push(value.as_ref());
        }
        let scalar = Scalar::Array(builder.build());
        Box::new((
            ScalarExpr::ConstantExpr(ConstantExpr {
                span,
                value: scalar,
            }),
            DataType::Array(Box::new(element_ty)),
        ))
    }

    fn resolve_map(
        &mut self,
        span: Span,
        kvs: &[(Literal, Expr)],
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let mut keys = Vec::with_capacity(kvs.len());
        let mut vals = Vec::with_capacity(kvs.len());
        for (key_expr, val_expr) in kvs {
            let box (key_arg, _data_type) = self.resolve_literal(span, key_expr)?;
            keys.push(key_arg);
            let box (val_arg, _data_type) = self.resolve(val_expr)?;
            vals.push(val_arg);
        }
        let box (key_arg, _data_type) =
            self.resolve_scalar_function_call(span, "array", vec![], keys)?;
        let box (val_arg, _data_type) =
            self.resolve_scalar_function_call(span, "array", vec![], vals)?;
        let args = vec![key_arg, val_arg];

        self.resolve_scalar_function_call(span, "map", vec![], args)
    }

    fn resolve_tuple(&mut self, span: Span, exprs: &[Expr]) -> Result<Box<(ScalarExpr, DataType)>> {
        let mut args = Vec::with_capacity(exprs.len());
        for expr in exprs {
            let box (arg, _data_type) = self.resolve(expr)?;
            args.push(arg);
        }

        self.resolve_scalar_function_call(span, "tuple", vec![], args)
    }

    fn resolve_like(
        &mut self,
        op: &BinaryOperator,
        span: Span,
        left: &Expr,
        right: &Expr,
        like_str: &str,
        escape: &Option<String>,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let new_like_str = if let Some(escape) = escape {
            Cow::Owned(convert_escape_pattern(
                like_str,
                escape.chars().next().unwrap(),
            ))
        } else {
            Cow::Borrowed(like_str)
        };
        if check_percent(&new_like_str) {
            // Convert to `a is not null`
            let is_not_null = Expr::IsNull {
                span: None,
                expr: Box::new(left.clone()),
                not: true,
            };
            self.resolve(&is_not_null)
        } else if check_const(&new_like_str) {
            // Convert to equal comparison
            self.resolve_binary_op(span, &BinaryOperator::Eq, left, right)
        } else if check_prefix(&new_like_str) {
            // Convert to `a >= like_str and a < like_str + 1`
            let mut char_vec: Vec<char> = new_like_str[0..new_like_str.len() - 1].chars().collect();
            let len = char_vec.len();
            let ascii_val = *char_vec.last().unwrap() as u8 + 1;
            char_vec[len - 1] = ascii_val as char;
            let like_str_plus: String = char_vec.iter().collect();
            let (new_left, _) =
                *self.resolve_binary_op(span, &BinaryOperator::Gte, left, &Expr::Literal {
                    span: None,
                    value: Literal::String(new_like_str[..new_like_str.len() - 1].to_owned()),
                })?;
            let (new_right, _) =
                *self.resolve_binary_op(span, &BinaryOperator::Lt, left, &Expr::Literal {
                    span: None,
                    value: Literal::String(like_str_plus),
                })?;
            self.resolve_scalar_function_call(span, "and", vec![], vec![new_left, new_right])
        } else {
            self.resolve_like_escape(op, span, left, right, escape)
        }
    }

    fn resolve_like_escape(
        &mut self,
        op: &BinaryOperator,
        span: Span,
        left: &Expr,
        right: &Expr,
        escape: &Option<String>,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let name = op.to_func_name();
        let escape_expr = escape.as_ref().map(|escape| Expr::Literal {
            span,
            value: Literal::String(escape.clone()),
        });
        let mut arguments = vec![left, right];
        if let Some(expr) = &escape_expr {
            arguments.push(expr)
        }
        self.resolve_function(span, name.as_str(), vec![], &arguments)
    }

    fn resolve_udf(
        &mut self,
        span: Span,
        udf_name: &str,
        arguments: &[Expr],
    ) -> Result<Option<Box<(ScalarExpr, DataType)>>> {
        if self.forbid_udf {
            return Ok(None);
        }

        let tenant = self.ctx.get_tenant();
        let provider = UserApiProvider::instance();
        let udf = databend_common_base::runtime::block_on(provider.get_udf(&tenant, udf_name))?;

        let Some(udf) = udf else {
            return Ok(None);
        };

        let name = udf.name;

        match udf.definition {
            UDFDefinition::LambdaUDF(udf_def) => Ok(Some(
                self.resolve_lambda_udf(span, name, arguments, udf_def)?,
            )),
            UDFDefinition::UDFServer(udf_def) => Ok(Some(
                self.resolve_udf_server(span, name, arguments, udf_def)?,
            )),
            UDFDefinition::UDFScript(udf_def) => Ok(Some(
                self.resolve_udf_script(span, name, arguments, udf_def)?,
            )),
            UDFDefinition::UDAFScript(udf_def) => Ok(Some(
                self.resolve_udaf_script(span, name, arguments, udf_def)?,
            )),
            UDFDefinition::UDTF(_) => unreachable!(),
            UDFDefinition::UDTFServer(_) => unreachable!(),
            UDFDefinition::ScalarUDF(udf_def) => Ok(Some(
                self.resolve_scalar_udf(span, name, arguments, udf_def)?,
            )),
        }
    }

    fn resolve_udf_server(
        &mut self,
        span: Span,
        name: String,
        arguments: &[Expr],
        udf_definition: UDFServer,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        self.resolve_udf_server_internal(span, name, arguments, udf_definition, true)
    }

    fn resolve_udf_server_internal(
        &mut self,
        span: Span,
        name: String,
        arguments: &[Expr],
        mut udf_definition: UDFServer,
        validate_address: bool,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        if validate_address {
            UDFValidator::is_udf_server_allowed(&udf_definition.address)?;
        }
        if arguments.len() != udf_definition.arg_types.len() {
            return Err(ErrorCode::InvalidArgument(format!(
                "Require {} parameters, but got: {}",
                udf_definition.arg_types.len(),
                arguments.len()
            ))
            .set_span(span));
        }

        let mut all_args_const = true;
        let mut args = Vec::with_capacity(arguments.len());
        let mut stage_locations = Vec::new();
        for (i, (argument, dest_type)) in arguments
            .iter()
            .zip(udf_definition.arg_types.iter())
            .enumerate()
        {
            let box (arg, ty) = self.resolve(argument)?;
            // TODO: support cast constant
            if !matches!(arg, ScalarExpr::ConstantExpr(_))
                || (ty != dest_type.remove_nullable()
                    && dest_type.remove_nullable() != DataType::StageLocation)
            {
                all_args_const = false;
            }
            if dest_type.remove_nullable() == DataType::StageLocation {
                if udf_definition.arg_names.is_empty() {
                    return Err(ErrorCode::InvalidArgument(
                        "StageLocation must have a corresponding variable name",
                    ));
                }
                let expr = arg.as_expr()?;
                let (expr, _) = ConstantFolder::fold(&expr, &self.func_ctx, &BUILTIN_FUNCTIONS);
                let Ok(Some(location)) =
                    expr.into_constant().map(|c| c.scalar.as_string().cloned())
                else {
                    return Err(ErrorCode::SemanticError(format!(
                        "invalid parameter {argument} for udf function, expected constant string",
                    ))
                    .set_span(span));
                };
                let (stage_info, relative_path) = databend_common_base::runtime::block_on(
                    resolve_stage_location(self.ctx.as_ref(), &location),
                )?;

                if !matches!(stage_info.stage_type, StageType::External) {
                    return Err(ErrorCode::SemanticError(format!(
                        "stage {} type is {}, UDF only support External Stage",
                        stage_info.stage_name, stage_info.stage_type,
                    ))
                    .set_span(span));
                }
                if let StorageParams::S3(config) = &stage_info.stage_params.storage {
                    if !config.security_token.is_empty() || !config.role_arn.is_empty() {
                        return Err(ErrorCode::SemanticError(format!(
                            "StageLocation: @{} must use a separate credential",
                            location
                        )));
                    }
                }

                stage_locations.push(StageLocationParam {
                    param_name: udf_definition.arg_names[i].clone(),
                    relative_path,
                    stage_info,
                });
                continue;
            }
            if ty != *dest_type {
                args.push(wrap_cast(&arg, dest_type));
            } else {
                args.push(arg);
            }
        }
        if !stage_locations.is_empty() {
            let stage_location_value = serde_json::to_string(&stage_locations)?;
            udf_definition
                .headers
                .insert("databend-stage-mapping".to_string(), stage_location_value);
        }
        let immutable = udf_definition.immutable.unwrap_or_default();
        if immutable && all_args_const {
            let mut arg_scalars = Vec::with_capacity(args.len());
            for arg in &args {
                let arg_scalar = match arg {
                    ScalarExpr::ConstantExpr(constant) => constant.value.clone(),
                    ScalarExpr::CastExpr(cast) => {
                        let constant = ConstantExpr::try_from(*cast.argument.clone()).unwrap();
                        constant.value.clone()
                    }
                    _ => unreachable!(),
                };
                arg_scalars.push(arg_scalar);
            }
            let value = databend_common_base::runtime::block_on(self.fold_udf_server(
                name.as_str(),
                arg_scalars,
                udf_definition.clone(),
            ))?;
            return Ok(Box::new((
                ConstantExpr { span, value }.into(),
                udf_definition.return_type.clone(),
            )));
        }

        let arg_names = arguments.iter().map(|arg| format!("{}", arg)).join(", ");
        let display_name = format!("{}({})", udf_definition.handler, arg_names);

        self.bind_context.have_udf_server = true;
        self.ctx.set_cacheable(false);
        Ok(Box::new((
            UDFCall {
                span,
                name,
                handler: udf_definition.handler,
                headers: udf_definition.headers,
                display_name,
                udf_type: UDFType::Server(udf_definition.address.clone()),
                arg_types: udf_definition.arg_types,
                return_type: Box::new(udf_definition.return_type.clone()),
                arguments: args,
            }
            .into(),
            udf_definition.return_type.clone(),
        )))
    }

    #[async_backtrace::framed]
    pub async fn fold_udf_server(
        &mut self,
        name: &str,
        args: Vec<Scalar>,
        udf_definition: UDFServer,
    ) -> Result<Scalar> {
        let mut block_entries = Vec::with_capacity(args.len());
        for (arg, dest_type) in args.into_iter().zip(
            udf_definition
                .arg_types
                .iter()
                .filter(|ty| ty.remove_nullable() != DataType::StageLocation),
        ) {
            if matches!(dest_type, DataType::StageLocation) {
                continue;
            }
            let entry = BlockEntry::new_const_column(dest_type.clone(), arg, 1);
            block_entries.push(entry);
        }

        let settings = self.ctx.get_settings();
        let connect_timeout = settings.get_external_server_connect_timeout_secs()?;
        let request_timeout = settings.get_external_server_request_timeout_secs()?;

        let endpoint = UDFFlightClient::build_endpoint(
            &udf_definition.address,
            connect_timeout,
            request_timeout,
            &self.ctx.get_version().udf_client_user_agent(),
        )?;

        let num_rows = 1;
        let mut client =
            UDFFlightClient::connect(&udf_definition.handler, endpoint, connect_timeout, num_rows)
                .await?
                .with_tenant(self.ctx.get_tenant().tenant_name())?
                .with_func_name(name)?
                .with_handler_name(&udf_definition.handler)?
                .with_query_id(&self.ctx.get_id())?
                .with_headers(udf_definition.headers)?;

        let result = client
            .do_exchange(
                name,
                &udf_definition.handler,
                Some(num_rows),
                block_entries,
                &udf_definition.return_type,
            )
            .await?;

        let value = unsafe { result.get_by_offset(0).index_unchecked(0) };
        Ok(value.to_owned())
    }

    fn apply_udf_cloud_resource(
        &self,
        resource_name: &str,
        resource_type: &str,
        script: String,
    ) -> Result<(String, BTreeMap<String, String>)> {
        let Some(_) = &GlobalConfig::instance()
            .query
            .common
            .cloud_control_grpc_server_address
        else {
            return Err(ErrorCode::Unimplemented(
                "SandboxUDF requires cloud control enabled, please set cloud_control_grpc_server_address in config",
            ));
        };

        let provider = CloudControlApiProvider::instance();
        let tenant = self.ctx.get_tenant();
        let user = self
            .ctx
            .get_current_user()?
            .identity()
            .display()
            .to_string();
        let query_id = self.ctx.get_id();
        let mut cfg = build_client_config(
            tenant.tenant_name().to_string(),
            user,
            query_id,
            provider.get_timeout(),
        );
        cfg.add_worker_version_info();

        let req = CreateWorkerRequest {
            tenant_id: tenant.tenant_name().to_string(),
            name: resource_name.to_string(),
            if_not_exists: true,
            tags: Default::default(),
            options: Default::default(),
            r#type: resource_type.to_string(),
            script,
        };

        let resp = databend_common_base::runtime::block_on(
            provider
                .get_worker_client()
                .create_worker(make_request(req, cfg)),
        )?;

        let endpoint = resp.endpoint;
        if endpoint.is_empty() {
            return Err(ErrorCode::CloudControlConnectError(
                "UDF cloud resource endpoint is empty".to_string(),
            ));
        }

        Ok((endpoint, resp.headers))
    }

    fn build_udf_cloud_imports(
        &self,
        imports: &[String],
        expire: Duration,
    ) -> Result<Vec<UdfAsset>> {
        if imports.is_empty() {
            return Ok(Vec::new());
        }

        let locations = imports
            .iter()
            .map(|location| location.trim_start_matches('@').to_string())
            .collect::<Vec<_>>();

        let stage_locations = databend_common_base::runtime::block_on(resolve_stage_locations(
            self.ctx.as_ref(),
            &locations,
        ))?;

        databend_common_base::runtime::block_on(async move {
            let mut results = Vec::with_capacity(stage_locations.len());
            for ((stage_info, path), location) in stage_locations.into_iter().zip(imports.iter()) {
                let op = init_stage_operator(&stage_info).map_err(|err| {
                    ErrorCode::SemanticError(format!(
                        "Failed to get StageTable operator for UDF import '{}': {}",
                        location, err
                    ))
                })?;
                if !op.info().full_capability().presign {
                    return Err(ErrorCode::StorageUnsupported(
                        "storage doesn't support presign operation",
                    ));
                }
                op.stat(&path).await.map_err(|err| {
                    ErrorCode::SemanticError(format!(
                        "Failed to stat UDF import '{}': {}",
                        location, err
                    ))
                })?;
                let presigned = op.presign_read(&path, expire).await.map_err(|err| {
                    ErrorCode::SemanticError(format!(
                        "Failed to presign UDF import '{}': {}",
                        location, err
                    ))
                })?;
                let headers = presigned
                    .header()
                    .into_iter()
                    .map(|(name, value)| {
                        let value = value.to_str().map_err(|err| {
                            ErrorCode::SemanticError(format!(
                                "Failed to parse presigned header for UDF import '{}': {err}",
                                location
                            ))
                        })?;
                        Ok((name.as_str().to_string(), value.to_string()))
                    })
                    .collect::<Result<BTreeMap<_, _>>>()?;
                results.push(UdfAsset {
                    location: location.to_string(),
                    url: presigned.uri().to_string(),
                    headers,
                });
            }

            Ok(results)
        })
    }

    fn build_udf_cloud_script(
        &self,
        code: &str,
        handler: &str,
        imports: &[UdfAsset],
        packages: &[String],
        arg_types: &[DataType],
        return_type: &DataType,
    ) -> Result<String> {
        let input_types = arg_types.iter().map(udf_type_string).collect::<Vec<_>>();
        let result_type = udf_type_string(return_type);
        build_udf_cloud_script(code, handler, imports, packages, &input_types, &result_type)
    }

    async fn resolve_udf_with_stage(&mut self, code: String) -> Result<Vec<u8>> {
        let file_location = match code.strip_prefix('@') {
            Some(location) => FileLocation::Stage(location.to_string()),
            None => {
                let uri = UriLocation::from_uri(code.clone(), BTreeMap::default());

                match uri {
                    Ok(uri) => FileLocation::Uri(uri),
                    Err(_) => {
                        // fallback to use the code as real code
                        return Ok(code.into());
                    }
                }
            }
        };

        let (stage_info, module_path) = resolve_file_location(self.ctx.as_ref(), &file_location)
            .await
            .map_err(|err| {
                ErrorCode::SemanticError(format!(
                    "Failed to resolve code location {:?}: {}",
                    code, err
                ))
            })?;

        let op = init_stage_operator(&stage_info).map_err(|err| {
            ErrorCode::SemanticError(format!("Failed to get StageTable operator: {}", err))
        })?;

        let code_blob = op
            .read(&module_path)
            .await
            .map_err(|err| {
                ErrorCode::SemanticError(format!("Failed to read module {}: {}", module_path, err))
            })?
            .to_vec();

        let compress_algo = CompressAlgorithm::from_path(&module_path);
        log::trace!(
            "Detecting compression algorithm for module: {}",
            &module_path
        );
        log::info!("Detected compression algorithm: {:#?}", &compress_algo);

        let code_blob = match compress_algo {
            Some(algo) => {
                log::trace!("Decompressing module using {:?} algorithm", algo);
                if algo == CompressAlgorithm::Zip {
                    DecompressDecoder::decompress_all_zip(
                        &code_blob,
                        &module_path,
                        GLOBAL_MEM_STAT.get_limit() as usize,
                    )
                } else {
                    let mut decoder = DecompressDecoder::new(algo);
                    decoder.decompress_all(&code_blob)
                }
                .map_err(|err| {
                    let error_msg = format!("Failed to decompress module {}: {}", module_path, err);
                    log::error!("{}", error_msg);
                    ErrorCode::SemanticError(error_msg)
                })?
            }
            None => code_blob,
        };

        Ok(code_blob)
    }

    fn resolve_udf_script(
        &mut self,
        span: Span,
        name: String,
        args: &[Expr],
        udf_definition: UDFScript,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let UDFScript {
            code,
            handler,
            language,
            arg_types,
            return_type,
            runtime_version,
            imports,
            packages,
            immutable: _immutable,
        } = udf_definition;

        let language = language.parse()?;
        let use_cloud = matches!(language, UDFLanguage::Python)
            && GlobalConfig::instance().query.common.enable_udf_sandbox;
        if use_cloud {
            UDFValidator::is_udf_cloud_script_allowed(&language)?;
        } else {
            UDFValidator::is_udf_script_allowed(&language)?;
        }
        let mut arguments = Vec::with_capacity(args.len());
        for (argument, dest_type) in args.iter().zip(arg_types.iter()) {
            let box (arg, ty) = self.resolve(argument)?;
            if ty != *dest_type {
                arguments.push(wrap_cast(&arg, dest_type));
            } else {
                arguments.push(arg);
            }
        }

        if use_cloud {
            let code_bytes =
                databend_common_base::runtime::block_on(self.resolve_udf_with_stage(code))?;
            let resolved_code = String::from_utf8(code_bytes).map_err(|err| {
                ErrorCode::SemanticError(format!("Failed to parse UDF code as utf-8: {err}"))
            })?;
            let settings = self.ctx.get_settings();
            let import_assets = self.build_udf_cloud_imports(
                &imports,
                Duration::from_secs(settings.get_udf_cloud_import_presign_expire_secs()?),
            )?;
            let mut merged_packages = extract_script_metadata_deps(&resolved_code);
            merged_packages.extend_from_slice(&packages);
            let script = self.build_udf_cloud_script(
                &resolved_code,
                &handler,
                &import_assets,
                &merged_packages,
                &arg_types,
                &return_type,
            )?;
            let (endpoint, headers) = self.apply_udf_cloud_resource(&name, "udf", script)?;
            let udf_definition = UDFServer {
                address: endpoint,
                handler,
                headers,
                language: language.to_string(),
                arg_names: Vec::new(),
                arg_types,
                return_type,
                immutable: _immutable,
            };
            return self.resolve_udf_server_internal(span, name, args, udf_definition, false);
        }

        let code_blob = databend_common_base::runtime::block_on(self.resolve_udf_with_stage(code))?
            .into_boxed_slice();

        let imports_stage_info = databend_common_base::runtime::block_on(resolve_stage_locations(
            self.ctx.as_ref(),
            &imports
                .iter()
                .map(|s| s.trim_start_matches('@').to_string())
                .collect::<Vec<String>>(),
        ))?;

        let udf_type = UDFType::Script(Box::new(UDFScriptCode {
            language,
            runtime_version,
            code: code_blob.into(),
            imports_stage_info,
            imports,
            packages,
        }));

        let arg_names = args.iter().map(|arg| format!("{arg}")).join(", ");
        let display_name = format!("{}({})", &handler, arg_names);

        self.bind_context.have_udf_script = true;
        self.ctx.set_cacheable(false);
        Ok(Box::new((
            UDFCall {
                span,
                name,
                handler,
                headers: BTreeMap::default(),
                display_name,
                arg_types,
                return_type: Box::new(return_type.clone()),
                udf_type,
                arguments,
            }
            .into(),
            return_type,
        )))
    }

    fn resolve_udaf_script(
        &mut self,
        span: Span,
        name: String,
        args: &[Expr],
        udf_definition: UDAFScript,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let UDAFScript {
            code,
            language,
            arg_types,
            state_fields,
            return_type,
            runtime_version,
            imports,
            packages,
        } = udf_definition;
        let language = language.parse()?;
        let code_blob = databend_common_base::runtime::block_on(self.resolve_udf_with_stage(code))?
            .into_boxed_slice();
        let imports_stage_info = databend_common_base::runtime::block_on(resolve_stage_locations(
            self.ctx.as_ref(),
            &imports
                .iter()
                .map(|s| s.trim_start_matches('@').to_string())
                .collect::<Vec<String>>(),
        ))?;

        let udf_type = UDFType::Script(Box::new(UDFScriptCode {
            language,
            runtime_version,
            code: code_blob.into(),
            imports,
            imports_stage_info,
            packages,
        }));

        let arguments = args
            .iter()
            .zip(arg_types.iter())
            .map(|(argument, dest_type)| {
                let box (arg, ty) = self.resolve(argument)?;
                Ok(if ty == *dest_type {
                    arg
                } else {
                    wrap_cast(&arg, dest_type)
                })
            })
            .collect::<Result<Vec<_>>>()?;

        let display_name = format!(
            "{name}({})",
            args.iter().map(|arg| format!("{:#}", arg)).join(", ")
        );

        self.bind_context.have_udf_script = true;
        self.ctx.set_cacheable(false);
        Ok(Box::new((
            UDAFCall {
                span,
                name,
                display_name,
                arg_types,
                state_fields: state_fields
                    .iter()
                    .map(|f| UDFField {
                        name: f.name().to_string(),
                        data_type: f.data_type().clone(),
                    })
                    .collect(),
                return_type: Box::new(return_type.clone()),
                udf_type,
                arguments,
            }
            .into(),
            return_type,
        )))
    }

    fn resolve_lambda_udf(
        &mut self,
        span: Span,
        func_name: String,
        arguments: &[Expr],
        udf_definition: LambdaUDF,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let parameters = udf_definition.parameters;
        if parameters.len() != arguments.len() {
            return Err(ErrorCode::SyntaxException(format!(
                "Require {} parameters, but got: {}",
                parameters.len(),
                arguments.len()
            ))
            .set_span(span));
        }
        let settings = self.ctx.get_settings();
        let sql_dialect = settings.get_sql_dialect()?;
        let sql_tokens = tokenize_sql(udf_definition.definition.as_str())?;
        let expr = parse_expr(&sql_tokens, sql_dialect)?;
        let mut args_map = HashMap::new();
        arguments.iter().enumerate().for_each(|(idx, argument)| {
            if let Some(parameter) = parameters.get(idx) {
                args_map.insert(parameter.as_str(), (*argument).clone());
            }
        });

        let udf_expr = Self::clone_expr_with_replacement(&expr, |nest_expr| {
            if let Expr::ColumnRef { column, .. } = nest_expr {
                if let Some(arg) = args_map.get(column.column.name()) {
                    return Ok(Some(arg.clone()));
                }
            }
            Ok(None)
        })
        .map_err(|e| e.set_span(span))?;
        let scalar = self.resolve(&udf_expr)?;
        Ok(Box::new((
            UDFLambdaCall {
                span,
                func_name,
                scalar: Box::new(scalar.0),
            }
            .into(),
            scalar.1,
        )))
    }

    fn resolve_scalar_udf(
        &mut self,
        span: Span,
        func_name: String,
        arguments: &[Expr],
        udf_definition: ScalarUDF,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let arg_types = udf_definition.arg_types;
        if arg_types.len() != arguments.len() {
            return Err(ErrorCode::SyntaxException(format!(
                "Require {} parameters, but got: {}",
                arg_types.len(),
                arguments.len()
            ))
            .set_span(span));
        }
        let settings = self.ctx.get_settings();
        let sql_dialect = settings.get_sql_dialect()?;
        let sql_tokens = tokenize_sql(udf_definition.definition.as_str())?;
        let mut udf_expr = parse_expr(&sql_tokens, sql_dialect)?;
        let mut visitor = UDFArgVisitor::new(&arg_types, arguments);
        udf_expr.drive_mut(&mut visitor);

        // Use current binding context so column references inside arguments can be resolved.
        let box (expr, _) = TypeChecker::try_create(
            self.bind_context,
            self.ctx.clone(),
            self.name_resolution_ctx,
            self.metadata.clone(),
            self.aliases,
            self.forbid_udf,
        )?
        .resolve(&udf_expr)?;
        let return_ty = udf_definition.return_type;
        let expr = CastExpr {
            span,
            is_try: false,
            argument: Box::new(expr),
            target_type: Box::new(return_ty.clone()),
        };
        Ok(Box::new((
            UDFLambdaCall {
                span,
                func_name,
                scalar: Box::new(expr.into()),
            }
            .into(),
            return_ty,
        )))
    }

    fn resolve_async_function(
        &mut self,
        span: Span,
        func_name: &str,
        arguments: &[&Expr],
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        if matches!(self.bind_context.expr_context, ExprContext::InAsyncFunction) {
            return Err(
                ErrorCode::SemanticError("async functions cannot be nested".to_string())
                    .set_span(span),
            );
        }
        let original_context = self.bind_context.expr_context.clone();
        self.bind_context
            .set_expr_context(ExprContext::InAsyncFunction);
        let result = match func_name {
            "nextval" => self.resolve_nextval_async_function(span, func_name, arguments)?,
            "dict_get" => self.resolve_dict_get_async_function(span, func_name, arguments)?,
            "read_file" => self.resolve_read_file_async_function(span, func_name, arguments)?,
            _ => {
                return Err(ErrorCode::SemanticError(format!(
                    "cannot find async function {}",
                    func_name
                ))
                .set_span(span));
            }
        };
        // Restore the original context
        self.bind_context.set_expr_context(original_context);
        self.bind_context.have_async_func = true;
        Ok(result)
    }

    fn resolve_nextval_async_function(
        &mut self,
        span: Span,
        func_name: &str,
        arguments: &[&Expr],
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        if arguments.len() != 1 {
            return Err(ErrorCode::SemanticError(format!(
                "nextval function need one argument but got {}",
                arguments.len()
            ))
            .set_span(span));
        }
        let (sequence_name, display_name) = if let Expr::ColumnRef { column, .. } = arguments[0] {
            if column.database.is_some() || column.table.is_some() {
                return Err(ErrorCode::SemanticError(
                    "nextval function argument identifier should only contain one part".to_string(),
                )
                .set_span(span));
            }
            match &column.column {
                ColumnID::Name(ident) => {
                    let ident = normalize_identifier(ident, self.name_resolution_ctx);
                    (ident.name.to_string(), format!("{}({})", func_name, ident))
                }
                ColumnID::Position(pos) => {
                    return Err(ErrorCode::SemanticError(format!(
                        "nextval function argument don't support identifier {}",
                        pos
                    ))
                    .set_span(span));
                }
            }
        } else {
            return Err(ErrorCode::SemanticError(format!(
                "nextval function argument don't support expr {}",
                arguments[0]
            ))
            .set_span(span));
        };

        if !self.skip_sequence_check {
            let catalog = self.ctx.get_default_catalog()?;
            let req = GetSequenceReq {
                ident: SequenceIdent::new(self.ctx.get_tenant(), sequence_name.clone()),
            };

            let visibility_checker = if self
                .ctx
                .get_settings()
                .get_enable_experimental_sequence_privilege_check()?
            {
                Some(databend_common_base::runtime::block_on(async move {
                    self.ctx
                        .get_visibility_checker(false, Object::Sequence)
                        .await
                })?)
            } else {
                None
            };
            databend_common_base::runtime::block_on(
                catalog.get_sequence(req, &visibility_checker),
            )?;
        }

        let return_type = DataType::Number(NumberDataType::UInt64);
        let func_arg = AsyncFunctionArgument::SequenceFunction(sequence_name);

        let async_func = AsyncFunctionCall {
            span,
            func_name: func_name.to_string(),
            display_name,
            return_type: Box::new(return_type.clone()),
            arguments: vec![],
            func_arg,
        };

        Ok(Box::new((async_func.into(), return_type)))
    }

    fn resolve_dict_get_async_function(
        &mut self,
        span: Span,
        func_name: &str,
        args: &[&Expr],
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        if args.len() != 3 {
            return Err(ErrorCode::SemanticError(format!(
                "dict_get function need three arguments but got {}",
                args.len()
            ))
            .set_span(span));
        }
        let tenant = self.ctx.get_tenant();
        let catalog = self.ctx.get_default_catalog()?;

        let dict_name_arg = args[0];
        let field_arg = args[1];
        let key_arg = args[2];

        // Get dict_name and dict_meta.
        let (db_name, dict_name) = if let Expr::ColumnRef { column, .. } = dict_name_arg {
            if column.database.is_some() {
                return Err(ErrorCode::SemanticError(
                    "dict_get function argument identifier should contain one or two parts"
                        .to_string(),
                )
                .set_span(dict_name_arg.span()));
            }
            let db_name = match &column.table {
                Some(ident) => normalize_identifier(ident, self.name_resolution_ctx).name,
                None => self.ctx.get_current_database(),
            };
            let dict_name = match &column.column {
                ColumnID::Name(ident) => normalize_identifier(ident, self.name_resolution_ctx).name,
                ColumnID::Position(pos) => {
                    return Err(ErrorCode::SemanticError(format!(
                        "dict_get function argument don't support identifier {}",
                        pos
                    ))
                    .set_span(dict_name_arg.span()));
                }
            };
            (db_name, dict_name)
        } else {
            return Err(ErrorCode::SemanticError(
                "async function can only used as column".to_string(),
            )
            .set_span(dict_name_arg.span()));
        };
        let db = databend_common_base::runtime::block_on(
            catalog.get_database(&tenant, db_name.as_str()),
        )?;
        let db_id = db.get_db_info().database_id.db_id;
        let req = DictionaryNameIdent::new(
            tenant.clone(),
            DictionaryIdentity::new(db_id, dict_name.clone()),
        );
        let reply = databend_common_base::runtime::block_on(catalog.get_dictionary(req))?;
        let dictionary = if let Some(r) = reply {
            r.dictionary_meta
        } else {
            return Err(ErrorCode::UnknownDictionary(format!(
                "Unknown dictionary {}",
                dict_name,
            )));
        };

        // Get attr_name, attr_type and return_type.
        let box (field_scalar, _field_data_type) = self.resolve(field_arg)?;
        let Ok(field_expr) = ConstantExpr::try_from(field_scalar.clone()) else {
            return Err(ErrorCode::SemanticError(format!(
                "invalid arguments for dict_get function, attr_name must be a constant string, but got {}",
                field_arg
            ))
            .set_span(field_scalar.span()));
        };
        let Some(attr_name) = field_expr.value.as_string() else {
            return Err(ErrorCode::SemanticError(format!(
                "invalid arguments for dict_get function, attr_name must be a constant string, but got {}",
                field_arg
            ))
            .set_span(field_scalar.span()));
        };
        let attr_field = dictionary.schema.field_with_name(attr_name)?;
        let attr_type: DataType = (&attr_field.data_type).into();
        let default_value = DefaultExprBinder::try_new(self.ctx.clone())?.get_scalar(attr_field)?;

        // Get primary_key_value and check type.
        let primary_column_id = dictionary.primary_column_ids[0];
        let primary_field = dictionary.schema.field_of_column_id(primary_column_id)?;
        let primary_type: DataType = (&primary_field.data_type).into();

        let mut args = Vec::with_capacity(1);
        let box (key_scalar, key_type) = self.resolve(key_arg)?;

        if primary_type != key_type.remove_nullable() {
            args.push(wrap_cast(&key_scalar, &primary_type));
        } else {
            args.push(key_scalar);
        }
        let dict_source = match dictionary.source.as_str() {
            "mysql" => {
                let connection_url = dictionary.build_sql_connection_url()?;
                let table = dictionary
                    .options
                    .get("table")
                    .ok_or_else(|| ErrorCode::BadArguments("Miss option `table`"))?;
                DictionarySource::Mysql(SqlSource {
                    connection_url,
                    table: table.to_string(),
                    key_field: primary_field.name.clone(),
                    value_field: attr_field.name.clone(),
                })
            }
            "redis" => {
                let host = dictionary
                    .options
                    .get("host")
                    .ok_or_else(|| ErrorCode::BadArguments("Miss option `host`"))?;
                let port_str = dictionary
                    .options
                    .get("port")
                    .ok_or_else(|| ErrorCode::BadArguments("Miss option `port`"))?;
                let port = port_str
                    .parse()
                    .expect("Failed to parse String port to u16");
                let username = dictionary.options.get("username").cloned();
                let password = dictionary.options.get("password").cloned();
                let db_index = dictionary
                    .options
                    .get("db_index")
                    .map(|i| i.parse::<i64>().unwrap());
                DictionarySource::Redis(RedisSource {
                    host: host.to_string(),
                    port,
                    username,
                    password,
                    db_index,
                })
            }
            _ => {
                return Err(ErrorCode::Unimplemented(format!(
                    "Unsupported source {}",
                    dictionary.source
                )));
            }
        };

        let dict_get_func_arg = DictGetFunctionArgument {
            dict_source,
            default_value,
        };
        let display_name = format!(
            "{}({}.{}, {}, {})",
            func_name, db_name, dict_name, field_arg, key_arg,
        );
        Ok(Box::new((
            ScalarExpr::AsyncFunctionCall(AsyncFunctionCall {
                span,
                func_name: func_name.to_string(),
                display_name,
                return_type: Box::new(attr_type.clone()),
                arguments: args,
                func_arg: AsyncFunctionArgument::DictGetFunction(dict_get_func_arg),
            }),
            attr_type,
        )))
    }

    fn resolve_read_file_async_function(
        &mut self,
        span: Span,
        func_name: &str,
        args: &[&Expr],
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        if args.len() != 1 && args.len() != 2 {
            return Err(ErrorCode::SemanticError(format!(
                "read_file function need one or two arguments but got {}",
                args.len()
            ))
            .set_span(span));
        }

        let mut resolved_args = Vec::with_capacity(args.len());
        let mut arg_types = Vec::with_capacity(args.len());
        for arg in args {
            let box (arg_scalar, arg_type) = self.resolve(arg)?;
            let arg_scalar = if arg_type.remove_nullable() != DataType::String {
                wrap_cast(&arg_scalar, &DataType::String)
            } else {
                arg_scalar
            };
            resolved_args.push(arg_scalar);
            arg_types.push(arg_type);
        }

        let mut read_file_arg = ReadFileFunctionArgument {
            stage_name: None,
            stage_info: None,
        };

        if args.len() == 1 {
            if let ScalarExpr::ConstantExpr(constant) = &resolved_args[0] {
                if let Some(location) = constant.value.as_string() {
                    if !location.starts_with('@') {
                        return Err(ErrorCode::SemanticError(format!(
                            "stage path must start with @, but got {}",
                            location
                        ))
                        .set_span(span));
                    }
                }
            }
        } else if let ScalarExpr::ConstantExpr(constant) = &resolved_args[0] {
            if let Some(stage) = constant.value.as_string() {
                let stage_name = parse_stage_name(stage).map_err(|err| {
                    ErrorCode::SemanticError(err.message().to_string()).set_span(span)
                })?;
                let stage_info =
                    Self::resolve_stage_info_for_read_file(self.ctx.as_ref(), span, &stage_name)?;
                read_file_arg.stage_name = Some(stage_name);
                read_file_arg.stage_info = Some(Box::new(stage_info));
            }
        }

        let return_type = if arg_types
            .iter()
            .any(|arg_type| arg_type.is_nullable_or_null())
        {
            DataType::Nullable(Box::new(DataType::Binary))
        } else {
            DataType::Binary
        };

        let display_name = if args.len() == 1 {
            format!("{}({})", func_name, args[0])
        } else {
            format!("{}({}, {})", func_name, args[0], args[1])
        };
        Ok(Box::new((
            ScalarExpr::AsyncFunctionCall(AsyncFunctionCall {
                span,
                func_name: func_name.to_string(),
                display_name,
                return_type: Box::new(return_type.clone()),
                arguments: resolved_args,
                func_arg: AsyncFunctionArgument::ReadFile(read_file_arg),
            }),
            return_type,
        )))
    }

    fn resolve_stage_info_for_read_file(
        ctx: &dyn TableContext,
        span: Span,
        stage_name: &str,
    ) -> Result<StageInfo> {
        databend_common_base::runtime::block_on(async move {
            let (stage_info, _) = resolve_stage_location(ctx, stage_name).await?;
            if ctx.get_settings().get_enable_experimental_rbac_check()? {
                let visibility_checker = ctx.get_visibility_checker(false, Object::Stage).await?;
                if !(stage_info.is_temporary
                    || visibility_checker.check_stage_read_visibility(&stage_info.stage_name)
                    || stage_info.stage_type == StageType::User
                        && stage_info.stage_name == ctx.get_current_user()?.name)
                {
                    return Err(ErrorCode::PermissionDenied(format!(
                        "Permission denied: privilege READ is required on stage {} for user {}",
                        stage_info.stage_name.clone(),
                        &ctx.get_current_user()?.identity().display(),
                    ))
                    .set_span(span));
                }
            }
            Ok(stage_info)
        })
    }

    fn resolve_cast_to_variant(
        &mut self,
        span: Span,
        source_type: &DataType,
        scalar: &ScalarExpr,
        is_try: bool,
    ) -> Option<Result<Box<(ScalarExpr, DataType)>>> {
        if !matches!(source_type.remove_nullable(), DataType::Tuple(_)) {
            return None;
        }
        // If the type of source column is a tuple, rewrite to json_object_keep_null function,
        // using the name of tuple inner fields as the object name.
        if let ScalarExpr::BoundColumnRef(BoundColumnRef { column, .. }) = scalar {
            let column_entry = self.metadata.read().column(column.index).clone();
            if let ColumnEntry::BaseTableColumn(BaseTableColumn { data_type, .. }) = column_entry {
                let new_scalar = Self::rewrite_cast_to_variant(span, scalar, &data_type, is_try);
                let return_type = if is_try || source_type.is_nullable() {
                    DataType::Nullable(Box::new(DataType::Variant))
                } else {
                    DataType::Variant
                };
                return Some(Ok(Box::new((new_scalar, return_type))));
            }
        }
        None
    }

    fn rewrite_cast_to_variant(
        span: Span,
        scalar: &ScalarExpr,
        data_type: &TableDataType,
        is_try: bool,
    ) -> ScalarExpr {
        match data_type.remove_nullable() {
            TableDataType::Tuple {
                fields_name,
                fields_type,
            } => {
                let mut args = Vec::with_capacity(fields_name.len() * 2);
                for ((idx, field_name), field_type) in
                    fields_name.iter().enumerate().zip(fields_type.iter())
                {
                    let key = ConstantExpr {
                        span,
                        value: Scalar::String(field_name.clone()),
                    }
                    .into();

                    let value = FunctionCall {
                        span,
                        params: vec![Scalar::Number(NumberScalar::Int64((idx + 1) as i64))],
                        arguments: vec![scalar.clone()],
                        func_name: "get".to_string(),
                    }
                    .into();

                    let value =
                        if matches!(field_type.remove_nullable(), TableDataType::Tuple { .. }) {
                            Self::rewrite_cast_to_variant(span, &value, field_type, is_try)
                        } else {
                            value
                        };

                    args.push(key);
                    args.push(value);
                }
                let func_name = if is_try {
                    "try_json_object_keep_null".to_string()
                } else {
                    "json_object_keep_null".to_string()
                };
                FunctionCall {
                    span,
                    params: vec![],
                    arguments: args,
                    func_name,
                }
                .into()
            }
            _ => {
                let func_name = if is_try {
                    "try_to_variant".to_string()
                } else {
                    "to_variant".to_string()
                };
                FunctionCall {
                    span,
                    params: vec![],
                    arguments: vec![scalar.clone()],
                    func_name,
                }
                .into()
            }
        }
    }

    fn resolve_map_access(
        &mut self,
        span: Span,
        expr: &Expr,
        mut paths: VecDeque<(Span, Literal)>,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let box (mut scalar, data_type) = self.resolve(expr)?;
        // Variant type can be converted to `get_by_keypath` function.
        if data_type.remove_nullable() == DataType::Variant {
            return self.resolve_variant_map_access(span, scalar, &mut paths);
        }

        let mut table_data_type = infer_schema_type(&data_type)?;
        // If it is a tuple column, convert it to the internal column specified by the paths.
        // For other types of columns, convert it to get functions.
        if let ScalarExpr::BoundColumnRef(BoundColumnRef { ref column, .. }) = scalar {
            if column.index < self.metadata.read().columns().len() {
                let column_entry = self.metadata.read().column(column.index).clone();
                if let ColumnEntry::BaseTableColumn(BaseTableColumn { ref data_type, .. }) =
                    column_entry
                {
                    // Use data type from meta to get the field names of tuple type.
                    table_data_type = data_type.clone();
                    if let TableDataType::Tuple { .. } = table_data_type.remove_nullable() {
                        let box (inner_scalar, _inner_data_type) = self
                            .resolve_tuple_map_access_pushdown(
                                expr.span(),
                                column.clone(),
                                &mut table_data_type,
                                &mut paths,
                            )?;
                        scalar = inner_scalar;
                    }
                }
            }
        }

        // Otherwise, desugar it into a `get` function.
        while let Some((span, path_lit)) = paths.pop_front() {
            table_data_type = table_data_type.remove_nullable();
            if let TableDataType::Tuple {
                fields_name,
                fields_type,
            } = table_data_type
            {
                let idx = match path_lit {
                    Literal::UInt64(idx) => {
                        if idx == 0 {
                            return Err(ErrorCode::SemanticError(
                                "tuple index is starting from 1, but 0 is found".to_string(),
                            ));
                        }
                        if idx as usize > fields_type.len() {
                            return Err(ErrorCode::SemanticError(format!(
                                "tuple index {} is out of bounds for length {}",
                                idx,
                                fields_type.len()
                            )));
                        }
                        (idx - 1) as usize
                    }
                    Literal::String(name) => match fields_name.iter().position(|k| k == &name) {
                        Some(idx) => idx,
                        None => {
                            return Err(ErrorCode::SemanticError(format!(
                                "tuple name `{}` does not exist, available names are: {:?}",
                                name, &fields_name
                            )));
                        }
                    },
                    _ => unreachable!(),
                };
                table_data_type = fields_type.get(idx).unwrap().clone();
                scalar = FunctionCall {
                    span: expr.span(),
                    func_name: "get".to_string(),
                    params: vec![Scalar::Number(NumberScalar::Int64((idx + 1) as i64))],
                    arguments: vec![scalar.clone()],
                }
                .into();
                continue;
            }
            let box (path_scalar, _) = self.resolve_literal(span, &path_lit)?;
            if let TableDataType::Array(inner_type) = table_data_type {
                table_data_type = *inner_type;
            }
            table_data_type = table_data_type.wrap_nullable();
            scalar = FunctionCall {
                span: path_scalar.span(),
                func_name: "get".to_string(),
                params: vec![],
                arguments: vec![scalar.clone(), path_scalar],
            }
            .into();
        }
        let return_type = scalar.data_type()?;
        Ok(Box::new((scalar, return_type)))
    }

    fn resolve_tuple_map_access_pushdown(
        &mut self,
        span: Span,
        column: ColumnBinding,
        table_data_type: &mut TableDataType,
        paths: &mut VecDeque<(Span, Literal)>,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let mut names = Vec::new();
        names.push(column.column_name.clone());
        let mut index_with_types = VecDeque::with_capacity(paths.len());
        while paths.front().is_some() {
            if let TableDataType::Tuple {
                fields_name,
                fields_type,
            } = table_data_type.remove_nullable()
            {
                let (span, path) = paths.pop_front().unwrap();
                let idx = match path {
                    Literal::UInt64(idx) => {
                        if idx == 0 {
                            return Err(ErrorCode::SemanticError(
                                "tuple index is starting from 1, but 0 is found".to_string(),
                            )
                            .set_span(span));
                        }
                        if idx as usize > fields_type.len() {
                            return Err(ErrorCode::SemanticError(format!(
                                "tuple index {} is out of bounds for length {}",
                                idx,
                                fields_type.len()
                            ))
                            .set_span(span));
                        }
                        idx as usize - 1
                    }
                    Literal::String(name) => match fields_name.iter().position(|k| k == &name) {
                        Some(idx) => idx,
                        None => {
                            return Err(ErrorCode::SemanticError(format!(
                                "tuple name `{}` does not exist, available names are: {:?}",
                                name, &fields_name
                            ))
                            .set_span(span));
                        }
                    },
                    _ => unreachable!(),
                };
                let inner_field_name = fields_name.get(idx).unwrap();
                let inner_name = display_tuple_field_name(inner_field_name);
                names.push(inner_name);
                let inner_type = fields_type.get(idx).unwrap();
                index_with_types.push_back((idx + 1, inner_type.clone()));
                *table_data_type = inner_type.clone();
            } else {
                // other data types use `get` function.
                break;
            };
        }

        let inner_column_ident = Identifier::from_name(span, names.join(":"));
        match self.bind_context.resolve_name(
            column.database_name.as_deref(),
            column.table_name.as_deref(),
            &inner_column_ident,
            self.aliases,
            self.name_resolution_ctx,
        ) {
            Ok(result) => {
                let (scalar, data_type) = match result {
                    NameResolutionResult::Column(column) => {
                        let data_type = *column.data_type.clone();
                        (BoundColumnRef { span, column }.into(), data_type)
                    }
                    _ => unreachable!(),
                };
                Ok(Box::new((scalar, data_type)))
            }
            Err(_) => {
                // inner column is not exist in view, desugar it into a `get` function.
                let mut scalar: ScalarExpr = BoundColumnRef { span, column }.into();
                while let Some((idx, table_data_type)) = index_with_types.pop_front() {
                    scalar = FunctionCall {
                        span,
                        params: vec![Scalar::Number(NumberScalar::Int64(idx as i64))],
                        arguments: vec![scalar.clone()],
                        func_name: "get".to_string(),
                    }
                    .into();
                    scalar = wrap_cast(&scalar, &DataType::from(&table_data_type));
                }
                let return_type = scalar.data_type()?;
                Ok(Box::new((scalar, return_type)))
            }
        }
    }

    fn convert_inlist_to_subquery(
        &mut self,
        expr: &Expr,
        list: &[Expr],
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let mut bind_context = BindContext::with_parent(self.bind_context.clone())?;
        let mut values = Vec::with_capacity(list.len());
        for val in list.iter() {
            values.push(vec![val.clone()])
        }
        let (const_scan, ctx) = bind_values(
            self.ctx.clone(),
            self.name_resolution_ctx,
            self.metadata.clone(),
            &mut bind_context,
            None,
            &values,
            None,
        )?;

        assert_eq!(ctx.columns.len(), 1);
        // Wrap group by on `const_scan` to deduplicate values
        let distinct_const_scan = SExpr::create_unary(
            Arc::new(
                Aggregate {
                    mode: AggregateMode::Initial,
                    group_items: vec![ScalarItem {
                        scalar: ScalarExpr::BoundColumnRef(BoundColumnRef {
                            span: None,
                            column: ctx.columns[0].clone(),
                        }),
                        index: self.metadata.read().columns().len() - 1,
                    }],
                    ..Default::default()
                }
                .into(),
            ),
            Arc::new(const_scan),
        );

        let box mut data_type = ctx.columns[0].data_type.clone();
        let rel_expr = RelExpr::with_s_expr(&distinct_const_scan);
        let rel_prop = rel_expr.derive_relational_prop()?;
        let box (scalar, expr_ty) = self.resolve(expr)?;
        // wrap nullable to make sure expr and list values have common type.
        if expr_ty.is_nullable() {
            data_type = data_type.wrap_nullable();
        }
        let child_scalar = Some(Box::new(scalar));
        let subquery_expr = SubqueryExpr {
            span: None,
            subquery: Box::new(distinct_const_scan),
            child_expr: child_scalar,
            compare_op: Some(SubqueryComparisonOp::Equal),
            output_column: ctx.columns[0].clone(),
            projection_index: None,
            data_type: Box::new(data_type),
            typ: SubqueryType::Any,
            outer_columns: rel_prop.outer_columns.clone(),
            contain_agg: None,
        };
        let data_type = subquery_expr.output_data_type();
        Ok(Box::new((subquery_expr.into(), data_type)))
    }

    fn try_rewrite_virtual_column(
        &mut self,
        span: Span,
        table_index: IndexType,
        column_id: ColumnId,
        column_name: &str,
        keypaths: &KeyPaths,
    ) -> Option<Box<(ScalarExpr, DataType)>> {
        if !self.bind_context.allow_virtual_column {
            return None;
        }
        let owned_keypaths = keypaths.to_owned();
        let key_name = Self::keypaths_to_name(column_name, keypaths);
        let virtual_column_name = VirtualColumnName {
            table_index,
            source_column_id: column_id,
            key_name,
        };

        // add virtual column binding into `BindContext`
        let column = self.bind_context.add_virtual_column_binding(
            self.metadata.clone(),
            column_name,
            virtual_column_name,
            owned_keypaths,
        )?;

        let data_type = *column.data_type.clone();
        Some(Box::new((
            BoundColumnRef { span, column }.into(),
            data_type,
        )))
    }

    fn keypaths_to_name(column_name: &str, keypaths: &KeyPaths) -> String {
        let mut name = column_name.to_string();
        for path in &keypaths.paths {
            name.push('[');
            match path {
                KeyPath::Index(idx) => {
                    name.push_str(&idx.to_string());
                }
                KeyPath::QuotedName(field) | KeyPath::Name(field) => {
                    name.push('\'');
                    name.push_str(field.as_ref());
                    name.push('\'');
                }
            }
            name.push(']');
        }
        name
    }

    // Rewrite variant map access as `get_by_keypath` function
    fn resolve_variant_map_access(
        &mut self,
        span: Span,
        scalar: ScalarExpr,
        paths: &mut VecDeque<(Span, Literal)>,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let mut key_paths = Vec::with_capacity(paths.len());
        for (span, path) in paths.iter() {
            let key_path = match path {
                Literal::UInt64(idx) => {
                    if let Ok(i) = i32::try_from(*idx) {
                        KeyPath::Index(i)
                    } else {
                        return Err(ErrorCode::SemanticError(format!(
                            "path index is overflow, max allowed value is {}, but got {}",
                            i32::MAX,
                            idx
                        ))
                        .set_span(*span));
                    }
                }
                Literal::String(field) => KeyPath::QuotedName(std::borrow::Cow::Borrowed(field)),
                _ => unreachable!(),
            };
            key_paths.push(key_path);
        }

        let keypaths = KeyPaths { paths: key_paths };

        // try rewrite as virtual column and pushdown to storage layer.
        if let ScalarExpr::BoundColumnRef(BoundColumnRef { ref column, .. }) = scalar {
            if column.index < self.metadata.read().columns().len() {
                let column_entry = self.metadata.read().column(column.index).clone();
                if let ColumnEntry::BaseTableColumn(base_column) = column_entry {
                    if let Some(box (scalar, data_type)) = self.try_rewrite_virtual_column(
                        span,
                        base_column.table_index,
                        base_column.column_id,
                        &base_column.column_name,
                        &keypaths,
                    ) {
                        return Ok(Box::new((scalar, data_type)));
                    }
                }
            }
        }

        let keypaths_str = format!("{}", keypaths);
        let path_scalar = ScalarExpr::ConstantExpr(ConstantExpr {
            span: None,
            value: Scalar::String(keypaths_str),
        });
        let args = vec![scalar, path_scalar];

        Ok(Box::new((
            ScalarExpr::FunctionCall(FunctionCall {
                span: None,
                func_name: "get_by_keypath".to_string(),
                params: vec![],
                arguments: args,
            }),
            DataType::Nullable(Box::new(DataType::Variant)),
        )))
    }

    fn resolve_stage_location(
        &mut self,
        span: Span,
        location: &str,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        Ok(Box::new((
            ScalarExpr::ConstantExpr(ConstantExpr {
                span,
                value: Scalar::String(location.to_string()),
            }),
            DataType::String,
        )))
    }

    #[allow(clippy::only_used_in_recursion)]
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

    fn try_fold_constant<Index: ColumnIndex>(
        &self,
        expr: &EExpr<Index>,
        enable_shrink: bool,
    ) -> Option<Box<(ScalarExpr, DataType)>> {
        if expr.is_deterministic(&BUILTIN_FUNCTIONS) && enable_shrink {
            if let (EExpr::Constant(expr::Constant { scalar, .. }), _) =
                ConstantFolder::fold(expr, &self.func_ctx, &BUILTIN_FUNCTIONS)
            {
                let scalar = if enable_shrink {
                    shrink_scalar(scalar)
                } else {
                    scalar
                };
                let ty = scalar.as_ref().infer_data_type();
                return Some(Box::new((
                    ConstantExpr {
                        span: expr.span(),
                        value: scalar,
                    }
                    .into(),
                    ty,
                )));
            }
        }

        None
    }
}

pub fn resolve_type_name_by_str(name: &str, not_null: bool) -> Result<TableDataType> {
    let sql_tokens = databend_common_ast::parser::tokenize_sql(name)?;
    let ast = databend_common_ast::parser::run_parser(
        &sql_tokens,
        databend_common_ast::parser::Dialect::default(),
        databend_common_ast::parser::ParseMode::Default,
        false,
        databend_common_ast::parser::expr::type_name,
    )?;
    resolve_type_name(&ast, not_null)
}

pub fn resolve_type_name(type_name: &TypeName, not_null: bool) -> Result<TableDataType> {
    let data_type = match type_name {
        TypeName::Boolean => TableDataType::Boolean,
        TypeName::UInt8 => TableDataType::Number(NumberDataType::UInt8),
        TypeName::UInt16 => TableDataType::Number(NumberDataType::UInt16),
        TypeName::UInt32 => TableDataType::Number(NumberDataType::UInt32),
        TypeName::UInt64 => TableDataType::Number(NumberDataType::UInt64),
        TypeName::Int8 => TableDataType::Number(NumberDataType::Int8),
        TypeName::Int16 => TableDataType::Number(NumberDataType::Int16),
        TypeName::Int32 => TableDataType::Number(NumberDataType::Int32),
        TypeName::Int64 => TableDataType::Number(NumberDataType::Int64),
        TypeName::Float32 => TableDataType::Number(NumberDataType::Float32),
        TypeName::Float64 => TableDataType::Number(NumberDataType::Float64),
        TypeName::Decimal { precision, scale } => {
            TableDataType::Decimal(DecimalSize::new(*precision, *scale)?.into())
        }
        TypeName::Binary => TableDataType::Binary,
        TypeName::String => TableDataType::String,
        TypeName::Timestamp => TableDataType::Timestamp,
        TypeName::Date => TableDataType::Date,
        TypeName::Array(item_type) => {
            TableDataType::Array(Box::new(resolve_type_name(item_type, not_null)?))
        }
        TypeName::Map { key_type, val_type } => {
            let key_type = resolve_type_name(key_type, true)?;
            match key_type {
                TableDataType::Boolean
                | TableDataType::String
                | TableDataType::Number(_)
                | TableDataType::Decimal(_)
                | TableDataType::Timestamp
                | TableDataType::Date => {
                    let val_type = resolve_type_name(val_type, not_null)?;
                    let inner_type = TableDataType::Tuple {
                        fields_name: vec!["key".to_string(), "value".to_string()],
                        fields_type: vec![key_type, val_type],
                    };
                    TableDataType::Map(Box::new(inner_type))
                }
                _ => {
                    return Err(ErrorCode::BadArguments(format!(
                        "Invalid Map key type \'{:?}\'",
                        key_type
                    )));
                }
            }
        }
        TypeName::Bitmap => TableDataType::Bitmap,
        TypeName::Interval => TableDataType::Interval,
        TypeName::Tuple {
            fields_type,
            fields_name,
        } => TableDataType::Tuple {
            fields_name: match fields_name {
                None => (0..fields_type.len())
                    .map(|i| (i + 1).to_string())
                    .collect(),
                Some(names) => names
                    .iter()
                    .map(|i| {
                        if i.is_quoted() {
                            i.name.clone()
                        } else {
                            i.name.to_lowercase()
                        }
                    })
                    .collect(),
            },
            fields_type: fields_type
                .iter()
                .map(|item_type| resolve_type_name(item_type, not_null))
                .collect::<Result<Vec<_>>>()?,
        },
        TypeName::Nullable(inner_type) => {
            let data_type = resolve_type_name(inner_type, not_null)?;
            data_type.wrap_nullable()
        }
        TypeName::Variant => TableDataType::Variant,
        TypeName::Geometry => TableDataType::Geometry,
        TypeName::Geography => TableDataType::Geography,
        TypeName::Vector(dimension) => {
            if *dimension == 0 || *dimension > 4096 {
                return Err(ErrorCode::BadArguments(format!(
                    "Invalid vector dimension '{}'",
                    dimension
                )));
            }
            // Only support float32 type currently.
            TableDataType::Vector(VectorDataType::Float32(*dimension))
        }
        TypeName::NotNull(inner_type) => {
            let data_type = resolve_type_name(inner_type, not_null)?;
            data_type.remove_nullable()
        }
        TypeName::StageLocation => TableDataType::StageLocation,
        TypeName::TimestampTz => TableDataType::TimestampTz,
    };
    if !matches!(type_name, TypeName::Nullable(_) | TypeName::NotNull(_)) && !not_null {
        return Ok(data_type.wrap_nullable());
    }
    Ok(data_type)
}

pub fn resolve_type_name_udf(type_name: &TypeName) -> Result<TableDataType> {
    let type_name = match type_name {
        name @ TypeName::Nullable(_) | name @ TypeName::NotNull(_) => name,
        name => &name.clone().wrap_nullable(),
    };
    resolve_type_name(type_name, true)
}

pub fn validate_function_arg(
    name: &str,
    args_len: usize,
    variadic_arguments: Option<(usize, usize)>,
    num_arguments: usize,
) -> Result<()> {
    match variadic_arguments {
        Some((start, end)) => {
            if args_len < start || args_len > end {
                Err(ErrorCode::NumberArgumentsNotMatch(format!(
                    "Function `{}` expect to have [{}, {}] arguments, but got {}",
                    name, start, end, args_len
                )))
            } else {
                Ok(())
            }
        }
        None => {
            if num_arguments != args_len {
                Err(ErrorCode::NumberArgumentsNotMatch(format!(
                    "Function `{}` expect to have {} arguments, but got {}",
                    name, num_arguments, args_len
                )))
            } else {
                Ok(())
            }
        }
    }
}

// optimize special cases for like expression
fn check_percent(like_str: &str) -> bool {
    !like_str.is_empty() && like_str.chars().all(|c| c == '%')
}

// Some check functions for like expression
fn check_const(like_str: &str) -> bool {
    for char in like_str.chars() {
        if char == '_' || char == '%' {
            return false;
        }
    }
    true
}

fn check_prefix(like_str: &str) -> bool {
    if like_str.contains("\\%") {
        return false;
    }
    if like_str.len() == 1 && matches!(like_str, "%" | "_") {
        return false;
    }
    if like_str.chars().filter(|c| *c == '%').count() != 1 {
        return false;
    }

    let mut i: usize = like_str.len();
    while i > 0 {
        if let Some(c) = like_str.chars().nth(i - 1) {
            if c != '%' {
                break;
            }
        } else {
            return false;
        }
        i -= 1;
    }
    if i == like_str.len() {
        return false;
    }
    for j in (0..i).rev() {
        if let Some(c) = like_str.chars().nth(j) {
            if c == '_' {
                return false;
            }
        } else {
            return false;
        }
    }
    true
}

// If `InList` expr satisfies the following conditions, it can be converted to `contain` function
// Note: the method mainly checks if list contains NULL literal, because `contain` can't handle NULL.
fn satisfy_contain_func(expr: &Expr) -> bool {
    match expr {
        Expr::Literal { value, .. } => !matches!(value, Literal::Null),
        Expr::Tuple { exprs, .. } => {
            // For each expr in `exprs`, check if it satisfies the conditions
            exprs.iter().all(satisfy_contain_func)
        }
        Expr::Array { exprs, .. } => exprs.iter().all(satisfy_contain_func),
        // FIXME: others expr won't exist in `InList` expr
        _ => false,
    }
}

impl<'a> TypeChecker<'a> {
    /// Get masking policy expression for a column reference
    /// This is the ONLY place where masking policy is applied - unifying all paths (SELECT/WHERE/HAVING)
    async fn get_masking_policy_expr_for_column(
        &self,
        column_binding: &crate::ColumnBinding,
        database: Option<&str>,
        table: Option<&str>,
    ) -> Result<Option<Expr>> {
        use databend_common_ast::ast;
        use databend_common_license::license::Feature::DataMask;
        use databend_common_license::license_manager::LicenseManagerSwitch;
        use databend_common_users::UserApiProvider;
        use databend_enterprise_data_mask_feature::get_datamask_handler;

        // Check if this column has a masking policy
        if let Some(table_index) = column_binding.table_index {
            // Extract all needed data before the await point to avoid holding the mutex lock
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
                    if let Some(policy_info) = table_info_ref
                        .meta
                        .column_mask_policy_columns_ids
                        .get(&field.column_id)
                    {
                        // Check license
                        if LicenseManagerSwitch::instance()
                            .check_enterprise_enabled(self.ctx.get_license_key(), DataMask)
                            .is_err()
                        {
                            return Ok(None);
                        }

                        // Extract data needed after await
                        Some((
                            policy_info.policy_id,
                            policy_info.columns_ids.clone(),
                            table_schema,
                        ))
                    } else {
                        None
                    }
                } else {
                    None
                }
            }; // metadata lock is released here

            if let Some((policy_id, using_columns, table_schema)) = policy_data {
                let tenant = self.ctx.get_tenant();
                let meta_api = UserApiProvider::instance().get_meta_store_client();
                let handler = get_datamask_handler();

                // Get the policy (now safe to await without holding the lock)
                match handler
                    .get_data_mask_by_id(meta_api.clone(), &tenant, policy_id)
                    .await
                {
                    Ok(policy) => {
                        let policy = policy.data;
                        let body = &policy.body;
                        let args = &policy.args;

                        // Parse the policy body
                        let tokens = tokenize_sql(body)?;
                        let settings = self.ctx.get_settings();
                        let ast_expr = parse_expr(&tokens, settings.get_sql_dialect()?)?;

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
                        let expr = Self::clone_expr_with_replacement(&ast_expr, |nest_expr| {
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
                    Err(err) => {
                        // Error when masking policy cannot be loaded
                        return Err(ErrorCode::UnknownMaskPolicy(format!(
                            "Failed to load masking policy (id: {}) for column '{}': {}. Query denied to prevent potential data leakage. Please verify the policy still exists and meta service is available",
                            policy_id, column_binding.column_name, err
                        )));
                    }
                }
            }
        }
        Ok(None)
    }
}
