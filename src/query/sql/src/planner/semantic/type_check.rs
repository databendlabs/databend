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

use std::collections::VecDeque;
use std::sync::Arc;

use databend_common_ast::Span;
use databend_common_ast::ast::ColumnRef;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::Literal;
use databend_common_ast::ast::Query;
use databend_common_ast::ast::TypeName;
use databend_common_ast::parser::Dialect;
use databend_common_catalog::catalog::CatalogManager;
use databend_common_catalog::table_context::TableContext;
use databend_common_cloud_control::cloud_api::CloudControlApiProvider;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::types::DataType;
use databend_common_functions::aggregates::AggregateFunctionFactory;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_meta_app::principal::StageInfo;
use databend_common_meta_app::principal::UDFScript;
use databend_common_meta_app::principal::UDFServer;
use databend_common_meta_app::principal::UserDefinedFunction;
use databend_common_settings::Settings;
use databend_common_users::UserApiProvider;
use databend_common_users::security_policy_cache::CachedSecurityPolicy;
use databend_common_users::security_policy_cache::SecurityPolicyCacheManager;
use smallvec::SmallVec;
use tokio::runtime::Handle;

use super::name_resolution::NameResolutionContext;
use crate::BindContext;
use crate::MetadataRef;
use crate::optimizer::ir::SExpr;
use crate::planner::expression::UdfValidationConfig;
use crate::plans::DictGetFunctionArgument;
use crate::plans::ScalarExpr;

const DEFAULT_DECIMAL_PRECISION: i64 = 38;
const DEFAULT_DECIMAL_SCALE: i64 = 0;

mod adapter;
mod aggregate;
mod async_functions;
mod column;
mod core_expr;
mod date;
mod in_list;
mod lambda;
mod literal;
mod resolve;
mod rewrite_function;
mod scalar_function;
mod scalar_rewrite;
mod search;
mod set_returning;
mod special_function;
mod string;
mod subquery;
mod udf;
mod variant;
mod vector;
mod window;

#[derive(Clone, Copy)]
struct CoreExprId {
    index: usize,
}

type CoreExprArgs = SmallVec<[CoreExprId; 4]>;
type CoreMapEntries = SmallVec<[(Literal, CoreExprId); 4]>;
type CoreFunctionParams = SmallVec<[(String, CoreExprId); 4]>;
type CoreOrderByExprs = SmallVec<[CoreOrderByExpr; 4]>;
type CoreDisplayExprArg = (String, CoreExprId);
type CoreDisplayExprArgs = SmallVec<[CoreDisplayExprArg; 4]>;
type CoreUdfCallArgs = SmallVec<[(String, CoreExprId); 4]>;

pub struct CoreExprArena<'a> {
    nodes: Vec<CoreExpr<'a>>,
    week_start: u64,
    pub(super) aggregate_function_factory: &'static AggregateFunctionFactory,
    pub(super) in_lambda_function: bool,
}

enum CoreExpr<'a> {
    ColumnRef {
        span: Span,
        column: &'a ColumnRef,
    },
    Literal {
        span: Span,
        value: Scalar,
    },
    Array {
        span: Span,
        exprs: CoreExprArgs,
    },
    Map {
        span: Span,
        kvs: CoreMapEntries,
    },
    Tuple {
        span: Span,
        exprs: CoreExprArgs,
    },
    MapAccess {
        span: Span,
        expr_span: Span,
        expr: CoreExprId,
        paths: VecDeque<(Span, Literal)>,
    },
    Call {
        span: Span,
        func_name: &'static str,
        args: CoreExprArgs,
    },
    UdfCall {
        span: Span,
        name: &'a Identifier,
        args: CoreUdfCallArgs,
        // Carried through to UDF resolution, where scalar UDFs and UDAFs are
        // distinguished before a FILTER clause can be handled or rejected.
        filter: Option<&'a Expr>,
    },
    LambdaFunction {
        span: Span,
        func_name: &'static str,
        args: CoreExprArgs,
        lambda_params: &'a [Identifier],
        lambda_expr: CoreExprId,
    },
    SearchFunction {
        span: Span,
        function: search::CoreSearchFunction,
    },
    AsyncFunction {
        span: Span,
        function: async_functions::CoreAsyncFunction<'a>,
    },
    SetReturningFunction {
        span: Span,
        func_name: &'static str,
        args: CoreExprArgs,
    },
    ScalarFunction {
        span: Span,
        func_name: &'static str,
        params: CoreFunctionParams,
        args: CoreExprArgs,
    },
    InList {
        span: Span,
        expr: CoreExprId,
        list: CoreExprArgs,
        not: bool,
    },
    Subquery {
        span: Span,
        subquery: &'a Query,
        typ: crate::plans::SubqueryType,
        child_expr: Option<CoreExprId>,
        compare_op: Option<crate::plans::SubqueryComparisonOp>,
    },
    Cast {
        span: Span,
        is_try: bool,
        expr: CoreExprId,
        target_type: TypeName,
    },
    SpecialFunction {
        span: Span,
        function: special_function::SpecialFunction,
    },
    AggregateFunction {
        display_name: String,
        span: Span,
        func_name: String,
        distinct: bool,
        params: CoreFunctionParams,
        args: CoreExprArgs,
        remove_count_args: bool,
        order_by: CoreOrderByExprs,
    },
    AggregateWindowFunction {
        display_name: String,
        span: Span,
        func_name: String,
        distinct: bool,
        params: CoreFunctionParams,
        args: CoreExprArgs,
        remove_count_args: bool,
        order_by: CoreOrderByExprs,
        window: window::CoreWindowDesc<'a>,
    },
    CountAllWindowFunction {
        display_name: String,
        span: Span,
        window: window::CoreWindow<'a>,
    },
    GeneralWindowFunction {
        display_name: String,
        span: Span,
        func_name: &'static str,
        args: CoreExprArgs,
        order_by: CoreOrderByExprs,
        window: window::CoreWindowDesc<'a>,
    },
    StageLocation {
        span: Span,
        location: &'a str,
    },
}

struct CoreOrderByExpr {
    pub(super) expr: CoreExprId,
    pub(super) asc: Option<bool>,
    pub(super) nulls_first: Option<bool>,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub struct StageLocationParam {
    pub param_name: String,
    pub relative_path: String,
    pub stage_info: StageInfo,
}

#[derive(Clone, Copy)]
pub enum NamespaceFunction {
    CurrentCatalog,
    CurrentDatabase,
}

#[derive(Clone, Copy)]
pub enum SessionFunction<'a> {
    Version,
    ConnectionId,
    ClientSessionId,
    LastQueryId(i32),
    Variable(&'a str),
}

#[derive(Clone, Copy)]
pub enum AuthFunction {
    CurrentUser,
    CurrentRole,
    CurrentSecondaryRoles,
    CurrentAvailableRoles,
}

pub struct TypeCheckSubqueryPlan {
    pub s_expr: SExpr,
    pub output_context: BindContext,
}

pub struct TypeCheckDictionary {
    pub db_name: String,
    pub attr_type: DataType,
    pub primary_type: DataType,
    pub func_arg: DictGetFunctionArgument,
}

#[derive(Clone)]
pub struct FullTypeCheckAdapter {
    ctx: Arc<dyn TableContext>,
    dependencies: FullTypeCheckAdapterDependencies,
    forbid_udf: bool,
    skip_sequence_check: bool,
}

#[derive(Clone)]
struct FullTypeCheckAdapterDependencies {
    async_runtime_handle: fn() -> Result<Handle>,
    aggregate_function_factory: &'static AggregateFunctionFactory,
    license_manager: Arc<LicenseManagerSwitch>,
    catalog_manager: Arc<CatalogManager>,
    user_api_provider: Arc<UserApiProvider>,
    storage_allow_insecure: bool,
    security_policy_cache_manager: Arc<SecurityPolicyCacheManager>,
    cloud_control_api_provider: Option<Arc<CloudControlApiProvider>>,
}

fn missing_type_check_adapter_dependency(name: &str) -> ErrorCode {
    ErrorCode::Internal(format!("type check adapter does not provide {name}"))
}

pub trait UdfAdapter: Clone {
    fn load_definition(&self, _udf_name: &str) -> Result<Option<UserDefinedFunction>> {
        Err(missing_type_check_adapter_dependency("definition loader"))
    }

    fn load_stage_locations(&self, _locations: &[String]) -> Result<Vec<(StageInfo, String)>> {
        Err(missing_type_check_adapter_dependency("stage loader"))
    }

    fn load_udf_code(&self, _code: String) -> Result<Vec<u8>> {
        Err(missing_type_check_adapter_dependency("code loader"))
    }

    fn fold_udf_server(
        &self,
        _name: &str,
        _args: Vec<Scalar>,
        _udf_definition: UDFServer,
    ) -> Result<Scalar> {
        Err(missing_type_check_adapter_dependency("udf server folding"))
    }

    fn validation_config(&self) -> Result<UdfValidationConfig> {
        Err(missing_type_check_adapter_dependency(
            "udf validation config",
        ))
    }

    fn apply_udf_cloud_script(
        &self,
        _resource_name: &str,
        _udf_definition: UDFScript,
    ) -> Result<UDFServer> {
        Err(missing_type_check_adapter_dependency("udf cloud script"))
    }
}

pub trait TypeCheckAdapter: Clone + Sized {
    type UdfAdapter: UdfAdapter;

    fn function_context(&self) -> Result<FunctionContext>;

    fn settings(&self) -> Arc<Settings>;

    fn aggregate_function_factory(&self) -> &'static AggregateFunctionFactory;

    fn udf_adapter(&self) -> Result<Self::UdfAdapter>;

    fn check_core_expr_context(&self, _arena: &CoreExprArena<'_>) -> Result<()> {
        Ok(())
    }

    fn forbid_udf(&self) -> bool {
        false
    }

    fn validate_sequence(&self, _sequence_name: &str) -> Result<()> {
        Err(missing_type_check_adapter_dependency("sequence resolver"))
    }

    fn resolve_dictionary(
        &self,
        _db_name: Option<&str>,
        _dict_name: &str,
        _attr_name: &str,
    ) -> Result<TypeCheckDictionary> {
        Err(missing_type_check_adapter_dependency("dictionary resolver"))
    }

    fn resolve_read_file_stage_info(&self, _span: Span, _stage_name: &str) -> Result<StageInfo> {
        Err(missing_type_check_adapter_dependency("stage resolver"))
    }

    fn bind_subquery(
        &self,
        _parent_context: &BindContext,
        _name_resolution_ctx: &NameResolutionContext,
        _metadata: MetadataRef,
        _subquery: &Query,
    ) -> Result<TypeCheckSubqueryPlan> {
        Err(missing_type_check_adapter_dependency("subquery planner"))
    }

    fn resolve_namespace_function(&self, _function: NamespaceFunction) -> Result<Scalar> {
        Err(missing_type_check_adapter_dependency("namespace function"))
    }

    fn resolve_session_function(&self, _function: SessionFunction<'_>) -> Result<Scalar> {
        Err(missing_type_check_adapter_dependency("session function"))
    }

    fn resolve_authorization_function(&self, _function: AuthFunction) -> Result<Scalar> {
        Err(missing_type_check_adapter_dependency(
            "authorization function",
        ))
    }

    fn resolve_effective_role_names(&self) -> Result<Vec<String>> {
        Err(missing_type_check_adapter_dependency(
            "effective role names",
        ))
    }

    fn set_result_cache_uncacheable(&self);

    fn resolve_data_mask_policy(
        &self,
        _policy_id: u64,
    ) -> Result<Option<Arc<CachedSecurityPolicy>>> {
        Ok(None)
    }
}

/// A helper for type checking.
///
/// `TypeChecker::resolve` first lowers an AST `Expr` into a core expression tree,
/// then resolves the lowered tree into a typed expression `Scalar`. At the same
/// time, name resolution will be performed, which check validity of unbound
/// `ColumnRef` and try to replace it with qualified `BoundColumnRef`.
///
/// If failed, a `SemanticError` will be raised. This may caused by incompatible
/// argument types of expressions, or unresolvable columns.
pub struct TypeChecker<'a, A> {
    bind_context: &'a mut BindContext,
    adapter: A,
    dialect: Dialect,
    func_ctx: FunctionContext,
    name_resolution_ctx: &'a NameResolutionContext,
    metadata: MetadataRef,

    aliases: &'a [(String, ScalarExpr)],
    fallback_aliases: Option<&'a [(String, ScalarExpr)]>,

    // true if current expr is inside an aggregate function.
    // This is used to check if there is nested aggregate function.
    in_aggregate_function: bool,

    // true if current expr is inside a window function.
    // This is used to allow aggregation function in window's aggregate function.
    in_window_function: bool,

    // true if currently resolving a masking policy expression.
    // This prevents infinite recursion when a masking policy references the masked column itself.
    in_masking_policy: bool,
}
