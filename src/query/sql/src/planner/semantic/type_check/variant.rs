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

use databend_common_ast::Span;
use databend_common_ast::ast::Expr;
use databend_common_ast::ast::Identifier;
use databend_common_ast::ast::Literal;
use databend_common_ast::ast::MapAccessor;
use databend_common_ast::ast::TypeName;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::display::display_tuple_field_name;
use databend_common_expression::format_runtime_keypaths;
use databend_common_expression::infer_schema_type;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberScalar;
use jsonb::keypath::OwnedKeyPath;
use jsonb::keypath::OwnedKeyPaths;
use jsonb::keypath::parse_key_paths;
use unicase::Ascii;

use super::CoreExpr;
use super::CoreExprArena;
use super::CoreExprArgs;
use super::CoreExprId;
use super::TypeChecker;
use crate::BaseTableColumn;
use crate::ColumnBinding;
use crate::ColumnEntry;
use crate::binder::NameResolutionResult;
use crate::binder::VirtualColumnName;
use crate::binder::wrap_cast;
use crate::planner::semantic::resolve_type_name;
use crate::plans::BoundColumnRef;
use crate::plans::ConstantExpr;
use crate::plans::FunctionCall;
use crate::plans::ScalarExpr;

// Keep this local to avoid introducing a dependency from common-sql to common-storages-fuse.
const FUSE_OPT_KEY_ENABLE_VIRTUAL_COLUMN: &str = "enable_virtual_column";

impl<'a> CoreExprArena<'a> {
    pub(super) fn lower_map_access_expr(
        &mut self,
        root_span: Span,
        root_expr: &'a Expr,
        root_accessor: &'a MapAccessor,
    ) -> Result<CoreExprId> {
        let mut current_span = root_span;
        let mut expr = root_expr;
        let mut accessor = root_accessor;
        let mut paths = VecDeque::new();
        loop {
            let path = match accessor {
                MapAccessor::Bracket {
                    key: box Expr::Literal { value, .. },
                } => {
                    if !matches!(value, Literal::UInt64(_) | Literal::String(_)) {
                        return Err(ErrorCode::SemanticError(format!(
                            "Unsupported accessor: {:?}",
                            value
                        ))
                        .set_span(current_span));
                    }
                    value.clone()
                }
                MapAccessor::Colon { key } => Literal::String(key.name.clone()),
                MapAccessor::DotNumber { key } => Literal::UInt64(*key),
                MapAccessor::Bracket { key } => {
                    let expr = self.lower_call_expr(current_span, "get", [expr, key.as_ref()])?;
                    return Ok(if paths.is_empty() {
                        expr
                    } else {
                        self.alloc(CoreExpr::MapAccess {
                            span: root_span,
                            expr_span: current_span,
                            expr,
                            paths,
                        })
                    });
                }
            };
            paths.push_front((current_span, path));

            let Expr::MapAccess {
                span,
                expr: inner_expr,
                accessor: inner_accessor,
                ..
            } = expr
            else {
                break;
            };
            current_span = *span;
            expr = inner_expr;
            accessor = inner_accessor;
        }
        let expr_span = expr.span();
        let expr = self.lower_ast_expr(expr)?;
        Ok(self.alloc(CoreExpr::MapAccess {
            span: root_span,
            expr_span,
            expr,
            paths,
        }))
    }
}

pub(super) fn json_op_core_function(op: &databend_common_ast::ast::JsonOperator) -> &'static str {
    match op {
        databend_common_ast::ast::JsonOperator::Arrow => "get",
        databend_common_ast::ast::JsonOperator::LongArrow => "get_string",
        databend_common_ast::ast::JsonOperator::HashArrow => "get_by_keypath",
        databend_common_ast::ast::JsonOperator::HashLongArrow => "get_by_keypath_string",
        databend_common_ast::ast::JsonOperator::Question => "json_exists_key",
        databend_common_ast::ast::JsonOperator::QuestionOr => "json_exists_any_keys",
        databend_common_ast::ast::JsonOperator::QuestionAnd => "json_exists_all_keys",
        databend_common_ast::ast::JsonOperator::AtArrow => "json_contains_in_left",
        databend_common_ast::ast::JsonOperator::ArrowAt => "json_contains_in_right",
        databend_common_ast::ast::JsonOperator::AtQuestion => "json_path_exists",
        databend_common_ast::ast::JsonOperator::AtAt => "json_path_match",
        databend_common_ast::ast::JsonOperator::HashMinus => "delete_by_keypath",
    }
}

// =============================================================================
// Variant virtual-column pushdown
// =============================================================================
//
// Goal: rewrite static JSON path access on a Variant base column into a virtual
// column that storage can materialize (and optionally cast) without evaluating
// get / get_by_keypath at runtime.
//
// Flow (three entry shapes, one shared bind path):
//
//   CoreExpr::Cast                              // data['k']::int, get(...)::string
//     └─ try_resolve_variant_cast_pushdown
//          ├─ StaticVariantAccess::from_expr
//          │    MapAccess | get* chain | get_by_keypath*
//          ├─ resolve base once
//          ├─ PushdownTarget::from_type_name    // non-nullable cast target
//          ├─ success → BoundColumnRef
//          └─ fail → rebuild access from the resolved base, then apply Cast
//
//   resolve_call                                // get(data, 'k'), get_by_keypath(data, ...)
//     └─ try_resolve_variant_function
//          ├─ StaticVariantAccess::from_function_call
//          │    get / get_string /
//          │    get_by_keypath / get_by_keypath_string
//          ├─ resolve base once
//          ├─ target = String | Variant
//          ├─ success → BoundColumnRef
//          └─ fail → rebuild the original function chain from the resolved base
//
//   resolve_map_access_from_scalar              // data['a']['b']
//     └─ resolve_variant_map_access
//          ├─ paths → OwnedKeyPaths
//          └─ try_pushdown_variant_paths(target = Variant)
//               success → BoundColumnRef
//               fail → get_by_keypath FunctionCall
//
//   try_pushdown_variant_paths(target)  (sole gate + bind)
//     1. allow_virtual_column
//     2. base is BoundColumnRef
//     3. BaseTableColumn | VirtualColumn → full keypaths
//     4. table option enable_virtual_column
//     5. add_virtual_column_binding (wrap target as Nullable)
//
// Target type rules:
// - Recorded pushdown cast types are always non-nullable inner types.
// - Binding re-wraps as Nullable(...) because the path may be missing.
// - Equivalent casts unify: CAST AS String / String NULL / get_string → one binding.
// - Display name encodes cast: data['k']::Int32, try_cast(data['k'] AS String).
// =============================================================================

impl<'a, A> TypeChecker<'a, A>
where A: super::TypeCheckAdapter
{
    /// Try to push a `Cast(inner AS type)` and `TRY_Cast(inner AS type)` expression
    /// down as part of a virtual column when `inner` is a static variant JSON access
    /// on a variant column or an existing variant virtual column.
    ///
    /// Supported inner forms (via [`StaticVariantAccess::from_expr`]):
    /// - `MapAccess`: `data['k']::int`
    /// - `get` chain: `get(get(v,'a'),'b')::int`
    /// - `get_by_keypath`: `get_by_keypath(v, '{"k"}')::int`
    ///
    /// On successful pushdown the outer `CastExpr` is dropped and the returned scalar is a
    /// `BoundColumnRef` whose column has the requested target type. Once the base has been
    /// resolved, failed pushdown is handled here by rebuilding the access and applying the cast,
    /// so callers only receive `Ok(None)` when no static access shape was recognized.
    pub(super) fn try_resolve_variant_cast_pushdown(
        &mut self,
        arena: &CoreExprArena<'_>,
        span: Span,
        inner: CoreExprId,
        target_type: &TypeName,
        is_try: bool,
    ) -> Result<Option<Box<(ScalarExpr, DataType)>>> {
        let Some(access) = StaticVariantAccess::from_expr(arena, inner)? else {
            return Ok(None);
        };
        // `get_string` / `get_by_keypath_string` first convert the JSON value to
        // String. Folding an outer cast directly into the virtual column would
        // instead cast the original JSON value to the outer target, which is not
        // generally equivalent. Push down the String-producing access on its own
        // when possible; otherwise rebuild it from the resolved base below. The
        // outer cast is applied only after String semantics have been preserved.
        let box (scalar, data_type) = self.resolve_core(arena, access.base)?;
        if data_type.remove_nullable() == DataType::Variant {
            if access.string_result {
                if let Some(box (string_scalar, string_type)) = self.try_pushdown_variant_paths(
                    span,
                    &scalar,
                    &access.keypaths,
                    PushdownTarget::string(),
                ) {
                    return self
                        .resolve_cast_expr(span, string_scalar, string_type, target_type, is_try)
                        .map(Some);
                }
            } else {
                let target = PushdownTarget::from_type_name(target_type, is_try)?;
                if !matches!(target, PushdownTarget::Variant)
                    && let Some(result) =
                        self.try_pushdown_variant_paths(span, &scalar, &access.keypaths, target)
                {
                    return Ok(Some(result));
                }
            }
        }

        // Resolving the base can bind subqueries and update metadata. Reuse that
        // result when pushdown is unavailable instead of resolving the access again.
        let box (scalar, data_type) =
            self.resolve_static_variant_access_fallback(arena, span, scalar, data_type, access)?;
        self.resolve_cast_expr(span, scalar, data_type, target_type, is_try)
            .map(Some)
    }

    /// Try to rewrite a variant JSON access call into a virtual column bound against the
    /// underlying variant source column.
    ///
    /// Handles four function shapes uniformly via [`StaticVariantAccess::from_function_call`]:
    /// - `get(v, key)` / `get_string(v, key)` and nested `get(get(v, ...), ...)`.
    /// - `get_by_keypath(v, '{"a","b"}')` / `get_by_keypath_string(v, ...)`.
    ///
    /// Once the base has been resolved, failed pushdown is handled here by rebuilding the
    /// original function chain from that scalar. `None` is reserved for calls that are not
    /// recognized as static Variant access and have not resolved any argument.
    pub(super) fn try_resolve_variant_function(
        &mut self,
        arena: &CoreExprArena<'_>,
        span: Span,
        func_name: &str,
        args: &CoreExprArgs,
    ) -> Option<Result<Box<(ScalarExpr, DataType)>>> {
        let access = StaticVariantAccess::from_function_call(arena, func_name, args)?;
        let target = if access.string_result {
            PushdownTarget::string()
        } else {
            PushdownTarget::Variant
        };

        let box (scalar, data_type) = match self.resolve_core(arena, access.base) {
            Ok(resolved) => resolved,
            Err(err) => return Some(Err(err)),
        };

        if data_type.remove_nullable() == DataType::Variant
            && let Some(result) =
                self.try_pushdown_variant_paths(span, &scalar, &access.keypaths, target)
        {
            return Some(Ok(result));
        }

        // Once the base has been resolved, this helper owns the fallback path.
        // Returning None here would make resolve_call resolve the whole chain again.
        Some(self.resolve_static_variant_access_fallback(arena, span, scalar, data_type, access))
    }

    fn resolve_static_variant_access_fallback(
        &mut self,
        arena: &CoreExprArena<'_>,
        span: Span,
        mut scalar: ScalarExpr,
        data_type: DataType,
        access: StaticVariantAccess,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let StaticVariantAccess {
            keypaths,
            string_result,
            kind,
            ..
        } = access;
        match kind {
            StaticVariantAccessKind::MapAccess { expr_span, paths } => {
                if data_type.remove_nullable() == DataType::Variant {
                    return Ok(Self::resolve_variant_map_access_fallback(scalar, &keypaths));
                }
                self.resolve_map_access_from_scalar(span, expr_span, scalar, data_type, paths)
            }
            StaticVariantAccessKind::GetChain { path_ids } => {
                let last_index = path_ids.len().saturating_sub(1);
                let mut result_type = data_type;
                for (index, path_id) in path_ids.into_iter().enumerate() {
                    let box (path_scalar, _) = self.resolve_core(arena, path_id)?;
                    let func_name = if string_result && index == last_index {
                        "get_string"
                    } else {
                        "get"
                    };
                    let box (next_scalar, next_type) =
                        self.resolve_scalar_function_call(span, func_name, vec![], vec![
                            scalar,
                            path_scalar,
                        ])?;
                    scalar = next_scalar;
                    result_type = next_type;
                }
                Ok(Box::new((scalar, result_type)))
            }
            StaticVariantAccessKind::KeyPathCall { path_id } => {
                let box (path_scalar, _) = self.resolve_core(arena, path_id)?;
                let func_name = if string_result {
                    "get_by_keypath_string"
                } else {
                    "get_by_keypath"
                };
                self.resolve_scalar_function_call(span, func_name, vec![], vec![
                    scalar,
                    path_scalar,
                ])
            }
        }
    }

    /// Try to push a static variant JSON path down as a virtual column on the storage layer.
    ///
    /// Returns `Some(BoundColumnRef)` when `base` resolves to a base-table variant column or an
    /// existing virtual column and the containing table has virtual columns enabled. `target`
    /// controls whether the storage layer is asked to keep the value as `Nullable(Variant)` or
    /// to cast it to a concrete type up-front (see `PushdownTarget`).
    ///
    /// `keypaths` is borrowed; it is only cloned when pushdown actually binds a virtual column.
    fn try_pushdown_variant_paths(
        &mut self,
        span: Span,
        base: &ScalarExpr,
        keypaths: &OwnedKeyPaths,
        target: PushdownTarget,
    ) -> Option<Box<(ScalarExpr, DataType)>> {
        if !self.bind_context.allow_virtual_column {
            return None;
        }
        let ScalarExpr::BoundColumnRef(BoundColumnRef { column, .. }) = base else {
            return None;
        };

        // Normalize both `BaseTableColumn(v)` and `VirtualColumn(v['a'])` down to
        // `(table_index, source_column_id, source_column_name, full_keypaths)` so the pushdown
        // logic doesn't need to know which representation it started from.
        // Single metadata read covers column lookup and the enable_virtual_column table option.
        let (table_index, source_column_id, source_column_name, full_keypaths) = {
            let metadata = self.metadata.read();
            if column.index.as_usize() >= metadata.columns().len() {
                return None;
            }

            let (table_index, source_column_id, source_column_name, existing_paths) =
                match metadata.column(column.index) {
                    ColumnEntry::BaseTableColumn(base_column) => (
                        base_column.table_index,
                        base_column.column_id,
                        base_column.column_name.clone(),
                        None,
                    ),
                    ColumnEntry::VirtualColumn(virtual_column) => (
                        virtual_column.table_index,
                        virtual_column.source_column_id,
                        virtual_column.source_column_name.clone(),
                        Some(&virtual_column.key_paths),
                    ),
                    _ => return None,
                };

            if !metadata
                .table(table_index)
                .table()
                .get_table_info()
                .get_option(FUSE_OPT_KEY_ENABLE_VIRTUAL_COLUMN, false)
            {
                return None;
            }

            // Clone keypaths only after the enable check succeeds.
            let full_keypaths = match existing_paths {
                Some(prefix) => {
                    let mut owned = prefix.clone();
                    owned.paths.extend(keypaths.paths.iter().cloned());
                    owned
                }
                None => keypaths.clone(),
            };
            (
                table_index,
                source_column_id,
                source_column_name,
                full_keypaths,
            )
        };

        let key_name = owned_keypaths_to_name(&source_column_name, &full_keypaths);
        let virtual_column_name = VirtualColumnName::new(
            table_index,
            source_column_id,
            key_name,
            target.into_option(),
        );

        let column = self.bind_context.add_virtual_column_binding(
            self.metadata.clone(),
            &source_column_name,
            virtual_column_name,
            full_keypaths,
        )?;

        let data_type = *column.data_type.clone();
        Some(Box::new((
            BoundColumnRef { span, column }.into(),
            data_type,
        )))
    }

    pub(super) fn resolve_cast_to_variant(
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
        let scalar_is_nullable = scalar.data_type().is_nullable_or_null();
        let return_type = if is_try || data_type.is_nullable() || scalar_is_nullable {
            DataType::Nullable(Box::new(DataType::Variant))
        } else {
            DataType::Variant
        };
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

                    let field_return_type =
                        ScalarExpr::passthrough_nullable_type(DataType::from(field_type), [scalar]);
                    let value = FunctionCall {
                        span,
                        params: vec![Scalar::Number(NumberScalar::Int64((idx + 1) as i64))],
                        arguments: vec![scalar.clone()],
                        func_name: "get".to_string(),
                        return_type: Box::new(field_return_type),
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
                    return_type: Box::new(if is_try {
                        DataType::Variant.wrap_nullable()
                    } else {
                        DataType::Variant
                    }),
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
                    return_type: Box::new(return_type),
                }
                .into()
            }
        }
    }

    pub(super) fn resolve_map_access_from_scalar(
        &mut self,
        span: Span,
        expr_span: Span,
        scalar: ScalarExpr,
        data_type: DataType,
        mut paths: VecDeque<(Span, Literal)>,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let mut scalar = scalar;
        // Variant type can be converted to `get_by_keypath` function.
        if data_type.remove_nullable() == DataType::Variant {
            return self.resolve_variant_map_access(span, scalar, &mut paths);
        }

        let mut table_data_type = infer_schema_type(&data_type)?.physical_type().into_owned();
        // If it is a tuple column, convert it to the internal column specified by the paths.
        // For other types of columns, convert it to get functions.
        if let ScalarExpr::BoundColumnRef(BoundColumnRef { ref column, .. }) = scalar {
            if column.index.as_usize() < self.metadata.read().columns().len() {
                let column_entry = self.metadata.read().column(column.index).clone();
                if let ColumnEntry::BaseTableColumn(BaseTableColumn { ref data_type, .. }) =
                    column_entry
                {
                    // Use data type from meta to get the field names of tuple type.
                    table_data_type = data_type.physical_type().into_owned();
                    if let TableDataType::Tuple { .. } = table_data_type.remove_nullable() {
                        let box (inner_scalar, _inner_data_type) = self
                            .resolve_tuple_map_access_pushdown(
                                expr_span,
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
                let return_type =
                    ScalarExpr::passthrough_nullable_type(DataType::from(&table_data_type), [
                        &scalar,
                    ]);
                scalar = FunctionCall {
                    span: expr_span,
                    func_name: "get".to_string(),
                    params: vec![Scalar::Number(NumberScalar::Int64((idx + 1) as i64))],
                    arguments: vec![scalar.clone()],
                    return_type: Box::new(return_type),
                }
                .into();
                continue;
            }
            let box (path_scalar, _) = self.resolve_literal(span, &path_lit)?;
            table_data_type = match table_data_type {
                TableDataType::Array(inner_type) => *inner_type,
                TableDataType::Map(inner_type) => match inner_type.remove_nullable() {
                    TableDataType::Tuple { fields_type, .. } => fields_type[1].clone(),
                    _ => unreachable!("map inner type must be a tuple"),
                },
                TableDataType::EmptyArray | TableDataType::EmptyMap => TableDataType::Null,
                data_type => data_type,
            };
            table_data_type = table_data_type.wrap_nullable();
            scalar = FunctionCall {
                span: path_scalar.span(),
                func_name: "get".to_string(),
                params: vec![],
                arguments: vec![scalar.clone(), path_scalar],
                return_type: Box::new(DataType::from(&table_data_type)),
            }
            .into();
        }
        let return_type = scalar.data_type().into_owned();
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
                    let return_type =
                        ScalarExpr::passthrough_nullable_type(DataType::from(&table_data_type), [
                            &scalar,
                        ]);
                    scalar = FunctionCall {
                        span,
                        params: vec![Scalar::Number(NumberScalar::Int64(idx as i64))],
                        arguments: vec![scalar.clone()],
                        func_name: "get".to_string(),
                        return_type: Box::new(return_type),
                    }
                    .into();
                    scalar = wrap_cast(&scalar, &DataType::from(&table_data_type));
                }
                let return_type = scalar.data_type().into_owned();
                Ok(Box::new((scalar, return_type)))
            }
        }
    }

    // Rewrite variant map access as `get_by_keypath`,
    // using a virtual column when the static path can be pushed to storage.
    fn resolve_variant_map_access(
        &mut self,
        span: Span,
        scalar: ScalarExpr,
        paths: &mut VecDeque<(Span, Literal)>,
    ) -> Result<Box<(ScalarExpr, DataType)>> {
        let key_paths = paths
            .iter()
            .map(|(s, l)| literal_to_owned_keypath(*s, l))
            .collect::<Result<Vec<_>>>()?;
        let owned_keypaths = OwnedKeyPaths { paths: key_paths };

        // Try rewriting as a virtual column and pushing down to the storage layer.
        // keypaths is borrowed, only cloned inside try_pushdown on successful bind.
        if let Some(result) =
            self.try_pushdown_variant_paths(span, &scalar, &owned_keypaths, PushdownTarget::Variant)
        {
            return Ok(result);
        }

        Ok(Self::resolve_variant_map_access_fallback(
            scalar,
            &owned_keypaths,
        ))
    }

    fn resolve_variant_map_access_fallback(
        scalar: ScalarExpr,
        keypaths: &OwnedKeyPaths,
    ) -> Box<(ScalarExpr, DataType)> {
        // Keep the original `get_by_keypath` runtime semantics. This helper is
        // also used after an earlier pushdown attempt, so it must not retry it.
        let keypaths_str = format_runtime_keypaths(keypaths);
        let path_scalar = ScalarExpr::ConstantExpr(ConstantExpr {
            span: None,
            value: Scalar::String(keypaths_str),
        });
        let return_type = DataType::Nullable(Box::new(DataType::Variant));

        Box::new((
            ScalarExpr::FunctionCall(FunctionCall {
                span: None,
                func_name: "get_by_keypath".to_string(),
                params: vec![],
                arguments: vec![scalar, path_scalar],
                return_type: Box::new(return_type.clone()),
            }),
            return_type,
        ))
    }
}

/// Target of a variant JSON path pushdown to the storage layer.
///
/// Invariant: `Cast`'s `TableDataType` is always non-nullable. Pushdown results are always
/// nullable; outer `Nullable` is stripped here and re-applied when the virtual column is bound.
#[derive(Debug)]
enum PushdownTarget {
    Variant,
    Cast(TableDataType, bool),
}

impl PushdownTarget {
    /// Target used by `get_string` / `get_by_keypath_string`.
    /// Same as `CAST(... AS String)` / `::String`: non-nullable inner type with strict cast.
    fn string() -> Self {
        PushdownTarget::Cast(TableDataType::String, false)
    }

    fn from_type_name(target: &TypeName, is_try: bool) -> Result<Self> {
        let ty = resolve_type_name(target, true)?;
        // Always strip Nullable so equivalent casts unify:
        // `CAST(x AS String)` / `CAST(x AS String NULL)` / `get_string` → same binding.
        let inner = ty.remove_nullable();
        let target_type = if matches!(
            inner,
            TableDataType::Boolean
                | TableDataType::String
                | TableDataType::Number(_)
                | TableDataType::Decimal(_)
                | TableDataType::Date
                | TableDataType::Timestamp
        ) {
            PushdownTarget::Cast(inner, is_try)
        } else {
            PushdownTarget::Variant
        };
        Ok(target_type)
    }

    fn into_option(self) -> Option<(TableDataType, bool)> {
        match self {
            PushdownTarget::Variant => None,
            // Defensive strip: constructors already produce non-nullable types.
            PushdownTarget::Cast(ty, is_try) => Some((ty.remove_nullable(), is_try)),
        }
    }
}

/// Nested `get(get(..., p1), p2)` chain flattened into its base expression and
/// parsed keypaths for pushdown. The original path expression IDs are retained
/// so normal resolution can reuse the resolved base when pushdown is unavailable.
struct GetChain {
    base: CoreExprId,
    key_paths: OwnedKeyPaths,
    path_ids: Vec<CoreExprId>,
}

impl GetChain {
    fn parse(arena: &CoreExprArena<'_>, args: &CoreExprArgs) -> Option<Self> {
        let mut key_paths = Vec::new();
        let mut path_ids = Vec::new();
        let mut current_args = args;
        let base = loop {
            let [expr, path_id] = current_args.as_slice() else {
                return None;
            };
            let CoreExpr::Literal { value, .. } = arena.get(*path_id) else {
                return None;
            };
            key_paths.push(keypath_from_scalar(value)?);
            path_ids.push(*path_id);

            match arena.get(*expr) {
                CoreExpr::Call {
                    func_name: "get",
                    args,
                    ..
                } => current_args = args,
                _ => break *expr,
            }
        };
        key_paths.reverse();
        path_ids.reverse();
        Some(Self {
            base,
            key_paths: OwnedKeyPaths { paths: key_paths },
            path_ids,
        })
    }
}

enum StaticVariantAccessKind {
    MapAccess {
        expr_span: Span,
        paths: VecDeque<(Span, Literal)>,
    },
    GetChain {
        path_ids: Vec<CoreExprId>,
    },
    KeyPathCall {
        path_id: CoreExprId,
    },
}

/// A static variant JSON access extracted from CoreExpr, shared by the cast / function
/// pushdown entry points.
///
/// - `base`: expression that should resolve to a Variant column
/// - `keypaths`: full static path to push down
/// - `string_result`: true for `get_string` / `get_by_keypath_string` (target String)
struct StaticVariantAccess {
    base: CoreExprId,
    keypaths: OwnedKeyPaths,
    string_result: bool,
    kind: StaticVariantAccessKind,
}

impl StaticVariantAccess {
    /// Extract a static variant access from an arbitrary CoreExpr (used by cast pushdown).
    ///
    /// Recognizes:
    /// - `MapAccess { expr, paths }` — `data['k']`
    /// - `Call { get / get_string, ... }` — nested get chain
    /// - `Call { get_by_keypath / get_by_keypath_string, ... }` — keypath literal
    ///
    /// Returns `Ok(None)` when the expression is not a static variant access.
    fn from_expr(arena: &CoreExprArena<'_>, expr: CoreExprId) -> Result<Option<Self>> {
        match arena.get(expr) {
            CoreExpr::MapAccess {
                expr_span,
                expr,
                paths,
                ..
            } => {
                let owned_keypaths = paths
                    .iter()
                    .map(|(s, l)| literal_to_owned_keypath(*s, l))
                    .collect::<Result<Vec<_>>>()?;
                Ok(Some(Self {
                    base: *expr,
                    keypaths: OwnedKeyPaths {
                        paths: owned_keypaths,
                    },
                    string_result: false,
                    kind: StaticVariantAccessKind::MapAccess {
                        expr_span: *expr_span,
                        paths: paths.clone(),
                    },
                }))
            }
            CoreExpr::Call {
                func_name, args, ..
            } => Ok(Self::from_function_call(arena, func_name, args)),
            _ => Ok(None),
        }
    }

    /// Extract a static variant access from a known variant function call
    /// (`get` / `get_string` / `get_by_keypath` / `get_by_keypath_string`).
    fn from_function_call(
        arena: &CoreExprArena<'_>,
        func_name: &str,
        args: &CoreExprArgs,
    ) -> Option<Self> {
        let string_result = matches!(func_name, "get_string" | "get_by_keypath_string");
        match func_name {
            "get" | "get_string" => {
                let chain = GetChain::parse(arena, args)?;
                Some(Self {
                    base: chain.base,
                    keypaths: chain.key_paths,
                    string_result,
                    kind: StaticVariantAccessKind::GetChain {
                        path_ids: chain.path_ids,
                    },
                })
            }
            "get_by_keypath" | "get_by_keypath_string" => {
                let [base_id, path_id] = args.as_slice() else {
                    return None;
                };
                let CoreExpr::Literal {
                    value: Scalar::String(path),
                    ..
                } = arena.get(*path_id)
                else {
                    return None;
                };
                let keypaths = parse_key_paths(path.as_bytes()).ok()?.to_owned();
                Some(Self {
                    base: *base_id,
                    keypaths,
                    string_result,
                    kind: StaticVariantAccessKind::KeyPathCall { path_id: *path_id },
                })
            }
            _ => None,
        }
    }
}

fn keypath_from_scalar(value: &Scalar) -> Option<OwnedKeyPath> {
    match value {
        Scalar::String(path) => Some(OwnedKeyPath::Name(path.clone())),
        Scalar::Number(number) => {
            let index = number.integer_to_i128()?;
            if index < 0 {
                return None;
            }
            Some(OwnedKeyPath::Index(i32::try_from(index).ok()?))
        }
        _ => None,
    }
}

/// Convert a map-access literal segment into an `OwnedKeyPath`. The literal kinds are
/// already restricted by `lower_map_access_expr`, so anything else is a programming error.
fn literal_to_owned_keypath(span: Span, literal: &Literal) -> Result<OwnedKeyPath> {
    match literal {
        Literal::UInt64(idx) => {
            let i = i32::try_from(*idx).map_err(|_| {
                ErrorCode::SemanticError(format!(
                    "path index is overflow, max allowed value is {}, but got {}",
                    i32::MAX,
                    idx
                ))
                .set_span(span)
            })?;
            Ok(OwnedKeyPath::Index(i))
        }
        Literal::String(field) => Ok(OwnedKeyPath::Name(field.clone())),
        _ => unreachable!("map access literals are validated by lower_map_access_expr"),
    }
}

/// Build a display name for a virtual column that appends every path segment, e.g.
/// `v['message']['id']`.
fn owned_keypaths_to_name(column_name: &str, keypaths: &OwnedKeyPaths) -> String {
    let mut name = column_name.to_string();
    for path in &keypaths.paths {
        name.push('[');
        match path {
            OwnedKeyPath::Index(idx) => {
                name.push_str(&idx.to_string());
            }
            OwnedKeyPath::Name(field) => {
                name.push('\'');
                for ch in field.chars() {
                    if ch == '\\' || ch == '\'' {
                        name.push('\\');
                    }
                    name.push(ch);
                }
                name.push('\'');
            }
        }
        name.push(']');
    }
    name
}
