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
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::display::display_tuple_field_name;
use databend_common_expression::infer_schema_type;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberScalar;
use jsonb::keypath::KeyPath;
use jsonb::keypath::KeyPaths;
use jsonb::keypath::parse_key_paths;
use unicase::Ascii;

use super::TypeChecker;
use crate::BaseTableColumn;
use crate::ColumnBinding;
use crate::ColumnEntry;
use crate::IndexType;
use crate::binder::NameResolutionResult;
use crate::binder::VirtualColumnName;
use crate::binder::wrap_cast;
use crate::plans::BoundColumnRef;
use crate::plans::CastExpr;
use crate::plans::ConstantExpr;
use crate::plans::FunctionCall;
use crate::plans::ScalarExpr;

impl<'a> TypeChecker<'a> {
    fn rewritable_variant_functions() -> &'static [Ascii<&'static str>] {
        static VARIANT_FUNCTIONS: &[Ascii<&'static str>] = &[
            Ascii::new("get_by_keypath"),
            Ascii::new("get_by_keypath_string"),
        ];
        VARIANT_FUNCTIONS
    }

    pub(super) fn try_rewrite_variant_function(
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
        if column.index.as_usize() >= self.metadata.read().columns().len() {
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

    pub(super) fn resolve_map_access(
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
            if column.index.as_usize() < self.metadata.read().columns().len() {
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

    pub(super) fn try_rewrite_virtual_column(
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
            if column.index.as_usize() < self.metadata.read().columns().len() {
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

    pub(super) fn resolve_stage_location(
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
}
