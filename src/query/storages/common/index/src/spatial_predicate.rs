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

use std::collections::HashSet;

use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::ColumnRef;
use databend_common_expression::Constant;
use databend_common_expression::ConstantFolder;
use databend_common_expression::Expr;
use databend_common_expression::ExprVisitor;
use databend_common_expression::FunctionCall;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::types::DataType;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_io::ewkb_to_geo;
use geo::BoundingRect;
use geo::Rect;
use geozero::wkb::Ewkb;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpatialOp {
    Contains,
    Intersects,
    Within,
    Equals,
}

#[derive(Debug, Clone)]
pub struct SpatialPredicate {
    pub column_id: ColumnId,
    pub query_rect: Rect<f64>,
    pub query_srid: i32,
    pub op: SpatialOp,
    pub placeholder: String,
    pub return_type: DataType,
}

#[derive(Debug, Clone)]
pub struct SpatialPredicateResult {
    pub expr: Expr<String>,
    pub predicates: Vec<SpatialPredicate>,
}

pub fn collect_spatial_predicates(
    schema: TableSchemaRef,
    expr: &Expr<String>,
    spatial_index_columns: Option<&HashSet<ColumnId>>,
) -> Result<Option<SpatialPredicateResult>> {
    let mut used_names = expr.column_refs().into_keys().collect::<HashSet<_>>();
    let mut visitor = SpatialPredicateVisitor::new(schema, spatial_index_columns, &mut used_names);
    let new_expr = match databend_common_expression::visit_expr(expr, &mut visitor)? {
        Some(expr) => expr,
        None => expr.clone(),
    };

    if visitor.predicates.is_empty() {
        return Ok(None);
    }

    Ok(Some(SpatialPredicateResult {
        expr: new_expr,
        predicates: visitor.predicates,
    }))
}

struct SpatialPredicateVisitor<'a> {
    schema: TableSchemaRef,
    spatial_index_columns: Option<&'a HashSet<ColumnId>>,
    func_ctx: FunctionContext,
    used_names: &'a mut HashSet<String>,
    next_placeholder: usize,
    predicates: Vec<SpatialPredicate>,
}

impl<'a> SpatialPredicateVisitor<'a> {
    fn new(
        schema: TableSchemaRef,
        spatial_index_columns: Option<&'a HashSet<ColumnId>>,
        used_names: &'a mut HashSet<String>,
    ) -> Self {
        Self {
            schema,
            spatial_index_columns,
            func_ctx: FunctionContext::default(),
            used_names,
            next_placeholder: 0,
            predicates: Vec::new(),
        }
    }

    fn next_placeholder_name(&mut self) -> String {
        loop {
            let candidate = format!("__spatial_predicate_{}", self.next_placeholder);
            self.next_placeholder += 1;
            if !self.schema.has_field(&candidate) && !self.used_names.contains(&candidate) {
                self.used_names.insert(candidate.clone());
                return candidate;
            }
        }
    }

    fn extract_column_arg(expr: &Expr<String>) -> Option<String> {
        match expr {
            Expr::ColumnRef(ColumnRef { id, data_type, .. }) => {
                if is_spatial_type(data_type) {
                    Some(id.clone())
                } else {
                    None
                }
            }
            Expr::Cast(cast) => match cast.expr.as_ref() {
                Expr::ColumnRef(ColumnRef { id, .. }) => {
                    if is_spatial_type(&cast.dest_type) {
                        Some(id.clone())
                    } else {
                        None
                    }
                }
                _ => None,
            },
            _ => None,
        }
    }

    fn extract_constant_arg(&self, expr: &Expr<String>) -> Option<Scalar> {
        if !expr.column_refs().is_empty() {
            return None;
        }
        let (folded, _) = ConstantFolder::fold(expr, &self.func_ctx, &BUILTIN_FUNCTIONS);
        if let Expr::Constant(Constant { scalar, .. }) = folded {
            Some(scalar)
        } else {
            None
        }
    }

    fn scalar_to_query(scalar: &Scalar) -> Option<(Rect<f64>, i32)> {
        match scalar {
            Scalar::Geometry(buffer) => {
                let mut ewkb = Ewkb(buffer.as_slice());
                let (geom, srid) = ewkb_to_geo(&mut ewkb).ok()?;
                let rect = geom.bounding_rect()?;
                Some((rect, srid.unwrap_or(0)))
            }
            Scalar::Geography(geo) => {
                let mut ewkb = Ewkb(geo.0.as_slice());
                let (geom, _) = ewkb_to_geo(&mut ewkb).ok()?;
                let rect = geom.bounding_rect()?;
                Some((rect, 4326))
            }
            _ => None,
        }
    }
}

impl ExprVisitor<String> for SpatialPredicateVisitor<'_> {
    type Error = databend_common_exception::ErrorCode;

    fn enter_function_call(&mut self, call: &FunctionCall<String>) -> Result<Option<Expr<String>>> {
        let FunctionCall {
            span,
            id,
            args,
            return_type,
            ..
        } = call;

        let func_name = id.name();
        let func_name = func_name.as_ref();
        let spatial_fn = matches!(
            func_name,
            "st_contains" | "st_intersects" | "st_within" | "st_equals"
        );
        if !spatial_fn || args.len() != 2 {
            return Self::visit_function_call(call, self);
        }

        let (left, right) = (&args[0], &args[1]);
        let (column, constant, column_is_left) = if let (Some(column), Some(constant)) = (
            Self::extract_column_arg(left),
            self.extract_constant_arg(right),
        ) {
            (column, constant, true)
        } else if let (Some(column), Some(constant)) = (
            Self::extract_column_arg(right),
            self.extract_constant_arg(left),
        ) {
            (column, constant, false)
        } else {
            return Self::visit_function_call(call, self);
        };

        let Ok(column_id) = self.schema.column_id_of(&column) else {
            return Self::visit_function_call(call, self);
        };
        if let Some(spatial_index_columns) = self.spatial_index_columns {
            if !spatial_index_columns.contains(&column_id) {
                return Self::visit_function_call(call, self);
            }
        }

        let Some((query_rect, query_srid)) = Self::scalar_to_query(&constant) else {
            return Self::visit_function_call(call, self);
        };

        let op = match func_name {
            "st_contains" => {
                if column_is_left {
                    SpatialOp::Contains
                } else {
                    SpatialOp::Within
                }
            }
            "st_within" => {
                if column_is_left {
                    SpatialOp::Within
                } else {
                    SpatialOp::Contains
                }
            }
            "st_intersects" => SpatialOp::Intersects,
            "st_equals" => SpatialOp::Equals,
            _ => unreachable!(),
        };

        let placeholder = self.next_placeholder_name();
        self.predicates.push(SpatialPredicate {
            column_id,
            query_rect,
            query_srid,
            op,
            placeholder: placeholder.clone(),
            return_type: return_type.clone(),
        });

        Ok(Some(Expr::ColumnRef(ColumnRef {
            span: *span,
            id: placeholder.clone(),
            data_type: return_type.clone(),
            display_name: placeholder,
        })))
    }
}

fn is_spatial_type(data_type: &DataType) -> bool {
    match data_type {
        DataType::Geometry | DataType::Geography => true,
        DataType::Nullable(inner) => is_spatial_type(inner),
        _ => false,
    }
}
