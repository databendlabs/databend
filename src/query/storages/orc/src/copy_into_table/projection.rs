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

use std::sync::Arc;

use arrow_cast::can_cast_types;
use arrow_schema::Field;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::type_check::check_cast;
use databend_common_expression::Expr;
use databend_common_expression::FieldDefaultExpr;
use databend_common_expression::TableSchemaRef;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_common_sql::binder::FieldDefaultExprEvaluator;

use crate::hashable_schema::HashableSchema;

#[derive(Clone)]
pub struct ProjectionFactory {
    pub output_schema: TableSchemaRef,
    default_value_eval: Option<Arc<FieldDefaultExprEvaluator>>,

    projections: Arc<dashmap::DashMap<HashableSchema, Vec<Expr>>>,
}

impl ProjectionFactory {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        output_schema: TableSchemaRef,
        default_values: Option<Vec<FieldDefaultExpr>>,
    ) -> Result<Self> {
        let default_value_eval = match &default_values {
            Some(vs) => Some(Arc::new(FieldDefaultExprEvaluator::try_create(
                &ctx,
                vs.clone(),
            )?)),
            None => None,
        };
        Ok(Self {
            output_schema,
            default_value_eval,
            projections: Default::default(),
        })
    }
    pub fn get(&self, schema: &HashableSchema, location: &str) -> Result<Vec<Expr>> {
        if let Some(v) = self.projections.get(schema) {
            Ok(v.clone())
        } else {
            let v = self
                .try_create_projection(schema.clone(), location)
                .unwrap();
            self.projections.insert(schema.clone(), v.clone());
            Ok(v)
        }
    }

    fn try_create_projection(&self, schema: HashableSchema, location: &str) -> Result<Vec<Expr>> {
        let mut pushdown_columns = vec![];
        let mut output_projection = vec![];

        let mut num_inputs = 0;
        for (i, to_field) in self.output_schema.fields().iter().enumerate() {
            let field_name = to_field.name();
            let expr = match schema
                .table_schema
                .fields()
                .iter()
                .position(|f| f.name() == field_name)
            {
                Some(pos) => {
                    pushdown_columns.push(pos);
                    let from_field = schema.table_schema.field(pos);
                    let expr = Expr::ColumnRef {
                        span: None,
                        id: pos,
                        data_type: from_field.data_type().into(),
                        display_name: from_field.name().clone(),
                    };

                    // find a better way to do check cast
                    if from_field.data_type == to_field.data_type {
                        expr
                    } else if can_cast_types(
                        Field::from(from_field).data_type(),
                        Field::from(to_field).data_type(),
                    ) {
                        check_cast(
                            None,
                            false,
                            expr,
                            &to_field.data_type().into(),
                            &BUILTIN_FUNCTIONS,
                        )?
                    } else {
                        return Err(ErrorCode::BadDataValueType(format!(
                            "fail to load file {}: Cannot cast column {} from {:?} to {:?}",
                            location,
                            field_name,
                            from_field.data_type(),
                            to_field.data_type()
                        )));
                    }
                }
                None => {
                    if let Some(default_value_eval) = &self.default_value_eval {
                        default_value_eval.get_expr(i, to_field.data_type().into())
                    } else {
                        return Err(ErrorCode::BadDataValueType(format!(
                            "{} missing column {}",
                            location, field_name,
                        )));
                    }
                }
            };
            if !matches!(expr, Expr::Constant { .. }) {
                num_inputs += 1;
            }
            output_projection.push(expr);
        }
        if num_inputs == 0 {
            return Err(ErrorCode::BadBytes(format!(
                "not column name match in parquet file {location}",
            )));
        }
        Ok(output_projection)
    }
}
