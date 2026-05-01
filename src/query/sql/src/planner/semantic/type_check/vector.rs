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

use databend_common_ast::Span;
use databend_common_catalog::plan::InternalColumn;
use databend_common_catalog::plan::InternalColumnType;
use databend_common_catalog::plan::VectorIndexInfo;
use databend_common_exception::Result;
use databend_common_expression::Scalar;
use databend_common_expression::VECTOR_SCORE_COL_NAME;
use databend_common_expression::types::DataType;
use databend_common_expression::types::F32;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::VectorScalar;
use databend_common_meta_app::schema::TableIndexType;
use unicase::Ascii;

use super::TypeChecker;
use crate::ColumnBinding;
use crate::binder::InternalColumnBinding;
use crate::plans::BoundColumnRef;
use crate::plans::ScalarExpr;

impl<'a> TypeChecker<'a> {
    fn vector_functions() -> &'static [Ascii<&'static str>] {
        static VECTOR_FUNCTIONS: &[Ascii<&'static str>] = &[
            Ascii::new("cosine_distance"),
            Ascii::new("l1_distance"),
            Ascii::new("l2_distance"),
        ];
        VECTOR_FUNCTIONS
    }

    fn try_fold_cast_to_vector(&self, expr: &ScalarExpr) -> Option<Vec<F32>> {
        if expr.evaluable() {
            let checked_expr = expr.as_expr().ok()?;
            let folded = self.try_fold_constant(&checked_expr, true)?;
            let constant = match &folded.0 {
                ScalarExpr::ConstantExpr(constant) | ScalarExpr::TypedConstantExpr(constant, _) => {
                    constant.clone()
                }
                _ => return None,
            };

            if let Scalar::Vector(VectorScalar::Float32(values)) = constant.value {
                return Some(values.clone());
            }
        }
        None
    }

    pub(super) fn try_rewrite_vector_function(
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
                    arg,
                ]
                | [
                    arg,
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
                    if table_index.is_some() && matches!(col_data_type, DataType::Vector(_)) {
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
                        let query_values = self.try_fold_cast_to_vector(arg)?;
                        let col_vector_type = col_data_type.as_vector().unwrap();
                        let col_dimension = col_vector_type.dimension() as usize;
                        let arg_dimension = query_values.len();
                        if col_dimension != arg_dimension {
                            return None;
                        }

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
}
