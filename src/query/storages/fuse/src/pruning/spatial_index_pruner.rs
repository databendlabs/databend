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
use std::collections::HashSet;
use std::io::Cursor;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_expression::ConstantFolder;
use databend_common_expression::Domain;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::ScalarRef;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::expr::Constant;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::boolean::BooleanDomain;
use databend_common_expression::types::nullable::NullableDomain;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_storages_common_index::SpatialPredicate;
use databend_storages_common_index::collect_spatial_predicates;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::BlockMeta;
use geo::Rect;
use geo_index::rtree::RTreeIndex;
use geo_index::rtree::RTreeRef;
use opendal::Operator;
use roaring::RoaringBitmap;

use crate::io::read::load_spatial_index_files;
use crate::io::read::load_spatial_index_meta;

pub struct SpatialIndexPruner {
    func_ctx: FunctionContext,
    operator: Operator,
    settings: ReadSettings,
    expr: Expr<String>,
    base_domains: HashMap<String, Domain>,
    predicates: Vec<SpatialPredicate>,
    column_ids: Vec<ColumnId>,
}

impl SpatialIndexPruner {
    pub fn create(
        func_ctx: FunctionContext,
        table_schema: &TableSchemaRef,
        filter_expr: Option<&Expr<String>>,
        spatial_index_columns: &HashSet<ColumnId>,
        operator: Operator,
        settings: ReadSettings,
    ) -> Result<Option<Arc<SpatialIndexPruner>>> {
        if spatial_index_columns.is_empty() {
            return Ok(None);
        }
        let Some(expr) = filter_expr else {
            return Ok(None);
        };

        let Some(result) =
            collect_spatial_predicates(table_schema.clone(), expr, Some(spatial_index_columns))?
        else {
            return Ok(None);
        };

        let mut column_ids = Vec::new();
        let mut seen = HashSet::new();
        for predicate in &result.predicates {
            if seen.insert(predicate.column_id) {
                column_ids.push(predicate.column_id);
            }
        }

        let base_domains = ConstantFolder::full_input_domains(&result.expr);
        Ok(Some(Arc::new(SpatialIndexPruner {
            func_ctx,
            operator,
            settings,
            expr: result.expr,
            base_domains,
            predicates: result.predicates,
            column_ids,
        })))
    }

    pub async fn should_prune(&self, block_meta: &BlockMeta) -> Result<bool> {
        let Some(location) = block_meta.spatial_index_location.as_ref() else {
            return Ok(false);
        };

        let meta = load_spatial_index_meta(self.operator.clone(), &location.0).await?;
        let available_columns = meta
            .columns
            .iter()
            .map(|(name, _)| name.clone())
            .collect::<HashSet<_>>();

        let mut column_names = Vec::new();
        let mut column_id_to_index = HashMap::new();
        for column_id in &self.column_ids {
            let column_name = column_id.to_string();
            let srid_name = format!("{}_srid", column_id);
            if !available_columns.contains(&column_name) || !available_columns.contains(&srid_name)
            {
                continue;
            }
            let column_index = column_names.len();
            column_names.push(column_name);
            let srid_index = column_names.len();
            column_names.push(srid_name);
            let invalid_index = {
                let invalid_name = format!("{}_invalid_rows", column_id);
                if available_columns.contains(&invalid_name) {
                    let idx = column_names.len();
                    column_names.push(invalid_name);
                    Some(idx)
                } else {
                    None
                }
            };
            column_id_to_index.insert(*column_id, (column_index, srid_index, invalid_index));
        }

        if column_names.is_empty() {
            return Ok(false);
        }

        let columns = load_spatial_index_files(
            self.operator.clone(),
            &self.settings,
            &column_names,
            &location.0,
        )
        .await?;

        let mut domains = self.base_domains.clone();
        for predicate in &self.predicates {
            let Some(&(column_index, srid_index, invalid_index)) =
                column_id_to_index.get(&predicate.column_id)
            else {
                continue;
            };
            let Some(srid_column) = columns.get(srid_index) else {
                continue;
            };
            let Some(ScalarRef::Number(NumberScalar::Int32(srid))) = srid_column.index(0) else {
                continue;
            };
            if srid != predicate.query_srid {
                continue;
            }
            let invalid_rows_rb = match invalid_index.and_then(|idx| columns.get(idx)) {
                Some(invalid_rows_column) => match invalid_rows_column.index(0) {
                    Some(ScalarRef::Binary(buffer)) => {
                        let mut cursor = Cursor::new(buffer);
                        let invalid_rows_rb = RoaringBitmap::deserialize_from(&mut cursor)
                            .map_err(|e| {
                                ErrorCode::Internal(format!("Invalid invalid-rows bitmap: {e}"))
                            })?;
                        Some(invalid_rows_rb)
                    }
                    _ => None,
                },
                None => None,
            };

            let has_null = invalid_rows_rb
                .as_ref()
                .is_some_and(|bitmap| !bitmap.is_empty());

            let Some(column) = columns.get(column_index) else {
                continue;
            };
            let Some(ScalarRef::Binary(buffer)) = column.index(0) else {
                continue;
            };
            let tree = match RTreeRef::<f64>::try_new(&buffer) {
                Ok(tree) => tree,
                Err(e) => {
                    return Err(ErrorCode::Internal(format!("Invalid spatial index: {e}")));
                }
            };
            if !spatial_intersects(&tree, &predicate.query_rect, invalid_rows_rb.as_ref()) {
                domains.insert(
                    predicate.placeholder.clone(),
                    spatial_false_domain(&predicate.return_type, has_null),
                );
            }
        }
        if domains.is_empty() {
            return Ok(false);
        }

        let (folded, _) = ConstantFolder::fold_with_domain(
            &self.expr,
            &domains,
            &self.func_ctx,
            &BUILTIN_FUNCTIONS,
        );
        Ok(matches!(
            folded,
            Expr::Constant(Constant {
                scalar: Scalar::Boolean(false),
                ..
            })
        ))
    }
}

fn spatial_intersects(
    tree: &RTreeRef<'_, f64>,
    query_rect: &Rect<f64>,
    invalid_rows_rb: Option<&RoaringBitmap>,
) -> bool {
    let hits = tree.search_rect(query_rect);
    if hits.is_empty() {
        return false;
    }
    let Some(invalid_rows_rb) = invalid_rows_rb else {
        return true;
    };
    hits.into_iter().any(|row| !invalid_rows_rb.contains(row))
}

fn spatial_false_domain(return_type: &DataType, has_null: bool) -> Domain {
    let bool_domain = Domain::Boolean(BooleanDomain {
        has_false: true,
        has_true: false,
    });
    if return_type.is_nullable() {
        Domain::Nullable(NullableDomain {
            has_null,
            value: Some(Box::new(bool_domain)),
        })
    } else {
        bool_domain
    }
}
