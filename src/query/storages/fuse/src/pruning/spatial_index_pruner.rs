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
use std::sync::Arc;

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
use databend_common_expression::types::boolean::BooleanDomain;
use databend_common_expression::types::nullable::NullableDomain;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_storages_common_index::SpatialPredicate;
use databend_storages_common_index::collect_spatial_predicates;
use databend_storages_common_io::ReadSettings;
use databend_storages_common_table_meta::meta::BlockMeta;
use geo_index::rtree::RTreeIndex;
use geo_index::rtree::RTreeRef;
use opendal::Operator;

use crate::io::read::SpatialIndexReader;

pub struct SpatialIndexPruner {
    func_ctx: FunctionContext,
    expr: Expr<String>,
    base_domains: HashMap<String, Domain>,
    predicates: Vec<SpatialPredicate>,
    reader: SpatialIndexReader,
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
        let reader = SpatialIndexReader::create(operator.clone(), settings, column_ids);

        let base_domains = ConstantFolder::full_input_domains(&result.expr);

        Ok(Some(Arc::new(SpatialIndexPruner {
            func_ctx,
            expr: result.expr,
            base_domains,
            predicates: result.predicates,
            reader,
        })))
    }

    pub async fn should_prune(&self, block_meta: &BlockMeta) -> Result<bool> {
        let Some(spatial_stats) = block_meta.spatial_stats.as_ref() else {
            return Ok(false);
        };
        let Some(location) = block_meta.spatial_index_location.as_ref() else {
            return Ok(false);
        };

        let result = self.reader.read(&location.0).await?;
        let columns = result.columns;
        let column_id_to_index = result.column_id_to_index;

        let mut domains = self.base_domains.clone();
        for predicate in &self.predicates {
            let Some(stat) = spatial_stats.get(&predicate.column_id) else {
                continue;
            };
            if !stat.is_valid || stat.srid != predicate.query_srid {
                continue;
            };
            let Some(column_index) = column_id_to_index.get(&predicate.column_id) else {
                continue;
            };
            let Some(column) = columns.get(*column_index) else {
                continue;
            };
            let Some(ScalarRef::Binary(buffer)) = column.index(0) else {
                continue;
            };
            let tree = match RTreeRef::<f64>::try_new(&buffer) {
                Ok(tree) => tree,
                Err(e) => {
                    return Err(databend_common_exception::ErrorCode::Internal(format!(
                        "Invalid spatial index: {e}"
                    )));
                }
            };
            let is_intersect = if let Some(query_rect) = &predicate.query_rect {
                let hits = tree.search_rect(query_rect);
                !hits.is_empty()
            } else {
                false
            };
            if !is_intersect {
                domains.insert(
                    predicate.placeholder.clone(),
                    spatial_false_domain(&predicate.return_type, stat.has_null),
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
