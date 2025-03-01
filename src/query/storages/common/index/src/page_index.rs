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
use std::ops::Range;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::ConstantFolder;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::Domain;
use databend_common_expression::Expr;
use databend_common_expression::FunctionContext;
use databend_common_expression::Scalar;
use databend_common_expression::TableSchemaRef;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_storages_common_table_meta::meta::ClusterStatistics;
use databend_storages_common_table_meta::meta::ColumnStatistics;

use crate::range_index::statistics_to_domain;

#[derive(Clone)]
pub struct PageIndex {
    expr: Expr<String>,
    column_refs: HashMap<String, DataType>,
    func_ctx: FunctionContext,
    cluster_key_id: u32,

    // index of the cluster key inside the schema
    cluster_key_fields: Vec<DataField>,
}

impl PageIndex {
    pub fn try_create(
        func_ctx: FunctionContext,
        cluster_key_id: u32,
        cluster_keys: Vec<String>,
        expr: &Expr<String>,
        schema: TableSchemaRef,
    ) -> Result<Self> {
        let data_schema: DataSchemaRef = Arc::new((&schema).into());
        let cluster_key_fields = cluster_keys
            .iter()
            .map(|name| data_schema.field_with_name(name.as_str()).unwrap().clone())
            .collect::<Vec<_>>();

        Ok(Self {
            column_refs: expr.column_refs(),
            expr: expr.clone(),
            cluster_key_fields,
            cluster_key_id,
            func_ctx,
        })
    }

    pub fn try_apply_const(&self) -> Result<bool> {
        // if the exprs did not contains the first cluster key, we should return true
        if self.cluster_key_fields.is_empty()
            || !self
                .column_refs
                .iter()
                .any(|c| self.cluster_key_fields.iter().any(|f| f.name() == c.0))
        {
            return Ok(true);
        }

        // Only return false, which means to skip this block, when the expression is folded to a constant false.
        Ok(!matches!(self.expr, Expr::Constant {
            scalar: Scalar::Boolean(false),
            ..
        }))
    }

    #[fastrace::trace]
    pub fn apply(&self, stats: &Option<ClusterStatistics>) -> Result<(bool, Option<Range<usize>>)> {
        let stats = match stats {
            Some(stats) => stats,
            None => return Ok((true, None)),
        };
        let min_values: Vec<Scalar> = match stats.pages {
            Some(ref pages) => pages.clone(),
            None => return Ok((true, None)),
        };

        let max_value = Scalar::Tuple(stats.max().clone());

        if self.cluster_key_id != stats.cluster_key_id {
            return Ok((true, None));
        }

        let pages = min_values.len();
        let mut start = 0;
        let mut end = pages - 1;

        while start <= end {
            let min_value = &min_values[start];
            let max_value = if start + 1 < pages {
                &min_values[start + 1]
            } else {
                &max_value
            };

            if self.eval_single_page(min_value, max_value)? {
                break;
            }
            start += 1;
        }

        while end >= start {
            let min_value = &min_values[end];
            let max_value = if end + 1 < pages {
                &min_values[end + 1]
            } else {
                &max_value
            };

            if self.eval_single_page(min_value, max_value)? {
                break;
            }
            end -= 1;
        }

        // no page is pruned
        if start + pages == end + 1 {
            return Ok((true, None));
        }

        if start > end {
            Ok((false, None))
        } else {
            Ok((true, Some(start..end + 1)))
        }
    }

    fn eval_single_page(&self, min_value: &Scalar, max_value: &Scalar) -> Result<bool> {
        let min_value = min_value
            .as_tuple()
            .ok_or_else(|| ErrorCode::StorageOther("cluster stats must be tuple scalar"))?;
        let max_value = max_value
            .as_tuple()
            .ok_or_else(|| ErrorCode::StorageOther("cluster stats must be tuple scalar"))?;

        let mut input_domains = HashMap::with_capacity(self.cluster_key_fields.len());
        for (idx, (min, max)) in min_value.iter().zip(max_value.iter()).enumerate() {
            if self
                .column_refs
                .contains_key(self.cluster_key_fields[idx].name())
            {
                let f = &self.cluster_key_fields[idx];

                let stat = ColumnStatistics::new(min.clone(), max.clone(), 1, 0, None);
                let domain = statistics_to_domain(vec![&stat], f.data_type());
                input_domains.insert(f.name().clone(), domain);
            }

            // For Tuple scalars, if the first element is not equal, then the monotonically increasing property is broken.
            if min != max {
                break;
            }
        }

        if input_domains.is_empty() {
            return Ok(true);
        }

        // Fill missing stats to be full domain
        for (name, ty) in self.column_refs.iter() {
            if !input_domains.contains_key(name.as_str()) {
                input_domains.insert(name.clone(), Domain::full(ty));
            }
        }

        let (new_expr, _) = ConstantFolder::fold_for_prune(
            &self.expr,
            &input_domains,
            &self.func_ctx,
            &BUILTIN_FUNCTIONS,
        );

        // Only return false, which means to skip this block, when the expression is folded to a constant false.
        Ok(!matches!(new_expr, Expr::Constant {
            scalar: Scalar::Boolean(false),
            ..
        }))
    }
}
