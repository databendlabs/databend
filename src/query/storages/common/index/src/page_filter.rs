// Copyright 2021 Datafuse Labs.
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

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::type_check::check_function;
use common_expression::types::nullable::NullableDomain;
use common_expression::types::number::SimpleDomain;
use common_expression::types::string::StringDomain;
use common_expression::types::DataType;
use common_expression::types::DateType;
use common_expression::types::NumberDataType;
use common_expression::types::NumberType;
use common_expression::types::StringType;
use common_expression::types::TimestampType;
use common_expression::types::ValueType;
use common_expression::with_number_mapped_type;
use common_expression::ConstantFolder;
use common_expression::DataField;
use common_expression::DataSchema;
use common_expression::DataSchemaRef;
use common_expression::Domain;
use common_expression::Expr;
use common_expression::FunctionContext;
use common_expression::Scalar;
use common_expression::TableSchemaRef;
use common_functions::scalars::BUILTIN_FUNCTIONS;
use storages_common_table_meta::meta::ClusterStatistics;
use storages_common_table_meta::meta::ColumnStatistics;
use storages_common_table_meta::meta::StatisticsOfColumns;

use crate::statistics_to_domain;

#[derive(Clone)]
pub struct PageFilter {
    expr: Expr<String>,
    func_ctx: FunctionContext,
    cluster_key_id: u32,

    // index of the cluster key inside the schema
    cluster_key_fields: Vec<DataField>,
}

impl PageFilter {
    pub fn try_create(
        func_ctx: FunctionContext,
        cluster_key_id: u32,
        cluster_keys: Vec<String>,
        exprs: &[Expr<String>],
        schema: TableSchemaRef,
    ) -> Result<Self> {
        let conjunction = exprs
            .iter()
            .cloned()
            .reduce(|lhs, rhs| {
                check_function(None, "and", &[], &[lhs, rhs], &BUILTIN_FUNCTIONS).unwrap()
            })
            .unwrap();

        let (new_expr, _) = ConstantFolder::fold(&conjunction, func_ctx, &BUILTIN_FUNCTIONS);
        let data_schema: DataSchemaRef = Arc::new((&schema).into());
        let cluster_key_fields = cluster_keys
            .iter()
            .map(|name| data_schema.field_with_name(name.as_str()).unwrap().clone())
            .collect::<Vec<_>>();

        Ok(Self {
            expr: new_expr,
            cluster_key_fields,
            cluster_key_id,
            func_ctx,
        })
    }

    pub fn try_eval_const(&self) -> Result<bool> {
        // Only return false, which means to skip this block, when the expression is folded to a constant false.
        Ok(!matches!(self.expr, Expr::Constant {
            scalar: Scalar::Boolean(false),
            ..
        }))
    }

    #[tracing::instrument(level = "debug", name = "page_filter_eval", skip_all)]
    pub fn eval(
        &self,
        stats: &Option<ClusterStatistics>
    ) -> Result<(bool, Option<Range<usize>>)> {
        let stats = match stats {
            Some(stats) => stats,
            None => return Ok((true, None)),
        };
        let min_values = match stats.pages {
            Some(ref pages) => pages,
            None => return Ok((true, None)),
        };
        
        let max_value = Scalar::Tuple(stats.max.clone());
        
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
            let f = &self.cluster_key_fields[idx];

            let stats = ColumnStatistics {
                min: min.clone(),
                max: max.clone(),
                null_count: 1,
                in_memory_size: 0,
                distinct_of_values: None,
            };
            let domain = statistics_to_domain(Some(&stats), &f.data_type());
            input_domains.insert(f.name().clone(), domain);
            
            // For Tuple scalars, if the first element is not equal, then the monotonically increasing property is broken.
            if min != max {
                break;
            }
        }

        let (new_expr, _) = ConstantFolder::fold_with_domain(
            &self.expr,
            input_domains,
            self.func_ctx,
            &BUILTIN_FUNCTIONS,
        );

        // Only return false, which means to skip this block, when the expression is folded to a constant false.
        Ok(!matches!(new_expr, Expr::Constant {
            scalar: Scalar::Boolean(false),
            ..
        }))
    }
}
