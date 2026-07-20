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

use std::borrow::Cow;
use std::collections::HashMap;
use std::ops::Range;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Constant;
use databend_common_expression::ConstantFolder;
use databend_common_expression::DataField;
use databend_common_expression::DataSchemaRef;
use databend_common_expression::Domain;
use databend_common_expression::FunctionContext;
use databend_common_expression::FunctionID;
use databend_common_expression::RemoteExpr;
use databend_common_expression::Scalar;
use databend_common_expression::TableSchemaRef;
use databend_common_expression::expr::*;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::visit_expr;
use databend_common_functions::BUILTIN_FUNCTIONS;
use databend_storages_common_table_meta::meta::ClusterStatistics;
use databend_storages_common_table_meta::meta::ColumnStatistics;

use super::eliminate_cast::*;
use crate::range_index::statistics_to_domain;

#[derive(Clone)]
pub struct PageIndex {
    expr: Expr<String>,
    column_refs: HashMap<String, DataType>,
    func_ctx: FunctionContext,
    cluster_key_id: u32,

    // index of the cluster key inside the schema
    cluster_key_fields: Vec<DataField>,
    cluster_key_sources: Vec<ClusterKeyDomainSource>,
}

#[derive(Clone)]
enum ClusterKeyDomainSource {
    None,
    Column { name: String, data_type: DataType },
    StringPrefix { name: String, data_type: DataType },
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

        let cluster_key_sources = cluster_key_fields
            .iter()
            .map(|field| ClusterKeyDomainSource::Column {
                name: field.name().clone(),
                data_type: field.data_type().clone(),
            })
            .collect();

        Ok(Self {
            column_refs: expr.column_refs(),
            expr: expr.clone(),
            cluster_key_fields,
            cluster_key_sources,
            cluster_key_id,
            func_ctx,
        })
    }

    pub fn try_create_with_exprs(
        func_ctx: FunctionContext,
        cluster_key_id: u32,
        cluster_keys: &[RemoteExpr<String>],
        expr: &Expr<String>,
    ) -> Result<Self> {
        let mut cluster_key_fields = Vec::with_capacity(cluster_keys.len());
        let mut cluster_key_sources = Vec::with_capacity(cluster_keys.len());
        for cluster_key in cluster_keys {
            cluster_key_fields.push(DataField::new(
                &cluster_key_field_name(cluster_key),
                remote_expr_data_type(cluster_key).clone(),
            ));
            cluster_key_sources.push(cluster_key_domain_source(cluster_key));
        }

        Ok(Self {
            column_refs: expr.column_refs(),
            expr: expr.clone(),
            cluster_key_fields,
            cluster_key_sources,
            cluster_key_id,
            func_ctx,
        })
    }

    pub fn try_apply_const(&self) -> Result<bool> {
        // if the exprs did not contains the first cluster key, we should return true
        if self.cluster_key_fields.is_empty()
            || !self.column_refs.iter().any(|column| {
                self.cluster_key_sources
                    .iter()
                    .filter_map(ClusterKeyDomainSource::column_name)
                    .any(|source| source == column.0)
            })
        {
            return Ok(true);
        }

        // Only return false, which means to skip this block, when the expression is folded to a constant false.
        Ok(!matches!(
            self.expr,
            Expr::Constant(Constant {
                scalar: Scalar::Boolean(false),
                ..
            })
        ))
    }

    #[fastrace::trace]
    pub fn apply(&self, stats: &Option<ClusterStatistics>) -> Result<(bool, Option<Range<usize>>)> {
        let Some(stats) = stats else {
            return Ok((true, None));
        };

        if self.cluster_key_id != stats.cluster_key_id {
            return Ok((true, None));
        }

        let max_value = Scalar::Tuple(stats.max().clone());
        let min_values: Vec<Scalar> = match stats.pages {
            Some(ref pages) => pages.clone(),
            None => {
                let min_value = Scalar::Tuple(stats.min().clone());
                return Ok((self.eval_single_page(&min_value, &max_value)?, None));
            }
        };

        if min_values.is_empty() {
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
            if let Some((column, domain)) = self.cluster_key_sources[idx].domain(min, max)
                && self.column_refs.contains_key(&column)
            {
                input_domains.insert(column, domain);
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

        let mut visitor = RewriteVisitor {
            input_domains,
            func_ctx: &self.func_ctx,
            fn_registry: &BUILTIN_FUNCTIONS,
        };

        let expr = match visit_expr(&self.expr, &mut visitor).unwrap() {
            Some(expr) => Cow::Owned(expr),
            None => Cow::Borrowed(&self.expr),
        };

        let (new_expr, _) = ConstantFolder::fold_with_domain(
            &expr,
            &visitor.input_domains,
            &self.func_ctx,
            &BUILTIN_FUNCTIONS,
        );

        // Only return false, which means to skip this block, when the expression is folded to a constant false.
        Ok(!matches!(
            new_expr,
            Expr::Constant(Constant {
                scalar: Scalar::Boolean(false),
                ..
            })
        ))
    }
}

impl ClusterKeyDomainSource {
    fn column_name(&self) -> Option<&str> {
        match self {
            ClusterKeyDomainSource::None => None,
            ClusterKeyDomainSource::Column { name, .. }
            | ClusterKeyDomainSource::StringPrefix { name, .. } => Some(name),
        }
    }

    fn domain(&self, min: &Scalar, max: &Scalar) -> Option<(String, Domain)> {
        match self {
            ClusterKeyDomainSource::None => None,
            ClusterKeyDomainSource::Column { name, data_type } => {
                let stat = ColumnStatistics::new(min.clone(), max.clone(), 1, 0, None);
                Some((name.clone(), statistics_to_domain(vec![&stat], data_type)))
            }
            ClusterKeyDomainSource::StringPrefix { name, data_type } => {
                let min = min.as_string()?;
                let max = max.as_string()?;
                let upper = string_prefix_upper_bound(max)?;
                let stat = ColumnStatistics::new(
                    Scalar::String(min.to_string()),
                    Scalar::String(upper),
                    1,
                    0,
                    None,
                );
                Some((name.clone(), statistics_to_domain(vec![&stat], data_type)))
            }
        }
    }
}

fn cluster_key_field_name(expr: &RemoteExpr<String>) -> String {
    match expr {
        RemoteExpr::ColumnRef { id, .. } => id.clone(),
        _ => expr.as_expr(&BUILTIN_FUNCTIONS).sql_display(),
    }
}

fn cluster_key_domain_source(expr: &RemoteExpr<String>) -> ClusterKeyDomainSource {
    match expr {
        RemoteExpr::ColumnRef { id, data_type, .. } => {
            if matches!(data_type.remove_nullable(), DataType::String) {
                ClusterKeyDomainSource::StringPrefix {
                    name: id.clone(),
                    data_type: data_type.clone(),
                }
            } else {
                ClusterKeyDomainSource::Column {
                    name: id.clone(),
                    data_type: data_type.clone(),
                }
            }
        }
        RemoteExpr::FunctionCall { id, args, .. }
            if matches!(function_name(id), "substr" | "substring")
                && args.len() == 3
                && is_one(&args[1]) =>
        {
            match &args[0] {
                RemoteExpr::ColumnRef { id, data_type, .. }
                    if matches!(data_type.remove_nullable(), DataType::String) =>
                {
                    ClusterKeyDomainSource::StringPrefix {
                        name: id.clone(),
                        data_type: data_type.clone(),
                    }
                }
                _ => ClusterKeyDomainSource::None,
            }
        }
        _ => ClusterKeyDomainSource::None,
    }
}

fn function_name(id: &FunctionID) -> &str {
    match id {
        FunctionID::Builtin { name, .. } | FunctionID::Factory { name, .. } => name,
    }
}

fn is_one(expr: &RemoteExpr<String>) -> bool {
    match expr {
        RemoteExpr::Cast { expr, .. } => is_one(expr),
        RemoteExpr::Constant {
            scalar:
                Scalar::Number(
                    NumberScalar::UInt8(1)
                    | NumberScalar::UInt16(1)
                    | NumberScalar::UInt32(1)
                    | NumberScalar::UInt64(1)
                    | NumberScalar::Int8(1)
                    | NumberScalar::Int16(1)
                    | NumberScalar::Int32(1)
                    | NumberScalar::Int64(1),
                ),
            ..
        } => true,
        _ => false,
    }
}

fn remote_expr_data_type(expr: &RemoteExpr<String>) -> &DataType {
    match expr {
        RemoteExpr::Constant { data_type, .. } => data_type,
        RemoteExpr::ColumnRef { data_type, .. } => data_type,
        RemoteExpr::Cast { dest_type, .. } => dest_type,
        RemoteExpr::FunctionCall { return_type, .. } => return_type,
        RemoteExpr::LambdaFunctionCall { return_type, .. } => return_type,
    }
}

fn string_prefix_upper_bound(prefix: &str) -> Option<String> {
    let mut chars = prefix.chars().collect::<Vec<_>>();
    for index in (0..chars.len()).rev() {
        let codepoint = chars[index] as u32;
        if let Some(next) = char::from_u32(codepoint.checked_add(1)?) {
            chars[index] = next;
            chars.truncate(index + 1);
            return Some(chars.into_iter().collect());
        }
    }
    None
}
