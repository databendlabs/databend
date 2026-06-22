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
use databend_common_expression::types::Decimal;
use databend_common_expression::types::DecimalDomain;
use databend_common_expression::types::DecimalScalar;
use databend_common_expression::types::NumberDomain;
use databend_common_expression::types::NumberScalar;
use databend_common_expression::types::SimpleDomain;
use databend_common_expression::types::i256;
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
    Column(String),
    NegatedColumn { column: String, data_type: DataType },
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
        let cluster_key_sources = cluster_keys
            .into_iter()
            .map(ClusterKeyDomainSource::Column)
            .collect::<Vec<_>>();

        Self::try_create_with_fields(
            func_ctx,
            cluster_key_id,
            cluster_key_fields,
            cluster_key_sources,
            expr,
        )
    }

    pub fn try_create_with_exprs(
        func_ctx: FunctionContext,
        cluster_key_id: u32,
        cluster_keys: &[RemoteExpr<String>],
        expr: &Expr<String>,
    ) -> Result<Self> {
        let (cluster_key_fields, cluster_key_sources): (Vec<_>, Vec<_>) = cluster_keys
            .iter()
            .map(|expr| {
                (
                    DataField::new(
                        &cluster_key_field_name(expr),
                        remote_expr_data_type(expr).clone(),
                    ),
                    cluster_key_domain_source(expr),
                )
            })
            .unzip();

        Self::try_create_with_fields(
            func_ctx,
            cluster_key_id,
            cluster_key_fields,
            cluster_key_sources,
            expr,
        )
    }

    fn try_create_with_fields(
        func_ctx: FunctionContext,
        cluster_key_id: u32,
        cluster_key_fields: Vec<DataField>,
        cluster_key_sources: Vec<ClusterKeyDomainSource>,
        expr: &Expr<String>,
    ) -> Result<Self> {
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
            || !self.column_refs.iter().any(|c| {
                self.cluster_key_sources
                    .iter()
                    .filter_map(ClusterKeyDomainSource::column)
                    .any(|f| f == c.0)
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
        let min_values: Vec<Scalar> = match stats.pages {
            Some(ref pages) => pages.clone(),
            None => return Ok((true, None)),
        };

        let max_value = Scalar::Tuple(stats.max().clone());

        if self.cluster_key_id != stats.cluster_key_id {
            return Ok((true, None));
        }

        let pages = min_values.len();
        if pages == 0 {
            return Ok((true, None));
        }

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
            let f = &self.cluster_key_fields[idx];
            if let Some((column_ref, domain)) =
                self.cluster_key_sources[idx].domain(min, max, f.data_type())
                && self.column_refs.contains_key(&column_ref)
            {
                input_domains.insert(column_ref, domain);
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

fn cluster_key_field_name(expr: &RemoteExpr<String>) -> String {
    match expr {
        RemoteExpr::ColumnRef { id, .. } => id.clone(),
        _ => expr.as_expr(&BUILTIN_FUNCTIONS).sql_display(),
    }
}

impl ClusterKeyDomainSource {
    fn column(&self) -> Option<&String> {
        match self {
            ClusterKeyDomainSource::None => None,
            ClusterKeyDomainSource::Column(column) => Some(column),
            ClusterKeyDomainSource::NegatedColumn { column, .. } => Some(column),
        }
    }

    fn domain(
        &self,
        min: &Scalar,
        max: &Scalar,
        cluster_key_data_type: &DataType,
    ) -> Option<(String, Domain)> {
        match self {
            ClusterKeyDomainSource::None => None,
            ClusterKeyDomainSource::Column(column) => {
                let stat = ColumnStatistics::new(min.clone(), max.clone(), 1, 0, None);
                let domain = statistics_to_domain(vec![&stat], cluster_key_data_type);
                Some((column.clone(), domain))
            }
            ClusterKeyDomainSource::NegatedColumn { column, data_type } => {
                negated_domain(min, max, cluster_key_data_type, data_type)
                    .map(|domain| (column.clone(), domain))
            }
        }
    }
}

fn cluster_key_domain_source(expr: &RemoteExpr<String>) -> ClusterKeyDomainSource {
    match expr {
        RemoteExpr::ColumnRef { id, .. } => ClusterKeyDomainSource::Column(id.clone()),
        RemoteExpr::FunctionCall { id, args, .. } if function_name(id) == "minus" => {
            match args.as_slice() {
                [RemoteExpr::ColumnRef { id, data_type, .. }] => {
                    ClusterKeyDomainSource::NegatedColumn {
                        column: id.clone(),
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

fn remote_expr_data_type(expr: &RemoteExpr<String>) -> &DataType {
    match expr {
        RemoteExpr::Constant { data_type, .. } => data_type,
        RemoteExpr::ColumnRef { data_type, .. } => data_type,
        RemoteExpr::Cast { dest_type, .. } => dest_type,
        RemoteExpr::FunctionCall { return_type, .. } => return_type,
        RemoteExpr::LambdaFunctionCall { return_type, .. } => return_type,
    }
}

fn negated_domain(
    min: &Scalar,
    max: &Scalar,
    cluster_key_data_type: &DataType,
    source_data_type: &DataType,
) -> Option<Domain> {
    if let Some(domain) = negated_decimal_domain(min, max, cluster_key_data_type, source_data_type)
    {
        return Some(domain);
    }

    let min = min.as_number()?;
    let max = max.as_number()?;
    match (min, max, cluster_key_data_type, source_data_type) {
        (
            NumberScalar::Int16(min),
            NumberScalar::Int16(max),
            DataType::Number(databend_common_expression::types::NumberDataType::Int16),
            DataType::Number(databend_common_expression::types::NumberDataType::Int8),
        ) => Some(Domain::Number(NumberDomain::Int8(SimpleDomain {
            min: i8::try_from(max.checked_neg()?).ok()?,
            max: i8::try_from(min.checked_neg()?).ok()?,
        }))),
        (
            NumberScalar::Int32(min),
            NumberScalar::Int32(max),
            DataType::Number(databend_common_expression::types::NumberDataType::Int32),
            DataType::Number(databend_common_expression::types::NumberDataType::Int16),
        ) => Some(Domain::Number(NumberDomain::Int16(SimpleDomain {
            min: i16::try_from(max.checked_neg()?).ok()?,
            max: i16::try_from(min.checked_neg()?).ok()?,
        }))),
        (
            NumberScalar::Int64(min),
            NumberScalar::Int64(max),
            DataType::Number(databend_common_expression::types::NumberDataType::Int64),
            DataType::Number(databend_common_expression::types::NumberDataType::Int32),
        ) => Some(Domain::Number(NumberDomain::Int32(SimpleDomain {
            min: i32::try_from(max.checked_neg()?).ok()?,
            max: i32::try_from(min.checked_neg()?).ok()?,
        }))),
        (
            NumberScalar::Int64(min),
            NumberScalar::Int64(max),
            DataType::Number(databend_common_expression::types::NumberDataType::Int64),
            DataType::Number(databend_common_expression::types::NumberDataType::Int64),
        ) => {
            let source_min = max.checked_neg()?;
            let source_max = min.checked_neg()?;
            Some(Domain::Number(NumberDomain::Int64(SimpleDomain {
                min: source_min,
                max: source_max,
            })))
        }
        _ => None,
    }
}

fn negated_decimal_domain(
    min: &Scalar,
    max: &Scalar,
    cluster_key_data_type: &DataType,
    source_data_type: &DataType,
) -> Option<Domain> {
    let source_size = source_data_type.as_decimal()?;
    let cluster_key_size = cluster_key_data_type.as_decimal()?;
    if source_size != cluster_key_size {
        return None;
    }

    match (min.as_decimal()?, max.as_decimal()?) {
        (DecimalScalar::Decimal64(min, min_size), DecimalScalar::Decimal64(max, max_size))
            if min_size == source_size && max_size == source_size =>
        {
            let source_min = max.checked_neg()?;
            let source_max = min.checked_neg()?;
            if !(i64::DECIMAL_MIN..=i64::DECIMAL_MAX).contains(&source_min)
                || !(i64::DECIMAL_MIN..=i64::DECIMAL_MAX).contains(&source_max)
            {
                return None;
            }
            Some(Domain::Decimal(DecimalDomain::Decimal64(
                SimpleDomain {
                    min: source_min,
                    max: source_max,
                },
                *source_size,
            )))
        }
        (DecimalScalar::Decimal128(min, min_size), DecimalScalar::Decimal128(max, max_size))
            if min_size == source_size && max_size == source_size =>
        {
            let source_min = max.checked_neg()?;
            let source_max = min.checked_neg()?;
            if !(i128::DECIMAL_MIN..=i128::DECIMAL_MAX).contains(&source_min)
                || !(i128::DECIMAL_MIN..=i128::DECIMAL_MAX).contains(&source_max)
            {
                return None;
            }
            Some(Domain::Decimal(DecimalDomain::Decimal128(
                SimpleDomain {
                    min: source_min,
                    max: source_max,
                },
                *source_size,
            )))
        }
        (DecimalScalar::Decimal256(min, min_size), DecimalScalar::Decimal256(max, max_size))
            if min_size == source_size && max_size == source_size =>
        {
            let source_min = max.checked_neg()?;
            let source_max = min.checked_neg()?;
            if !(i256::DECIMAL_MIN..=i256::DECIMAL_MAX).contains(&source_min)
                || !(i256::DECIMAL_MIN..=i256::DECIMAL_MAX).contains(&source_max)
            {
                return None;
            }
            Some(Domain::Decimal(DecimalDomain::Decimal256(
                SimpleDomain {
                    min: source_min,
                    max: source_max,
                },
                *source_size,
            )))
        }
        _ => None,
    }
}
