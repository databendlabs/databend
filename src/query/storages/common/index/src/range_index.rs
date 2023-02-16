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
use common_expression::ColumnId;
use common_expression::ConstantFolder;
use common_expression::Domain;
use common_expression::Expr;
use common_expression::FunctionContext;
use common_expression::Scalar;
use common_expression::TableSchemaRef;
use common_functions::scalars::BUILTIN_FUNCTIONS;
use storages_common_table_meta::meta::ColumnStatistics;
use storages_common_table_meta::meta::StatisticsOfColumns;

use crate::Index;

#[derive(Clone)]
pub struct RangeIndex {
    expr: Expr<String>,
    func_ctx: FunctionContext,
    column_ids: HashMap<String, ColumnId>,
}

impl RangeIndex {
    pub fn try_create(
        func_ctx: FunctionContext,
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

        let leaf_fields = schema.leaf_fields();
        let column_ids = leaf_fields.iter().fold(
            HashMap::with_capacity(leaf_fields.len()),
            |mut acc, field| {
                acc.insert(field.name().clone(), field.column_id());
                acc
            },
        );

        Ok(Self {
            expr: new_expr,
            func_ctx,
            column_ids,
        })
    }

    pub fn try_apply_const(&self) -> Result<bool> {
        // Only return false, which means to skip this block, when the expression is folded to a constant false.
        Ok(!matches!(self.expr, Expr::Constant {
            scalar: Scalar::Boolean(false),
            ..
        }))
    }

    #[tracing::instrument(level = "debug", name = "range_filter_eval", skip_all)]
    pub fn apply(&self, stats: &StatisticsOfColumns) -> Result<bool> {
        let input_domains = self
            .expr
            .column_refs()
            .into_iter()
            .map(|(name, ty)| {
                let stat = match self.column_ids.get(&name) {
                    Some(column_id) => stats.get(column_id),
                    None => None,
                };
                let domain = statistics_to_domain(stat, &ty);
                Ok((name, domain))
            })
            .collect::<Result<_>>()?;

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

pub fn statistics_to_domain(stat: Option<&ColumnStatistics>, data_type: &DataType) -> Domain {
    if stat.is_none() {
        return Domain::full(data_type);
    }
    let stat = stat.unwrap();
    if stat.min.is_null() || stat.max.is_null() {
        return Domain::Nullable(NullableDomain {
            has_null: true,
            value: None,
        });
    }
    with_number_mapped_type!(|NUM_TYPE| match data_type {
        DataType::Number(NumberDataType::NUM_TYPE) => {
            NumberType::<NUM_TYPE>::upcast_domain(SimpleDomain {
                min: NumberType::<NUM_TYPE>::try_downcast_scalar(&stat.min.as_ref()).unwrap(),
                max: NumberType::<NUM_TYPE>::try_downcast_scalar(&stat.max.as_ref()).unwrap(),
            })
        }
        DataType::String => Domain::String(StringDomain {
            min: StringType::try_downcast_scalar(&stat.min.as_ref())
                .unwrap()
                .to_vec(),
            max: Some(
                StringType::try_downcast_scalar(&stat.max.as_ref())
                    .unwrap()
                    .to_vec()
            ),
        }),
        DataType::Timestamp => TimestampType::upcast_domain(SimpleDomain {
            min: TimestampType::try_downcast_scalar(&stat.min.as_ref()).unwrap(),
            max: TimestampType::try_downcast_scalar(&stat.max.as_ref()).unwrap(),
        }),
        DataType::Date => DateType::upcast_domain(SimpleDomain {
            min: DateType::try_downcast_scalar(&stat.min.as_ref()).unwrap(),
            max: DateType::try_downcast_scalar(&stat.max.as_ref()).unwrap(),
        }),
        DataType::Nullable(ty) => {
            let domain = statistics_to_domain(Some(stat), ty);
            Domain::Nullable(NullableDomain {
                has_null: stat.null_count > 0,
                value: Some(Box::new(domain)),
            })
        }
        // Unsupported data type
        _ => Domain::full(data_type),
    })
}

impl Index for RangeIndex {}
