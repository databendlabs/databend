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

use std::sync::Arc;

use common_catalog::table_context::TableContext;
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
use common_expression::Domain;
use common_expression::Expr;
use common_expression::FunctionContext;
use common_expression::RemoteExpr;
use common_expression::Scalar;
use common_expression::TableSchemaRef;
use common_functions_v2::scalars::BUILTIN_FUNCTIONS;
use common_storages_table_meta::meta::ColumnStatistics;
use common_storages_table_meta::meta::StatisticsOfColumns;

#[derive(Clone)]
pub struct RangeFilter {
    schema: TableSchemaRef,
    expr: Expr<String>,
    fn_ctx: FunctionContext,
}

impl RangeFilter {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
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

        Ok(Self {
            schema,
            expr: conjunction,
            fn_ctx: ctx.try_get_function_context()?,
        })
    }

    pub fn try_eval_const(&self) -> Result<bool> {
        let input_domains = self
            .expr
            .column_refs()
            .into_iter()
            .map(|(name, ty)| {
                let domain = Domain::full(&ty);
                Ok((name, domain))
            })
            .collect::<Result<_>>()?;

        let folder = ConstantFolder::new(input_domains, self.fn_ctx, &BUILTIN_FUNCTIONS);
        let (new_expr, _) = folder.fold(&self.expr);

        // Only return false, which means to skip this block, when the expression is folded to a constant false.
        Ok(!matches!(new_expr, Expr::Constant {
            scalar: Scalar::Boolean(false),
            ..
        }))
    }

    #[tracing::instrument(level = "debug", name = "range_filter_eval", skip_all)]
    pub fn eval(&self, stats: &StatisticsOfColumns) -> Result<bool> {
        let input_domains = self
            .expr
            .column_refs()
            .into_iter()
            .map(|(name, ty)| {
                let offset = self.schema.index_of(&name)?;
                let domain = statistics_to_domain(&stats[&(offset as u32)], &ty);
                Ok((name, domain))
            })
            .collect::<Result<_>>()?;

        let folder = ConstantFolder::new(input_domains, self.fn_ctx, &BUILTIN_FUNCTIONS);
        let (new_expr, _) = folder.fold(&self.expr);

        // Only return false, which means to skip this block, when the expression is folded to a constant false.
        Ok(!matches!(new_expr, Expr::Constant {
            scalar: Scalar::Boolean(false),
            ..
        }))
    }
}

fn statistics_to_domain(stat: &ColumnStatistics, data_type: &DataType) -> Domain {
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
            let domain = statistics_to_domain(stat, ty);
            Domain::Nullable(NullableDomain {
                has_null: stat.null_count > 0,
                value: Some(Box::new(domain)),
            })
        }
        // Unsupported data type
        _ => Domain::full(data_type),
    })
}
