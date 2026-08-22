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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use super::Cast;
use super::ColumnIndex;
use super::ColumnRef;
use super::ConstantFolder;
use super::Expr;
use super::FunctionCall;
use super::FunctionContext;
use super::FunctionDomain;
use super::FunctionEval;
use super::FunctionRegistry;
use super::Scalar;
use super::conversion::classify_conversion;
use super::function_stat::DeriveStat;
use super::stat_distribution::ArgStat;
use super::stat_distribution::BorrowedDistribution;
use super::stat_distribution::NdvEstimate;
use super::stat_distribution::OwnedDistribution;
use super::stat_distribution::ReturnStat;
use super::stat_distribution::StatArgs;
use super::stat_distribution::StatBinaryArg;
use super::stat_distribution::StatCardinality;
use super::stat_distribution::StatCount;
use super::stat_distribution::StatEstimate;
use super::stat_distribution::StatUnaryArg;
use crate::Constant;

pub struct StatEvaluator<'a> {
    func_ctx: &'a FunctionContext,
    fn_registry: &'a FunctionRegistry,
    cardinality: StatCardinality,
}

impl<'a> StatEvaluator<'a> {
    pub fn run<'s, I: ColumnIndex>(
        expr: &Expr<I>,
        func_ctx: &'a FunctionContext,
        fn_registry: &'a FunctionRegistry,
        cardinality: StatCardinality,
        input_stats: &'s HashMap<I, ArgStat<'s>>,
    ) -> Result<Option<CowStat<'s>>> {
        let evaluator = StatEvaluator {
            func_ctx,
            fn_registry,
            cardinality,
        };
        evaluator.eval(expr, input_stats)
    }

    fn eval<'s, I: ColumnIndex>(
        &'a self,
        expr: &Expr<I>,
        input_stats: &'s HashMap<I, ArgStat<'_>>,
    ) -> Result<Option<CowStat<'s>>> {
        if self.cardinality.is_zero() {
            return Ok(None);
        }
        match expr {
            Expr::Constant(Constant {
                scalar, data_type, ..
            }) => Ok(Some({
                let domain = scalar.as_ref().domain(data_type);
                let (ndv, null_count) = if scalar.is_null() {
                    (
                        NdvEstimate::proven_exact(0.0),
                        self.cardinality.as_null_count(),
                    )
                } else {
                    (NdvEstimate::proven_exact(1.0), StatCount::exact(0))
                };
                CowStat::Owned(ReturnStat {
                    domain,
                    ndv,
                    null_count,
                    distribution: OwnedDistribution::Unknown,
                })
            })),
            Expr::ColumnRef(col) => Ok(input_stats
                .get(&col.id)
                .map(|stat| CowStat::Borrowed(stat.clone()))),
            Expr::FunctionCall(call) => Ok(self
                .eval_function_call(call, input_stats)?
                .map(CowStat::Owned)),
            Expr::Cast(cast) => Ok(self.eval_cast(cast, input_stats)?.map(CowStat::Owned)),
            Expr::LambdaFunctionCall(_) => Ok(None),
        }
    }

    fn eval_cast<'s, I: ColumnIndex>(
        &'a self,
        cast: &Cast<I>,
        input_stats: &'s HashMap<I, ArgStat<'_>>,
    ) -> Result<Option<ReturnStat>> {
        let src_type = cast.expr.data_type();
        if cast.is_try || !classify_conversion(src_type, &cast.dest_type).is_lossless_injective() {
            return Ok(None);
        }

        let Some(input) = self.eval(&cast.expr, input_stats)? else {
            return Ok(None);
        };
        let input = input.as_ref();

        // Reuse the cast domain implementation without making statistics
        // evaluation depend on the physical value evaluator. The synthetic
        // column represents the already-derived statistics of the inner expr.
        let expr = Expr::Cast(Cast {
            span: cast.span,
            is_try: false,
            expr: Box::new(Expr::ColumnRef(ColumnRef {
                span: cast.span,
                id: 0,
                data_type: src_type.clone(),
                display_name: String::new(),
            })),
            dest_type: cast.dest_type.clone(),
        });
        let input_domains = HashMap::from([(0, input.domain.clone())]);
        let (_, Some(domain)) = ConstantFolder::fold_with_domain(
            &expr,
            &input_domains,
            self.func_ctx,
            self.fn_registry,
        ) else {
            return Ok(None);
        };

        let stat = ReturnStat {
            domain,
            // A lossless injective cast preserves distinct values and NULLs.
            // Histograms are typed, so their boundaries cannot be reused.
            ndv: input.ndv,
            null_count: input.null_count,
            distribution: OwnedDistribution::Unknown,
        };
        if let Err(msg) = stat.check_consistency_with_type(Some(&cast.dest_type)) {
            if cfg!(debug_assertions) {
                return Err(ErrorCode::Internal(format!(
                    "Failed to derive statistics for cast: {msg}"
                )));
            }
            log::warn!(msg; "Derived invalid cast statistics");
            return Ok(None);
        }
        Ok(Some(stat))
    }

    fn eval_function_call<'s, I: ColumnIndex>(
        &'a self,
        call: &FunctionCall<I>,
        input_stats: &'s HashMap<I, ArgStat<'s>>,
    ) -> Result<Option<ReturnStat>> {
        let FunctionEval::Scalar {
            derive_stat: Some(derive_stat),
            ..
        } = &call.function.eval
        else {
            return Ok(None);
        };

        let mut return_stat = Vec::new();
        for arg in &call.args {
            let Some(stat) = self.eval(arg, input_stats)? else {
                return Ok(None);
            };
            return_stat.push(stat);
        }
        let args: Vec<_> = return_stat.iter().map(CowStat::as_ref).collect();
        let res = derive_stat.stat_eval(self.func_ctx, StatArgs {
            cardinality: self.cardinality,
            args: &args,
        });

        match res {
            Ok(None) => Ok(None),
            Ok(Some(res)) => {
                let return_type = call.return_type.remove_generics(&call.generics);
                if let Err(msg) = res.check_consistency_with_type(Some(&return_type)) {
                    if cfg!(debug_assertions) {
                        return Err(ErrorCode::Internal(format!(
                            "Failed to derive statistics for function {:?}: {msg}",
                            call.function.signature.name
                        )));
                    }
                    log::warn!(function = call.function.signature.name, msg; "Derived invalid function statistics");
                    return Ok(None);
                }
                Ok(Some(res))
            }
            Err(msg) => {
                if cfg!(debug_assertions) {
                    Err(ErrorCode::Internal(format!(
                        "Failed to derive statistics for function {:?}: {msg}",
                        call.function.signature.name
                    )))
                } else {
                    log::warn!(function = call.function.signature.name, msg; "Failed to derive statistics for function");
                    Ok(None)
                }
            }
        }
    }
}

pub enum CowStat<'a> {
    Borrowed(ArgStat<'a>),
    Owned(ReturnStat),
}

impl<'a> CowStat<'a> {
    pub fn as_ref(&self) -> ArgStat<'_> {
        match *self {
            CowStat::Borrowed(ArgStat {
                ref domain,
                ndv,
                null_count,
                distribution,
            }) => ArgStat {
                domain: domain.clone(),
                ndv,
                null_count,
                distribution,
            },
            CowStat::Owned(ReturnStat {
                ref domain,
                ndv,
                null_count,
                ref distribution,
            }) => ArgStat {
                domain: domain.clone(),
                ndv,
                null_count,
                distribution: distribution.as_borrowed_distribution(),
            },
        }
    }

    pub fn into_owned(self) -> ReturnStat {
        match self {
            CowStat::Borrowed(ArgStat {
                domain,
                ndv,
                null_count,
                distribution,
            }) => ReturnStat {
                domain,
                ndv,
                null_count,
                distribution: match distribution {
                    BorrowedDistribution::Unknown => OwnedDistribution::Unknown,
                    BorrowedDistribution::Histogram(histogram) => {
                        OwnedDistribution::Histogram(histogram.clone())
                    }
                    BorrowedDistribution::Boolean(distribution) => {
                        OwnedDistribution::Boolean(distribution)
                    }
                },
            },
            CowStat::Owned(owned) => owned,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::types::DataType;
    use crate::types::NumberDataType;
    use crate::types::number::NumberScalar;

    #[test]
    fn test_constant_null_uses_exact_input_cardinality() {
        let expr = Expr::<usize>::constant(Scalar::Null, Some(DataType::Null));
        let registry = FunctionRegistry::empty();
        let stat = StatEvaluator::run(
            &expr,
            &FunctionContext::default(),
            &registry,
            StatCardinality::exact(7),
            &HashMap::new(),
        )
        .unwrap()
        .unwrap()
        .into_owned();

        assert_eq!(stat.null_count, StatCount::exact(7));
    }

    #[test]
    fn test_lossless_cast_preserves_basic_statistics() {
        let src_type = DataType::Number(NumberDataType::UInt8);
        let dest_type = src_type.clone().wrap_nullable();
        let expr = Expr::Cast(Cast {
            span: None,
            is_try: false,
            expr: Box::new(Expr::ColumnRef(ColumnRef {
                span: None,
                id: 0,
                data_type: src_type.clone(),
                display_name: "c0".to_string(),
            })),
            dest_type: dest_type.clone(),
        });
        let domain = crate::Domain::from_min_max(
            Scalar::Number(NumberScalar::UInt8(1)),
            Scalar::Number(NumberScalar::UInt8(3)),
            &src_type,
        );
        let input_stats = HashMap::from([(0, ArgStat {
            domain,
            ndv: NdvEstimate::exact(3.0),
            null_count: StatCount::exact(0),
            distribution: BorrowedDistribution::Unknown,
        })]);

        let stat = StatEvaluator::run(
            &expr,
            &FunctionContext::default(),
            &FunctionRegistry::empty(),
            StatCardinality::exact(3),
            &input_stats,
        )
        .unwrap()
        .unwrap()
        .into_owned();

        assert!(stat.domain.matches_data_type(&dest_type));
        assert_eq!(stat.ndv, NdvEstimate::exact(3.0));
        assert_eq!(stat.null_count, StatCount::exact(0));
        assert!(matches!(stat.distribution, OwnedDistribution::Unknown));
    }

    #[test]
    fn test_lossy_cast_is_not_derived() {
        let expr = Expr::Cast(Cast {
            span: None,
            is_try: false,
            expr: Box::new(Expr::ColumnRef(ColumnRef {
                span: None,
                id: 0,
                data_type: DataType::Number(NumberDataType::Int64),
                display_name: "c0".to_string(),
            })),
            dest_type: DataType::Number(NumberDataType::UInt8),
        });

        assert!(
            StatEvaluator::run(
                &expr,
                &FunctionContext::default(),
                &FunctionRegistry::empty(),
                StatCardinality::exact(3),
                &HashMap::new(),
            )
            .unwrap()
            .is_none()
        );
    }
}
