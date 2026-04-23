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

use super::ColumnIndex;
use super::Expr;
use super::FunctionCall;
use super::FunctionContext;
use super::FunctionDomain;
use super::FunctionEval;
use super::FunctionRegistry;
use super::Scalar;
use super::function_stat::DeriveStat;
use super::stat_distribution::ArgStat;
use super::stat_distribution::BorrowedDistribution;
use super::stat_distribution::Ndv;
use super::stat_distribution::OwnedDistribution;
use super::stat_distribution::ReturnStat;
use super::stat_distribution::StatArgs;
use super::stat_distribution::StatBinaryArg;
use super::stat_distribution::StatUnaryArg;
use crate::Constant;

pub struct StatEvaluator<'a> {
    func_ctx: &'a FunctionContext,
    fn_registry: &'a FunctionRegistry,
    cardinality: f64,
}

impl<'a> StatEvaluator<'a> {
    pub fn run<'s, I: ColumnIndex>(
        expr: &Expr<I>,
        func_ctx: &'a FunctionContext,
        fn_registry: &'a FunctionRegistry,
        cardinality: f64,
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
        if self.cardinality == 0.0 {
            return Ok(None);
        }
        match expr {
            Expr::Constant(Constant {
                scalar, data_type, ..
            }) => Ok(Some({
                let domain = scalar.as_ref().domain(data_type);
                let (ndv, null_count) = if scalar.is_null() {
                    (Ndv::Stat(0.0), self.cardinality.ceil() as u64)
                } else {
                    (Ndv::Stat(1.0), 0)
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
            Expr::Cast(_) | Expr::LambdaFunctionCall(_) => Ok(None),
        }
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
            Ok(res) => Ok(res),
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
