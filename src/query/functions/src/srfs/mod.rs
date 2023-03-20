// Copyright 2022 Datafuse Labs.
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

mod check;
mod evaluator;
mod register;

use std::collections::HashMap;
use std::fmt::Debug;
use std::fmt::Formatter;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::types::AnyType;
use common_expression::types::DataType;
use common_expression::ColumnIndex;
use common_expression::Expr;
use common_expression::RemoteExpr;
use common_expression::ScalarRef;
use common_expression::Value;
use ctor::ctor;
pub use evaluator::SrfEvaluator;
use serde::Deserialize;
use serde::Serialize;

use crate::scalars::BUILTIN_FUNCTIONS;
use crate::srfs::check::try_check_srf;
use crate::srfs::register::builtin_set_returning_functions;

#[ctor]
pub static BUILTIN_SET_RETURNING_FUNCTIONS: SetReturningFunctionRegistry =
    builtin_set_returning_functions();

pub type SetReturningFunctionID = (String, usize);

#[derive(Clone)]
pub struct SrfExpr<Index: ColumnIndex = usize> {
    pub id: SetReturningFunctionID,
    pub srf: Arc<SetReturningFunction>,
    pub args: Vec<Expr<Index>>,
    pub generic_types: Vec<DataType>,
    pub return_types: Vec<DataType>,
}

impl SrfExpr {
    pub fn sql_display(&self) -> String {
        let mut args = vec![];
        for arg in &self.args {
            args.push(arg.sql_display());
        }
        format!("{}({})", self.id.0, args.join(", "))
    }
}

impl Debug for SrfExpr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SrfExpr")
            .field("id", &self.id)
            .field("args", &self.args)
            .field("generic_types", &self.generic_types)
            .field("return_types", &self.return_types)
            .finish()
    }
}

impl SrfExpr {
    pub fn into_remote_srf_expr(self) -> RemoteSrfExpr {
        let mut args = vec![];
        for arg in self.args {
            args.push(arg.as_remote_expr());
        }
        RemoteSrfExpr {
            id: self.id,
            args,
            generic_types: self.generic_types,
            return_types: self.return_types,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct RemoteSrfExpr<Index: ColumnIndex = usize> {
    pub id: SetReturningFunctionID,
    pub args: Vec<RemoteExpr<Index>>,
    pub generic_types: Vec<DataType>,
    pub return_types: Vec<DataType>,
}

impl RemoteSrfExpr {
    pub fn into_srf_expr(self) -> SrfExpr {
        let mut args = vec![];
        for arg in self.args {
            args.push(arg.as_expr(&BUILTIN_FUNCTIONS));
        }
        SrfExpr {
            id: self.id.clone(),
            srf: BUILTIN_SET_RETURNING_FUNCTIONS.get(&self.id).unwrap(),
            args,
            generic_types: self.generic_types,
            return_types: self.return_types,
        }
    }
}

#[allow(clippy::type_complexity)]
pub struct SetReturningFunction {
    pub name: String,
    pub arg_types: Vec<DataType>,
    pub return_types: Vec<DataType>,

    pub eval: Box<dyn Fn(&[ScalarRef], &[DataType]) -> (Vec<Value<AnyType>>, usize) + Send + Sync>,
}

#[derive(Default)]
pub struct SetReturningFunctionRegistry {
    pub srfs: HashMap<String, Vec<Arc<SetReturningFunction>>>,
}

impl SetReturningFunctionRegistry {
    pub fn register<F>(
        &mut self,
        name: &str,
        arg_types: &[DataType],
        return_types: &[DataType],
        eval: F,
    ) where
        F: Fn(&[ScalarRef], &[DataType]) -> (Vec<Value<AnyType>>, usize) + Send + Sync + 'static,
    {
        if return_types.iter().any(|t| !t.is_nullable()) {
            panic!("return type of srf must be nullable");
        }

        let name = name.to_string();
        let srf = Arc::new(SetReturningFunction {
            name: name.to_string(),
            arg_types: arg_types.to_vec(),
            return_types: return_types.to_vec(),
            eval: Box::new(eval),
        });
        self.srfs.entry(name).or_insert_with(Vec::new).push(srf);
    }

    pub fn contains(&self, name: &str) -> bool {
        self.srfs.contains_key(name)
    }

    pub fn search_candidates<Index: ColumnIndex>(
        &self,
        name: &str,
        args: &[Expr<Index>],
    ) -> Vec<(Arc<SetReturningFunction>, usize)> {
        let mut candidates = vec![];
        self.srfs
            .get(name)
            .unwrap_or(&vec![])
            .iter()
            .enumerate()
            .for_each(|(index, srf)| {
                if srf.name == name && srf.arg_types.len() == args.len() {
                    candidates.push((srf.clone(), index));
                }
            });

        candidates
    }

    pub fn get(&self, id: &SetReturningFunctionID) -> Option<Arc<SetReturningFunction>> {
        self.srfs
            .get(&id.0)
            .and_then(|srfs| srfs.get(id.1))
            .cloned()
    }
}

pub fn check_srf<Index: ColumnIndex>(
    name: &str,
    args: &[Expr<Index>],
    registry: &SetReturningFunctionRegistry,
) -> Result<SrfExpr<Index>> {
    let candidates = registry.search_candidates(name, args);

    if candidates.is_empty() && !registry.contains(name) {
        return Err(ErrorCode::UnknownFunction(format!(
            "srf `{name}` does not exist"
        )));
    }

    let mut fail_reasons = Vec::with_capacity(candidates.len());
    for (srf, index) in candidates.iter() {
        match try_check_srf(&srf.arg_types, &srf.return_types, args) {
            Ok((checked_args, return_types, generic_types)) => {
                return Ok(SrfExpr {
                    id: (srf.name.clone(), *index),
                    srf: srf.clone(),
                    args: checked_args,
                    generic_types,
                    return_types,
                });
            }
            Err(err) => fail_reasons.push(err),
        }
    }

    Err(ErrorCode::SemanticError(format!(
        "no overload satisfies {name}({}): {}",
        args.iter()
            .map(|x| x.data_type().to_string())
            .collect::<Vec<_>>()
            .join(", "),
        fail_reasons
            .into_iter()
            .map(|e| e.message())
            .collect::<Vec<_>>()
            .join(", ")
    )))
}
