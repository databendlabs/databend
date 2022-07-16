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

use std::collections::HashMap;

use crate::expression::Expr;
use crate::expression::Literal;
use crate::expression::RawExpr;
use crate::function::FunctionRegistry;
use crate::function::FunctionSignature;
use crate::types::DataType;

// TODO: return result instead of option
pub fn check(ast: &RawExpr, fn_registry: &FunctionRegistry) -> Option<(Expr, DataType)> {
    match ast {
        RawExpr::Literal(lit) => {
            let ty = check_literal(lit);
            Some((Expr::Literal(lit.clone()), ty))
        }
        RawExpr::ColumnRef { id, data_type } => {
            Some((Expr::ColumnRef { id: *id }, data_type.clone()))
        }
        RawExpr::FunctionCall { name, args, params } => {
            let (mut args_expr, mut args_type) = (Vec::new(), Vec::new());

            for arg in args {
                let (arg, ty) = check(arg, fn_registry)?;
                args_expr.push(arg);
                args_type.push(ty);
            }

            check_function(name, params, &args_expr, &args_type, fn_registry)
        }
    }
}

pub fn check_literal(literal: &Literal) -> DataType {
    match literal {
        Literal::Null => DataType::Null,
        Literal::Int8(_) => DataType::Int8,
        Literal::Int16(_) => DataType::Int16,
        Literal::UInt8(_) => DataType::UInt8,
        Literal::UInt16(_) => DataType::UInt16,
        Literal::Boolean(_) => DataType::Boolean,
        Literal::String(_) => DataType::String,
    }
}

pub fn check_function(
    name: &str,
    params: &[usize],
    args: &[Expr],
    args_type: &[DataType],
    fn_registry: &FunctionRegistry,
) -> Option<(Expr, DataType)> {
    for (id, func) in fn_registry.search_candidates(name, params, args_type) {
        if let Some((checked_args, return_ty, generics)) =
            try_check_function(args, args_type, &func.signature)
        {
            return Some((
                Expr::FunctionCall {
                    id,
                    function: func.clone(),
                    generics,
                    args: checked_args,
                },
                return_ty,
            ));
        }
    }

    None
}

#[derive(Debug)]
pub struct Subsitution(pub HashMap<usize, DataType>);

impl Subsitution {
    pub fn empty() -> Self {
        Subsitution(HashMap::new())
    }

    pub fn equation(idx: usize, ty: DataType) -> Self {
        let mut subst = Self::empty();
        subst.0.insert(idx, ty);
        subst
    }

    pub fn merge(mut self, other: Self) -> Option<Self> {
        for (idx, ty1) in other.0 {
            if let Some(ty2) = self.0.remove(&idx) {
                let common_ty = common_super_type(ty1, ty2)?;
                self.0.insert(idx, common_ty);
            } else {
                self.0.insert(idx, ty1);
            }
        }

        Some(self)
    }

    pub fn apply(&self, ty: DataType) -> Option<DataType> {
        match ty {
            DataType::Generic(idx) => self.0.get(&idx).cloned(),
            DataType::Nullable(box ty) => Some(DataType::Nullable(Box::new(self.apply(ty)?))),
            DataType::Array(box ty) => Some(DataType::Array(Box::new(self.apply(ty)?))),
            ty => Some(ty),
        }
    }
}

#[allow(clippy::type_complexity)]
pub fn try_check_function(
    args: &[Expr],
    args_type: &[DataType],
    sig: &FunctionSignature,
) -> Option<(Vec<Expr>, DataType, Vec<DataType>)> {
    assert_eq!(args.len(), sig.args_type.len());

    let substs = args_type
        .iter()
        .zip(&sig.args_type)
        .map(|(src_ty, dest_ty)| unify(src_ty, dest_ty))
        .collect::<Option<Vec<_>>>()?;
    let subst = substs
        .into_iter()
        .try_reduce(|subst1, subst2| subst1.merge(subst2))?
        .unwrap_or_else(Subsitution::empty);

    let checked_args = args
        .iter()
        .zip(args_type)
        .zip(&sig.args_type)
        .map(|((arg, arg_type), sig_type)| {
            let sig_type = subst.apply(sig_type.clone())?;
            Some(if *arg_type == sig_type {
                arg.clone()
            } else {
                Expr::Cast {
                    expr: Box::new(arg.clone()),
                    dest_type: sig_type,
                }
            })
        })
        .collect::<Option<Vec<_>>>()?;

    let return_type = subst.apply(sig.return_type.clone())?;

    let generics = subst
        .0
        .keys()
        .cloned()
        .max()
        .map(|max_generic_idx| {
            (0..max_generic_idx + 1)
                .map(|idx| match subst.0.get(&idx) {
                    Some(ty) => ty.clone(),
                    None => DataType::Generic(idx),
                })
                .collect()
        })
        .unwrap_or_default();

    Some((checked_args, return_type, generics))
}

pub fn unify(src_ty: &DataType, dest_ty: &DataType) -> Option<Subsitution> {
    match (src_ty, dest_ty) {
        (DataType::Generic(_), _) => unreachable!("source type must not contain generic type"),
        (ty, DataType::Generic(idx)) => Some(Subsitution::equation(*idx, ty.clone())),
        (DataType::Null, DataType::Nullable(_)) => Some(Subsitution::empty()),
        (DataType::EmptyArray, DataType::Array(_)) => Some(Subsitution::empty()),
        (DataType::Nullable(src_ty), DataType::Nullable(dest_ty)) => unify(src_ty, dest_ty),
        (src_ty, DataType::Nullable(dest_ty)) => unify(src_ty, dest_ty),
        (DataType::Array(src_ty), DataType::Array(dest_ty)) => unify(src_ty, dest_ty),
        (DataType::Tuple(src_tys), DataType::Tuple(dest_tys))
            if src_tys.len() == dest_tys.len() =>
        {
            let substs = src_tys
                .iter()
                .zip(dest_tys)
                .map(|(src_ty, dest_ty)| unify(src_ty, dest_ty))
                .collect::<Option<Vec<_>>>()?;
            let subst = substs
                .into_iter()
                .try_reduce(|subst1, subst2| subst1.merge(subst2))?
                .unwrap_or_else(Subsitution::empty);
            Some(subst)
        }
        (src_ty, dest_ty) if can_cast_to(src_ty, dest_ty) => Some(Subsitution::empty()),
        _ => None,
    }
}

// TODO: should support fallable casts
pub fn can_cast_to(src_ty: &DataType, dest_ty: &DataType) -> bool {
    match (src_ty, dest_ty) {
        (src_ty, dest_ty) if src_ty == dest_ty => true,
        (DataType::Null, DataType::Nullable(_)) => true,
        (DataType::EmptyArray, DataType::Array(_)) => true,
        (DataType::Nullable(src_ty), DataType::Nullable(dest_ty)) => can_cast_to(src_ty, dest_ty),
        (src_ty, DataType::Nullable(dest_ty)) => can_cast_to(src_ty, dest_ty),
        (DataType::Array(src_ty), DataType::Array(dest_ty)) => can_cast_to(src_ty, dest_ty),
        (DataType::UInt8, DataType::UInt16)
        | (DataType::Int8, DataType::Int16)
        | (DataType::UInt8, DataType::Int16) => true,
        _ => false,
    }
}

pub fn common_super_type(ty1: DataType, ty2: DataType) -> Option<DataType> {
    match (ty1, ty2) {
        (ty1, ty2) if ty1 == ty2 => Some(ty1),
        (DataType::Null, ty @ DataType::Nullable(_))
        | (ty @ DataType::Nullable(_), DataType::Null) => Some(ty),
        (DataType::Null, ty) | (ty, DataType::Null) => Some(DataType::Nullable(Box::new(ty))),
        (DataType::Nullable(box ty1), DataType::Nullable(box ty2))
        | (DataType::Nullable(box ty1), ty2)
        | (ty1, DataType::Nullable(box ty2)) => {
            Some(DataType::Nullable(Box::new(common_super_type(ty1, ty2)?)))
        }
        (DataType::EmptyArray, ty @ DataType::Array(_))
        | (ty @ DataType::Array(_), DataType::EmptyArray) => Some(ty),
        (DataType::Array(box ty1), DataType::Array(box ty2)) => {
            Some(DataType::Array(Box::new(common_super_type(ty1, ty2)?)))
        }
        (DataType::UInt8, DataType::UInt16) | (DataType::UInt16, DataType::UInt8) => {
            Some(DataType::UInt16)
        }
        (DataType::Int8, DataType::Int16) | (DataType::Int16, DataType::Int8) => {
            Some(DataType::Int16)
        }
        (DataType::Int16, DataType::UInt8) | (DataType::UInt8, DataType::Int16) => {
            Some(DataType::Int16)
        }
        _ => None,
    }
}
