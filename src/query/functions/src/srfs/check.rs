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

use common_exception::ErrorCode;
use common_exception::Result;
use common_expression::type_check::check_cast;
use common_expression::type_check::try_unify_signature;
use common_expression::types::DataType;
use common_expression::ColumnIndex;
use common_expression::Expr;

use crate::scalars::BUILTIN_FUNCTIONS;

#[allow(clippy::type_complexity)]
pub fn try_check_srf<Index: ColumnIndex>(
    expected_arg_types: &[DataType],
    expected_return_types: &[DataType],
    args: &[Expr<Index>],
) -> Result<(Vec<Expr<Index>>, Vec<DataType>, Vec<DataType>)> {
    let subst = try_unify_signature(
        args.iter().map(Expr::data_type),
        expected_arg_types.iter(),
        &[],
    )?;

    let checked_args = args
        .iter()
        .zip(expected_arg_types.iter())
        .map(|(arg, sig_type)| {
            let sig_type = subst.apply(sig_type)?;
            let is_try = BUILTIN_FUNCTIONS.is_auto_try_cast_rule(arg.data_type(), &sig_type);
            check_cast(
                arg.span(),
                is_try,
                arg.clone(),
                &sig_type,
                &BUILTIN_FUNCTIONS,
            )
        })
        .collect::<Result<Vec<_>>>()?;

    let generics = subst
        .0
        .keys()
        .cloned()
        .max()
        .map(|max_generic_idx| {
            (0..max_generic_idx + 1)
                .map(|idx| match subst.0.get(&idx) {
                    Some(ty) => Ok(ty.clone()),
                    None => Err(ErrorCode::from_string_no_backtrace(format!(
                        "unable to resolve generic T{idx}"
                    ))),
                })
                .collect::<Result<Vec<_>>>()
        })
        .unwrap_or_else(|| Ok(vec![]))?;

    // TODO: we only support one return type for now, so we can reuse
    // the current implementation of `Substitution` to get the return type.
    let return_type = subst.apply(&expected_return_types[0])?;

    Ok((checked_args, vec![return_type], generics))
}
