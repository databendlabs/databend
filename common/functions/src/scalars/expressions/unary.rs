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

use std::simd::LaneCount;
use std::simd::Simd;
use std::simd::SimdElement;
use std::simd::SupportedLaneCount;

use common_datavalues::prelude::*;
use common_exception::Result;

use super::from_incomplete_chunk;
use super::EvalContext;

pub fn scalar_unary_op<L: Scalar, O: Scalar, F>(
    l: &ColumnRef,
    f: F,
    ctx: &mut EvalContext,
) -> Result<<O as Scalar>::ColumnType>
where
    F: Fn(L::RefType<'_>, &mut EvalContext) -> O,
{
    let left = Series::check_get_scalar::<L>(l)?;
    let it = left.scalar_iter().map(|a| f(a, ctx));
    let result = <O as Scalar>::ColumnType::from_owned_iterator(it);

    if let Some(error) = ctx.error.take() {
        return Err(error);
    }
    Ok(result)
}

pub fn unary_simd_op<L, O, F, const N: usize>(l: &ColumnRef, op: F) -> Result<PrimitiveColumn<O>>
where
    L: PrimitiveType + SimdElement,
    O: PrimitiveType + SimdElement,
    F: Fn(Simd<L, N>) -> Simd<O, N>,
    LaneCount<N>: SupportedLaneCount,
{
    let left: &PrimitiveColumn<L> = Series::check_get(l)?;
    let lhs_chunks = left.values().chunks_exact(N);
    let lhs_remainder = lhs_chunks.remainder();

    let mut values = Vec::<O>::with_capacity(l.len());
    lhs_chunks.for_each(|lhs| {
        let res = op(Simd::from_slice(lhs));
        values.extend_from_slice(res.as_array())
    });

    if !lhs_remainder.is_empty() {
        let lhs = from_incomplete_chunk(lhs_remainder, L::default());
        let res = op(lhs);
        values.extend_from_slice(&res.as_array()[0..lhs_remainder.len()])
    };

    Ok(PrimitiveColumn::<O>::new_from_vec(values))
}
