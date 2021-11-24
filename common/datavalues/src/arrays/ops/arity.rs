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

use crate::prelude::combine_validities;
use crate::prelude::to_primitive;
use crate::prelude::AlignedVec;
use crate::prelude::DFPrimitiveArray;
use crate::DFPrimitiveType;

#[inline]
pub fn binary<T, D, R, F>(
    lhs: &DFPrimitiveArray<T>,
    rhs: &DFPrimitiveArray<D>,
    op: F,
) -> DFPrimitiveArray<R>
where
    T: DFPrimitiveType,
    D: DFPrimitiveType,
    R: DFPrimitiveType,
    F: Fn(T, D) -> R,
{
    let validity = combine_validities(lhs.inner().validity(), rhs.inner().validity());
    let values = lhs
        .into_no_null_iter()
        .zip(rhs.into_no_null_iter())
        .map(|(l, r)| op(*l, *r));

    let av = AlignedVec::<_>::from_trusted_len_iter(values);
    to_primitive::<R>(av, validity)
}
