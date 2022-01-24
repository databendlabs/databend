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

use std::marker::PhantomData;

use common_datavalues2::prelude::*;
use common_exception::Result;

/// An common struct to caculate binary expression scalar op.
pub struct ScalarBinaryExpression<L: Scalar, R: Scalar, O: Scalar, F> {
    func: F,
    _phantom: PhantomData<(L, R, O)>,
}

impl<'a, L: Scalar, R: Scalar, O: Scalar, F> ScalarBinaryExpression<L, R, O, F>
where F: Fn(L::RefType<'a>, R::RefType<'a>) -> O
{
    /// Create a binary expression from generic columns  and a lambda function.
    pub fn new(func: F) -> Self {
        Self {
            func,
            _phantom: PhantomData,
        }
    }

    /// Evaluate the expression with the given array.
    pub fn eval(&self, l: &'a ColumnRef, r: &'a ColumnRef) -> Result<<O as Scalar>::ColumnType> {
        let left = ColumnViewerIter::<L>::try_create(l)?;
        let right = ColumnViewerIter::<R>::try_create(r)?;

        let it = left.zip(right).map(|(a, b)| (self.func)(a, b));
        Ok(<O as Scalar>::ColumnType::from_owned_iterator(it))
    }
}

// not used for now
pub struct ScalarBinaryExpressionVc<L: ScalarColumn, R: ScalarColumn, O: ScalarColumn, F> {
    func: F,
    _phantom: PhantomData<(L, R, O)>,
}

impl<'a, L: ScalarColumn, R: ScalarColumn, O: ScalarColumn, F> ScalarBinaryExpressionVc<L, R, O, F>
where F: Fn(L::RefItem<'a>, R::RefItem<'a>) -> O::OwnedItem
{
    /// Create a binary expression from generic columns  and a lambda function.
    pub fn new(func: F) -> Self {
        Self {
            func,
            _phantom: PhantomData,
        }
    }

    /// Evaluate the expression with the given array.
    pub fn eval(&self, l: &'a ColumnRef, r: &'a ColumnRef) -> Result<O> {
        let left = ColumnViewerIter::<L::OwnedItem>::try_create(l)?;
        let right = ColumnViewerIter::<R::OwnedItem>::try_create(r)?;

        let it = left.zip(right).map(|(a, b)| (self.func)(a, b));
        Ok(O::from_owned_iterator(it))
    }
}
