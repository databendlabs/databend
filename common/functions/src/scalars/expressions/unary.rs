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

/// An common struct to caculate Unary expression scalar op.
pub struct ScalarUnaryExpression<L: Scalar, O: Scalar, F> {
    func: F,
    _phantom: PhantomData<(L, O)>,
}

impl<'a, L: Scalar, O: Scalar, F> ScalarUnaryExpression<L, O, F>
where F: Fn(L::RefType<'a>) -> O
{
    /// Create a Unary expression from generic columns  and a lambda function.
    pub fn new(func: F) -> Self {
        Self {
            func,
            _phantom: PhantomData,
        }
    }

    /// Evaluate the expression with the given array.
    pub fn eval(&self, l: &'a ColumnRef) -> Result<<O as Scalar>::ColumnType> {
        let left = ColumnViewerIter::<L>::try_create(l)?;
        let it = left.map(|a| (self.func)(a));
        Ok(<O as Scalar>::ColumnType::from_owned_iterator(it))
    }
}

mod test {



    #[test]
    fn test_contains() {
        //create two string columns
        let l = Series::from_data(vec!["11", "22", "333"]);
        let expected = Series::from_data(vec![2i32, 2, 3]);
        let unary_expression =
            ScalarUnaryExpression::<Vec<u8>, i32, _>::new(|a| -> i32 { a.len() as i32 });
        let result = unary_expression.eval(&l).unwrap();
        let result = Arc::new(result) as ColumnRef;
        assert!(result == expected);
    }
}
