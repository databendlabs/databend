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

use common_datavalues::DataTypeImpl;
use common_exception::Result;

use crate::sql::binder::scalar_visitor::Recursion;
use crate::sql::binder::scalar_visitor::ScalarVisitor;
use crate::sql::plans::AndExpr;
use crate::sql::plans::CastExpr;
use crate::sql::plans::ComparisonExpr;
use crate::sql::plans::ComparisonOp;
use crate::sql::plans::Scalar;
use crate::sql::plans::ScalarExpr;

// Visitor that find Expressions that match a particular predicate
struct Finder<'a, F>
where F: Fn(&Scalar) -> bool
{
    find_fn: &'a F,
    scalars: Vec<Scalar>,
}

impl<'a, F> Finder<'a, F>
where F: Fn(&Scalar) -> bool
{
    /// Create a new finder with the `test_fn`
    #[allow(dead_code)]
    fn new(find_fn: &'a F) -> Self {
        Self {
            find_fn,
            scalars: Vec::new(),
        }
    }
}

impl<'a, F> ScalarVisitor for Finder<'a, F>
where F: Fn(&Scalar) -> bool
{
    fn pre_visit(mut self, scalar: &Scalar) -> Result<Recursion<Self>> {
        if (self.find_fn)(scalar) {
            if !(self.scalars.contains(scalar)) {
                self.scalars.push((*scalar).clone())
            }
            // stop recursing down this expr once we find a match
            return Ok(Recursion::Stop(self));
        }

        Ok(Recursion::Continue(self))
    }
}

pub fn split_conjunctions(scalar: &Scalar) -> Vec<Scalar> {
    match scalar {
        Scalar::AndExpr(AndExpr { left, right }) => {
            vec![split_conjunctions(left), split_conjunctions(right)].concat()
        }
        _ => {
            vec![scalar.clone()]
        }
    }
}

pub fn split_equivalent_predicate(scalar: &Scalar) -> Option<(Scalar, Scalar)> {
    match scalar {
        Scalar::ComparisonExpr(ComparisonExpr { op, left, right })
            if *op == ComparisonOp::Equal =>
        {
            Some((*left.clone(), *right.clone()))
        }
        _ => None,
    }
}

pub fn wrap_cast_if_needed(scalar: Scalar, target_type: &DataTypeImpl) -> Scalar {
    if scalar.data_type() != *target_type {
        let cast = CastExpr {
            from_type: scalar.data_type(),
            argument: Box::new(scalar),
            target_type: target_type.clone(),
        };
        cast.into()
    } else {
        scalar
    }
}
