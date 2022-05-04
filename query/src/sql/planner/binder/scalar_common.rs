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

use common_exception::Result;

use crate::sql::binder::scalar_visitor::Recursion;
use crate::sql::binder::scalar_visitor::ScalarVisitor;
use crate::sql::plans::Scalar;
use crate::sql::BindContext;

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

fn find_scalars_in_scalar<F>(scalar: &Scalar, find_fn: &F) -> Vec<Scalar>
where F: Fn(&Scalar) -> bool {
    let Finder { scalars, .. } = scalar
        .accept(Finder::new(find_fn))
        .expect("no way to return error during recursion");
    scalars
}

fn find_scalars_in_scalars<F>(scalars: &[Scalar], find_fn: &F) -> Vec<Scalar>
where F: Fn(&Scalar) -> bool {
    scalars
        .iter()
        .flat_map(|scalar| find_scalars_in_scalar(scalar, find_fn))
        .fold(vec![], |mut acc, scalar| {
            if !acc.contains(&scalar) {
                acc.push(scalar)
            }
            acc
        })
}

pub fn find_aggregate_scalars(scalars: &[Scalar]) -> Vec<Scalar> {
    find_scalars_in_scalars(scalars, &|nest_scalar| {
        matches!(nest_scalar, Scalar::AggregateFunction { .. })
    })
}

pub fn find_aggregate_scalars_from_bind_context(bind_context: &BindContext) -> Result<Vec<Scalar>> {
    let scalars = bind_context
        .all_column_bindings()
        .iter()
        .flat_map(|col_binding| col_binding.scalar.clone().map(|s| *s))
        .collect::<Vec<Scalar>>();
    Ok(find_aggregate_scalars(&scalars))
}
