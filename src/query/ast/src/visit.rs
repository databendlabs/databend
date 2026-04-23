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

use crate::ast::Expr;
use crate::ast::FunctionCall;
use crate::ast::Identifier;
use crate::ast::MapAccessor;
use crate::ast::Query;
use crate::ast::SelectStmt;
use crate::ast::SetExpr;
use crate::ast::Statement;
use crate::ast::TableReference;

macro_rules! try_control {
    ($expr:expr) => {
        match $expr? {
            VisitControl::Continue => {}
            VisitControl::SkipChildren => return Ok(VisitControl::Continue),
            VisitControl::Break(value) => return Ok(VisitControl::Break(value)),
        }
    };
}

macro_rules! try_walk {
    ($expr:expr) => {
        match $expr? {
            VisitControl::Continue => {}
            VisitControl::SkipChildren => {}
            VisitControl::Break(value) => return Ok(VisitControl::Break(value)),
        }
    };
}

macro_rules! impl_noop_walk {
    ($ty:ty) => {
        impl crate::visit::Walk for $ty {
            fn walk<V: crate::visit::Visitor + ?Sized>(
                &self,
                _visitor: &mut V,
            ) -> Result<crate::visit::VisitControl<V::Break>, V::Error> {
                Ok(crate::visit::VisitControl::Continue)
            }
        }

        impl crate::visit::WalkMut for $ty {
            fn walk_mut<V: crate::visit::VisitorMut + ?Sized>(
                &mut self,
                _visitor: &mut V,
            ) -> Result<crate::visit::VisitControl<V::Break>, V::Error> {
                Ok(crate::visit::VisitControl::Continue)
            }
        }
    };
}

mod expr;
mod query;
mod statement;
mod statement_connection;
mod statement_dml;
mod statement_routine;
mod statement_sequence;
mod statement_show;
mod statement_table;
mod statement_tag;
mod statement_user;
mod statement_warehouse;
mod statement_worker;
mod statement_workload;
mod table_reference;

/// Controls whether the walker continues into the current node's children.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VisitControl<B = ()> {
    Continue,
    SkipChildren,
    Break(B),
}

pub type VisitResult = Result<VisitControl, !>;

/// Pre-order visitor hooks for immutable AST traversal.
///
/// Returning `Continue` lets the walker descend into children automatically.
/// Returning `SkipChildren` stops descent for the current node.
/// Returning `Break` aborts the whole traversal.
///
/// Visitor methods should not call `walk` on the same node again. The walker
/// already handles recursion after `Continue`.
pub trait Visitor {
    type Error = !;
    type Break = ();

    fn visit_statement(
        &mut self,
        _stmt: &Statement,
    ) -> Result<VisitControl<Self::Break>, Self::Error> {
        Ok(VisitControl::Continue)
    }

    fn visit_query(&mut self, _query: &Query) -> Result<VisitControl<Self::Break>, Self::Error> {
        Ok(VisitControl::Continue)
    }

    fn visit_set_expr(
        &mut self,
        _set_expr: &SetExpr,
    ) -> Result<VisitControl<Self::Break>, Self::Error> {
        Ok(VisitControl::Continue)
    }

    fn visit_select_stmt(
        &mut self,
        _select: &SelectStmt,
    ) -> Result<VisitControl<Self::Break>, Self::Error> {
        Ok(VisitControl::Continue)
    }

    fn visit_table_reference(
        &mut self,
        _table: &TableReference,
    ) -> Result<VisitControl<Self::Break>, Self::Error> {
        Ok(VisitControl::Continue)
    }

    fn visit_expr(&mut self, _expr: &Expr) -> Result<VisitControl<Self::Break>, Self::Error> {
        Ok(VisitControl::Continue)
    }

    fn visit_function_call(
        &mut self,
        _call: &FunctionCall,
    ) -> Result<VisitControl<Self::Break>, Self::Error> {
        Ok(VisitControl::Continue)
    }

    fn visit_identifier(
        &mut self,
        _ident: &Identifier,
    ) -> Result<VisitControl<Self::Break>, Self::Error> {
        Ok(VisitControl::Continue)
    }

    fn visit_map_accessor(
        &mut self,
        _accessor: &MapAccessor,
    ) -> Result<VisitControl<Self::Break>, Self::Error> {
        Ok(VisitControl::Continue)
    }
}

/// Pre-order visitor hooks for mutable AST traversal.
///
/// Returning `Continue` lets the walker descend into children automatically.
/// Returning `SkipChildren` stops descent for the current node.
/// Returning `Break` aborts the whole traversal.
///
/// Visitor methods should not call `walk_mut` on the same node again. The
/// walker already handles recursion after `Continue`.
pub trait VisitorMut {
    type Error = !;
    type Break = ();

    fn visit_statement(
        &mut self,
        _stmt: &mut Statement,
    ) -> Result<VisitControl<Self::Break>, Self::Error> {
        Ok(VisitControl::Continue)
    }

    fn visit_query(
        &mut self,
        _query: &mut Query,
    ) -> Result<VisitControl<Self::Break>, Self::Error> {
        Ok(VisitControl::Continue)
    }

    fn visit_set_expr(
        &mut self,
        _set_expr: &mut SetExpr,
    ) -> Result<VisitControl<Self::Break>, Self::Error> {
        Ok(VisitControl::Continue)
    }

    fn visit_select_stmt(
        &mut self,
        _select: &mut SelectStmt,
    ) -> Result<VisitControl<Self::Break>, Self::Error> {
        Ok(VisitControl::Continue)
    }

    fn visit_table_reference(
        &mut self,
        _table: &mut TableReference,
    ) -> Result<VisitControl<Self::Break>, Self::Error> {
        Ok(VisitControl::Continue)
    }

    fn visit_expr(&mut self, _expr: &mut Expr) -> Result<VisitControl<Self::Break>, Self::Error> {
        Ok(VisitControl::Continue)
    }

    fn visit_function_call(
        &mut self,
        _call: &mut FunctionCall,
    ) -> Result<VisitControl<Self::Break>, Self::Error> {
        Ok(VisitControl::Continue)
    }

    fn visit_identifier(
        &mut self,
        _ident: &mut Identifier,
    ) -> Result<VisitControl<Self::Break>, Self::Error> {
        Ok(VisitControl::Continue)
    }

    fn visit_map_accessor(
        &mut self,
        _accessor: &mut MapAccessor,
    ) -> Result<VisitControl<Self::Break>, Self::Error> {
        Ok(VisitControl::Continue)
    }
}

pub trait Walk {
    fn walk<V: Visitor + ?Sized>(
        &self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error>;
}

pub trait WalkMut {
    fn walk_mut<V: VisitorMut + ?Sized>(
        &mut self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error>;
}

impl<T: Walk + ?Sized> Walk for &T {
    fn walk<V: Visitor + ?Sized>(
        &self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        T::walk(*self, visitor)
    }
}

impl_noop_walk!(String);
impl_noop_walk!(crate::span::Span);

impl<T: WalkMut + ?Sized> WalkMut for &mut T {
    fn walk_mut<V: VisitorMut + ?Sized>(
        &mut self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        T::walk_mut(*self, visitor)
    }
}

impl<T: Walk + ?Sized> Walk for Box<T> {
    fn walk<V: Visitor + ?Sized>(
        &self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        self.as_ref().walk(visitor)
    }
}

impl<T: WalkMut + ?Sized> WalkMut for Box<T> {
    fn walk_mut<V: VisitorMut + ?Sized>(
        &mut self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        self.as_mut().walk_mut(visitor)
    }
}

impl<T: Walk> Walk for Option<T> {
    fn walk<V: Visitor + ?Sized>(
        &self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        if let Some(value) = self {
            try_walk!(value.walk(visitor));
        }
        Ok(VisitControl::Continue)
    }
}

impl<T: WalkMut> WalkMut for Option<T> {
    fn walk_mut<V: VisitorMut + ?Sized>(
        &mut self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        if let Some(value) = self {
            try_walk!(value.walk_mut(visitor));
        }
        Ok(VisitControl::Continue)
    }
}

impl<T: Walk> Walk for Vec<T> {
    fn walk<V: Visitor + ?Sized>(
        &self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        self.as_slice().walk(visitor)
    }
}

impl<T: WalkMut> WalkMut for Vec<T> {
    fn walk_mut<V: VisitorMut + ?Sized>(
        &mut self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        self.as_mut_slice().walk_mut(visitor)
    }
}

impl<T: Walk> Walk for [T] {
    fn walk<V: Visitor + ?Sized>(
        &self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        for value in self {
            try_walk!(value.walk(visitor));
        }
        Ok(VisitControl::Continue)
    }
}

impl<T: WalkMut> WalkMut for [T] {
    fn walk_mut<V: VisitorMut + ?Sized>(
        &mut self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        for value in self {
            try_walk!(value.walk_mut(visitor));
        }
        Ok(VisitControl::Continue)
    }
}

impl<A: Walk, B: Walk> Walk for (A, B) {
    fn walk<V: Visitor + ?Sized>(
        &self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        try_walk!(self.0.walk(visitor));
        try_walk!(self.1.walk(visitor));
        Ok(VisitControl::Continue)
    }
}

impl<A: WalkMut, B: WalkMut> WalkMut for (A, B) {
    fn walk_mut<V: VisitorMut + ?Sized>(
        &mut self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        try_walk!(self.0.walk_mut(visitor));
        try_walk!(self.1.walk_mut(visitor));
        Ok(VisitControl::Continue)
    }
}

impl<A: Walk, B: Walk, C: Walk> Walk for (A, B, C) {
    fn walk<V: Visitor + ?Sized>(
        &self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        try_walk!(self.0.walk(visitor));
        try_walk!(self.1.walk(visitor));
        try_walk!(self.2.walk(visitor));
        Ok(VisitControl::Continue)
    }
}

impl<A: WalkMut, B: WalkMut, C: WalkMut> WalkMut for (A, B, C) {
    fn walk_mut<V: VisitorMut + ?Sized>(
        &mut self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        try_walk!(self.0.walk_mut(visitor));
        try_walk!(self.1.walk_mut(visitor));
        try_walk!(self.2.walk_mut(visitor));
        Ok(VisitControl::Continue)
    }
}
