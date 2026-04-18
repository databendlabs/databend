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

use crate::ast::ColumnFilter;
use crate::ast::OrderByExpr;
use crate::ast::Query;
use crate::ast::SelectStmt;
use crate::ast::SelectTarget;
use crate::ast::SetExpr;
use crate::ast::WithOptions;
use crate::visit::VisitControl;
use crate::visit::Visitor;
use crate::visit::VisitorMut;
use crate::visit::Walk;
use crate::visit::WalkMut;

impl Walk for OrderByExpr {
    fn walk<V: Visitor + ?Sized>(
        &self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        try_walk!(self.expr.walk(visitor));
        Ok(VisitControl::Continue)
    }
}

impl WalkMut for OrderByExpr {
    fn walk_mut<V: VisitorMut + ?Sized>(
        &mut self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        try_walk!(self.expr.walk_mut(visitor));
        Ok(VisitControl::Continue)
    }
}

impl Walk for Query {
    fn walk<V: Visitor + ?Sized>(
        &self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        try_control!(visitor.visit_query(self));

        if let Some(with) = &self.with {
            for cte in &with.ctes {
                try_walk!(cte.walk(visitor));
            }
        }
        try_walk!(self.body.walk(visitor));
        for item in &self.order_by {
            try_walk!(item.walk(visitor));
        }
        for expr in &self.limit {
            try_walk!(expr.walk(visitor));
        }
        if let Some(offset) = &self.offset {
            try_walk!(offset.walk(visitor));
        }

        Ok(VisitControl::Continue)
    }
}

impl WalkMut for Query {
    fn walk_mut<V: VisitorMut + ?Sized>(
        &mut self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        try_control!(visitor.visit_query(self));

        if let Some(with) = &mut self.with {
            for cte in &mut with.ctes {
                try_walk!(cte.walk_mut(visitor));
            }
        }
        try_walk!(self.body.walk_mut(visitor));
        for item in &mut self.order_by {
            try_walk!(item.walk_mut(visitor));
        }
        for expr in &mut self.limit {
            try_walk!(expr.walk_mut(visitor));
        }
        if let Some(offset) = &mut self.offset {
            try_walk!(offset.walk_mut(visitor));
        }

        Ok(VisitControl::Continue)
    }
}

// WithOptions is an opaque string map. Traversal is intentionally a no-op and
// must stay explicit in visit/ rather than hidden inside derive(Walk)/derive(WalkMut).
impl_noop_walk!(WithOptions);

impl Walk for SetExpr {
    fn walk<V: Visitor + ?Sized>(
        &self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        try_control!(visitor.visit_set_expr(self));

        match self {
            SetExpr::Select(select) => try_walk!(select.walk(visitor)),
            SetExpr::Query(query) => try_walk!(query.walk(visitor)),
            SetExpr::SetOperation(set_op) => {
                try_walk!(set_op.left.walk(visitor));
                try_walk!(set_op.right.walk(visitor));
            }
            SetExpr::Values { values, .. } => {
                for row in values {
                    for expr in row {
                        try_walk!(expr.walk(visitor));
                    }
                }
            }
        }

        Ok(VisitControl::Continue)
    }
}

impl WalkMut for SetExpr {
    fn walk_mut<V: VisitorMut + ?Sized>(
        &mut self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        try_control!(visitor.visit_set_expr(self));

        match self {
            SetExpr::Select(select) => try_walk!(select.walk_mut(visitor)),
            SetExpr::Query(query) => try_walk!(query.walk_mut(visitor)),
            SetExpr::SetOperation(set_op) => {
                try_walk!(set_op.left.walk_mut(visitor));
                try_walk!(set_op.right.walk_mut(visitor));
            }
            SetExpr::Values { values, .. } => {
                for row in values {
                    for expr in row {
                        try_walk!(expr.walk_mut(visitor));
                    }
                }
            }
        }

        Ok(VisitControl::Continue)
    }
}

impl Walk for SelectStmt {
    fn walk<V: Visitor + ?Sized>(
        &self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        try_control!(visitor.visit_select_stmt(self));

        if let Some(hints) = &self.hints {
            try_walk!(hints.walk(visitor));
        }

        for target in &self.select_list {
            match target {
                SelectTarget::AliasedExpr { expr, alias } => {
                    try_walk!(expr.walk(visitor));
                    if let Some(alias) = alias {
                        try_walk!(alias.walk(visitor));
                    }
                }
                SelectTarget::StarColumns {
                    qualified,
                    column_filter,
                } => {
                    for item in qualified {
                        if let crate::ast::Indirection::Identifier(ident) = item {
                            try_walk!(ident.walk(visitor));
                        }
                    }
                    if let Some(column_filter) = column_filter {
                        match column_filter {
                            ColumnFilter::Excludes(idents) => {
                                for ident in idents {
                                    try_walk!(ident.walk(visitor));
                                }
                            }
                            ColumnFilter::Lambda(lambda) => try_walk!(lambda.walk(visitor)),
                        }
                    }
                }
            }
        }

        for from in &self.from {
            try_walk!(from.walk(visitor));
        }
        if let Some(expr) = &self.selection {
            try_walk!(expr.walk(visitor));
        }
        if let Some(group_by) = &self.group_by {
            try_walk!(group_by.walk(visitor));
        }
        if let Some(expr) = &self.having {
            try_walk!(expr.walk(visitor));
        }
        if let Some(window_list) = &self.window_list {
            for win in window_list {
                try_walk!(win.walk(visitor));
            }
        }
        if let Some(expr) = &self.qualify {
            try_walk!(expr.walk(visitor));
        }

        Ok(VisitControl::Continue)
    }
}

impl WalkMut for SelectStmt {
    fn walk_mut<V: VisitorMut + ?Sized>(
        &mut self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        try_control!(visitor.visit_select_stmt(self));

        if let Some(hints) = &mut self.hints {
            try_walk!(hints.walk_mut(visitor));
        }

        for target in &mut self.select_list {
            match target {
                SelectTarget::AliasedExpr { expr, alias } => {
                    try_walk!(expr.walk_mut(visitor));
                    if let Some(alias) = alias {
                        try_walk!(alias.walk_mut(visitor));
                    }
                }
                SelectTarget::StarColumns {
                    qualified,
                    column_filter,
                } => {
                    for item in qualified {
                        if let crate::ast::Indirection::Identifier(ident) = item {
                            try_walk!(ident.walk_mut(visitor));
                        }
                    }
                    if let Some(column_filter) = column_filter {
                        match column_filter {
                            ColumnFilter::Excludes(idents) => {
                                for ident in idents {
                                    try_walk!(ident.walk_mut(visitor));
                                }
                            }
                            ColumnFilter::Lambda(lambda) => try_walk!(lambda.walk_mut(visitor)),
                        }
                    }
                }
            }
        }

        for from in &mut self.from {
            try_walk!(from.walk_mut(visitor));
        }
        if let Some(expr) = &mut self.selection {
            try_walk!(expr.walk_mut(visitor));
        }
        if let Some(group_by) = &mut self.group_by {
            try_walk!(group_by.walk_mut(visitor));
        }
        if let Some(expr) = &mut self.having {
            try_walk!(expr.walk_mut(visitor));
        }
        if let Some(window_list) = &mut self.window_list {
            for win in window_list {
                try_walk!(win.walk_mut(visitor));
            }
        }
        if let Some(expr) = &mut self.qualify {
            try_walk!(expr.walk_mut(visitor));
        }

        Ok(VisitControl::Continue)
    }
}
