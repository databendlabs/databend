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
use crate::ast::WindowDesc;
use crate::visit::VisitControl;
use crate::visit::Visitor;
use crate::visit::VisitorMut;
use crate::visit::Walk;
use crate::visit::WalkMut;

impl Walk for Expr {
    fn walk<V: Visitor + ?Sized>(
        &self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        match visitor.visit_expr(self)? {
            VisitControl::Continue => match self {
                Expr::ColumnRef { column, .. } => {
                    if let Some(database) = &column.database {
                        try_walk!(database.walk(visitor));
                    }
                    if let Some(table) = &column.table {
                        try_walk!(table.walk(visitor));
                    }
                    if let crate::ast::ColumnID::Name(ident) = &column.column {
                        try_walk!(ident.walk(visitor));
                    }
                }
                Expr::IsNull { expr, .. }
                | Expr::UnaryOp { expr, .. }
                | Expr::Extract { expr, .. }
                | Expr::DatePart { expr, .. }
                | Expr::Interval { expr, .. }
                | Expr::DateTrunc { date: expr, .. }
                | Expr::LastDay { date: expr, .. }
                | Expr::PreviousDay { date: expr, .. }
                | Expr::NextDay { date: expr, .. } => {
                    try_walk!(expr.walk(visitor));
                }
                Expr::Cast {
                    expr, target_type, ..
                }
                | Expr::TryCast {
                    expr, target_type, ..
                } => {
                    try_walk!(expr.walk(visitor));
                    try_walk!(target_type.walk(visitor));
                }
                Expr::IsDistinctFrom { left, right, .. }
                | Expr::BinaryOp { left, right, .. }
                | Expr::JsonOp { left, right, .. }
                | Expr::LikeWithEscape { left, right, .. }
                | Expr::LikeAnyWithEscape { left, right, .. } => {
                    try_walk!(left.walk(visitor));
                    try_walk!(right.walk(visitor));
                }
                Expr::InList { expr, list, .. } => {
                    try_walk!(expr.walk(visitor));
                    for item in list {
                        try_walk!(item.walk(visitor));
                    }
                }
                Expr::InSubquery { expr, subquery, .. }
                | Expr::LikeSubquery { expr, subquery, .. } => {
                    try_walk!(expr.walk(visitor));
                    try_walk!(subquery.walk(visitor));
                }
                Expr::Between {
                    expr, low, high, ..
                } => {
                    try_walk!(expr.walk(visitor));
                    try_walk!(low.walk(visitor));
                    try_walk!(high.walk(visitor));
                }
                Expr::Position {
                    substr_expr,
                    str_expr,
                    ..
                } => {
                    try_walk!(substr_expr.walk(visitor));
                    try_walk!(str_expr.walk(visitor));
                }
                Expr::Substring {
                    expr,
                    substring_from,
                    substring_for,
                    ..
                } => {
                    try_walk!(expr.walk(visitor));
                    try_walk!(substring_from.walk(visitor));
                    if let Some(substring_for) = substring_for {
                        try_walk!(substring_for.walk(visitor));
                    }
                }
                Expr::Trim {
                    expr, trim_where, ..
                } => {
                    try_walk!(expr.walk(visitor));
                    if let Some((_, trim_expr)) = trim_where {
                        try_walk!(trim_expr.walk(visitor));
                    }
                }
                Expr::CountAll {
                    window, qualified, ..
                } => {
                    for item in qualified {
                        if let crate::ast::Indirection::Identifier(ident) = item {
                            try_walk!(ident.walk(visitor));
                        }
                    }
                    if let Some(window) = window {
                        try_walk!(window.walk(visitor));
                    }
                }
                Expr::Tuple { exprs, .. } | Expr::Array { exprs, .. } => {
                    for expr in exprs {
                        try_walk!(expr.walk(visitor));
                    }
                }
                Expr::FunctionCall { func, .. } => try_walk!(func.walk(visitor)),
                Expr::Case {
                    operand,
                    conditions,
                    results,
                    else_result,
                    ..
                } => {
                    if let Some(operand) = operand {
                        try_walk!(operand.walk(visitor));
                    }
                    for expr in conditions {
                        try_walk!(expr.walk(visitor));
                    }
                    for expr in results {
                        try_walk!(expr.walk(visitor));
                    }
                    if let Some(expr) = else_result {
                        try_walk!(expr.walk(visitor));
                    }
                }
                Expr::Exists { subquery, .. } | Expr::Subquery { subquery, .. } => {
                    try_walk!(subquery.walk(visitor));
                }
                Expr::MapAccess { expr, accessor, .. } => {
                    try_walk!(expr.walk(visitor));
                    try_walk!(accessor.walk(visitor));
                }
                Expr::Map { kvs, .. } => {
                    for (_, value) in kvs {
                        try_walk!(value.walk(visitor));
                    }
                }
                Expr::DateAdd { interval, date, .. } | Expr::DateSub { interval, date, .. } => {
                    try_walk!(interval.walk(visitor));
                    try_walk!(date.walk(visitor));
                }
                Expr::DateDiff {
                    date_start,
                    date_end,
                    ..
                }
                | Expr::DateBetween {
                    date_start,
                    date_end,
                    ..
                } => {
                    try_walk!(date_start.walk(visitor));
                    try_walk!(date_end.walk(visitor));
                }
                Expr::TimeSlice { date, .. } => try_walk!(date.walk(visitor)),
                Expr::Literal { .. }
                | Expr::Hole { .. }
                | Expr::Placeholder { .. }
                | Expr::StageLocation { .. } => {}
            },
            VisitControl::SkipChildren => {}
            VisitControl::Break(value) => return Ok(VisitControl::Break(value)),
        }

        visitor.leave_expr(self)?;
        Ok(VisitControl::Continue)
    }
}

impl WalkMut for Expr {
    fn walk_mut<V: VisitorMut + ?Sized>(
        &mut self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        match visitor.visit_expr(self)? {
            VisitControl::Continue => match self {
                Expr::ColumnRef { column, .. } => {
                    if let Some(database) = &mut column.database {
                        try_walk!(database.walk_mut(visitor));
                    }
                    if let Some(table) = &mut column.table {
                        try_walk!(table.walk_mut(visitor));
                    }
                    if let crate::ast::ColumnID::Name(ident) = &mut column.column {
                        try_walk!(ident.walk_mut(visitor));
                    }
                }
                Expr::IsNull { expr, .. }
                | Expr::UnaryOp { expr, .. }
                | Expr::Extract { expr, .. }
                | Expr::DatePart { expr, .. }
                | Expr::Interval { expr, .. }
                | Expr::DateTrunc { date: expr, .. }
                | Expr::LastDay { date: expr, .. }
                | Expr::PreviousDay { date: expr, .. }
                | Expr::NextDay { date: expr, .. } => {
                    try_walk!(expr.walk_mut(visitor));
                }
                Expr::Cast {
                    expr, target_type, ..
                }
                | Expr::TryCast {
                    expr, target_type, ..
                } => {
                    try_walk!(expr.walk_mut(visitor));
                    try_walk!(target_type.walk_mut(visitor));
                }
                Expr::IsDistinctFrom { left, right, .. }
                | Expr::BinaryOp { left, right, .. }
                | Expr::JsonOp { left, right, .. }
                | Expr::LikeWithEscape { left, right, .. }
                | Expr::LikeAnyWithEscape { left, right, .. } => {
                    try_walk!(left.walk_mut(visitor));
                    try_walk!(right.walk_mut(visitor));
                }
                Expr::InList { expr, list, .. } => {
                    try_walk!(expr.walk_mut(visitor));
                    for item in list {
                        try_walk!(item.walk_mut(visitor));
                    }
                }
                Expr::InSubquery { expr, subquery, .. }
                | Expr::LikeSubquery { expr, subquery, .. } => {
                    try_walk!(expr.walk_mut(visitor));
                    try_walk!(subquery.walk_mut(visitor));
                }
                Expr::Between {
                    expr, low, high, ..
                } => {
                    try_walk!(expr.walk_mut(visitor));
                    try_walk!(low.walk_mut(visitor));
                    try_walk!(high.walk_mut(visitor));
                }
                Expr::Position {
                    substr_expr,
                    str_expr,
                    ..
                } => {
                    try_walk!(substr_expr.walk_mut(visitor));
                    try_walk!(str_expr.walk_mut(visitor));
                }
                Expr::Substring {
                    expr,
                    substring_from,
                    substring_for,
                    ..
                } => {
                    try_walk!(expr.walk_mut(visitor));
                    try_walk!(substring_from.walk_mut(visitor));
                    if let Some(substring_for) = substring_for {
                        try_walk!(substring_for.walk_mut(visitor));
                    }
                }
                Expr::Trim {
                    expr, trim_where, ..
                } => {
                    try_walk!(expr.walk_mut(visitor));
                    if let Some((_, trim_expr)) = trim_where {
                        try_walk!(trim_expr.walk_mut(visitor));
                    }
                }
                Expr::CountAll {
                    window, qualified, ..
                } => {
                    for item in qualified {
                        if let crate::ast::Indirection::Identifier(ident) = item {
                            try_walk!(ident.walk_mut(visitor));
                        }
                    }
                    if let Some(window) = window {
                        try_walk!(window.walk_mut(visitor));
                    }
                }
                Expr::Tuple { exprs, .. } | Expr::Array { exprs, .. } => {
                    for expr in exprs {
                        try_walk!(expr.walk_mut(visitor));
                    }
                }
                Expr::FunctionCall { func, .. } => try_walk!(func.walk_mut(visitor)),
                Expr::Case {
                    operand,
                    conditions,
                    results,
                    else_result,
                    ..
                } => {
                    if let Some(operand) = operand {
                        try_walk!(operand.walk_mut(visitor));
                    }
                    for expr in conditions {
                        try_walk!(expr.walk_mut(visitor));
                    }
                    for expr in results {
                        try_walk!(expr.walk_mut(visitor));
                    }
                    if let Some(expr) = else_result {
                        try_walk!(expr.walk_mut(visitor));
                    }
                }
                Expr::Exists { subquery, .. } | Expr::Subquery { subquery, .. } => {
                    try_walk!(subquery.walk_mut(visitor));
                }
                Expr::MapAccess { expr, accessor, .. } => {
                    try_walk!(expr.walk_mut(visitor));
                    try_walk!(accessor.walk_mut(visitor));
                }
                Expr::Map { kvs, .. } => {
                    for (_, value) in kvs {
                        try_walk!(value.walk_mut(visitor));
                    }
                }
                Expr::DateAdd { interval, date, .. } | Expr::DateSub { interval, date, .. } => {
                    try_walk!(interval.walk_mut(visitor));
                    try_walk!(date.walk_mut(visitor));
                }
                Expr::DateDiff {
                    date_start,
                    date_end,
                    ..
                }
                | Expr::DateBetween {
                    date_start,
                    date_end,
                    ..
                } => {
                    try_walk!(date_start.walk_mut(visitor));
                    try_walk!(date_end.walk_mut(visitor));
                }
                Expr::TimeSlice { date, .. } => try_walk!(date.walk_mut(visitor)),
                Expr::Literal { .. }
                | Expr::Hole { .. }
                | Expr::Placeholder { .. }
                | Expr::StageLocation { .. } => {}
            },
            VisitControl::SkipChildren => {}
            VisitControl::Break(value) => return Ok(VisitControl::Break(value)),
        }

        visitor.leave_expr(self)?;
        Ok(VisitControl::Continue)
    }
}

impl Walk for FunctionCall {
    fn walk<V: Visitor + ?Sized>(
        &self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        try_control!(visitor.visit_function_call(self));

        try_walk!(self.name.walk(visitor));
        for arg in &self.args {
            try_walk!(arg.walk(visitor));
        }
        for param in &self.params {
            try_walk!(param.walk(visitor));
        }
        for order in &self.order_by {
            try_walk!(order.expr.walk(visitor));
        }
        if let Some(window) = &self.window {
            try_walk!(window.walk(visitor));
        }
        if let Some(lambda) = &self.lambda {
            try_walk!(lambda.walk(visitor));
        }

        Ok(VisitControl::Continue)
    }
}

impl WalkMut for FunctionCall {
    fn walk_mut<V: VisitorMut + ?Sized>(
        &mut self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        try_control!(visitor.visit_function_call(self));

        try_walk!(self.name.walk_mut(visitor));
        for arg in &mut self.args {
            try_walk!(arg.walk_mut(visitor));
        }
        for param in &mut self.params {
            try_walk!(param.walk_mut(visitor));
        }
        for order in &mut self.order_by {
            try_walk!(order.expr.walk_mut(visitor));
        }
        if let Some(window) = &mut self.window {
            try_walk!(window.walk_mut(visitor));
        }
        if let Some(lambda) = &mut self.lambda {
            try_walk!(lambda.walk_mut(visitor));
        }

        Ok(VisitControl::Continue)
    }
}

impl Walk for Identifier {
    fn walk<V: Visitor + ?Sized>(
        &self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        try_control!(visitor.visit_identifier(self));
        Ok(VisitControl::Continue)
    }
}

impl WalkMut for Identifier {
    fn walk_mut<V: VisitorMut + ?Sized>(
        &mut self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        try_control!(visitor.visit_identifier(self));
        Ok(VisitControl::Continue)
    }
}

impl Walk for MapAccessor {
    fn walk<V: Visitor + ?Sized>(
        &self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        try_control!(visitor.visit_map_accessor(self));
        match self {
            MapAccessor::Bracket { key } => try_walk!(key.walk(visitor)),
            MapAccessor::Colon { key } => try_walk!(key.walk(visitor)),
            MapAccessor::DotNumber { .. } => {}
        }
        Ok(VisitControl::Continue)
    }
}

impl WalkMut for MapAccessor {
    fn walk_mut<V: VisitorMut + ?Sized>(
        &mut self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        try_control!(visitor.visit_map_accessor(self));
        match self {
            MapAccessor::Bracket { key } => try_walk!(key.walk_mut(visitor)),
            MapAccessor::Colon { key } => try_walk!(key.walk_mut(visitor)),
            MapAccessor::DotNumber { .. } => {}
        }
        Ok(VisitControl::Continue)
    }
}

impl Walk for WindowDesc {
    fn walk<V: Visitor + ?Sized>(
        &self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        self.window.walk(visitor)
    }
}

impl WalkMut for WindowDesc {
    fn walk_mut<V: VisitorMut + ?Sized>(
        &mut self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        self.window.walk_mut(visitor)
    }
}
