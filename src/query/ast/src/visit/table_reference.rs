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

use crate::ast::TableReference;
use crate::ast::TemporalClause;
use crate::ast::TimeTravelPoint;
use crate::visit::VisitControl;
use crate::visit::Visitor;
use crate::visit::VisitorMut;
use crate::visit::Walk;
use crate::visit::WalkMut;

impl Walk for TableReference {
    fn walk<V: Visitor + ?Sized>(
        &self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        try_control!(visitor.visit_table_reference(self));

        match self {
            TableReference::Table {
                table,
                alias,
                pivot,
                sample: _,
                temporal,
                unpivot,
                with_options: _,
                ..
            } => {
                if let Some(catalog) = &table.catalog {
                    try_walk!(catalog.walk(visitor));
                }
                if let Some(database) = &table.database {
                    try_walk!(database.walk(visitor));
                }
                try_walk!(table.table.walk(visitor));
                if let Some(alias) = alias {
                    try_walk!(alias.walk(visitor));
                }
                if let Some(pivot) = pivot {
                    try_walk!(pivot.walk(visitor));
                }
                if let Some(temporal) = temporal {
                    try_walk!(temporal.walk(visitor));
                }
                if let Some(unpivot) = unpivot {
                    try_walk!(unpivot.walk(visitor));
                }
            }
            TableReference::TableFunction {
                name,
                params,
                named_params,
                alias,
                ..
            } => {
                try_walk!(name.walk(visitor));
                for expr in params {
                    try_walk!(expr.walk(visitor));
                }
                for (ident, expr) in named_params {
                    try_walk!(ident.walk(visitor));
                    try_walk!(expr.walk(visitor));
                }
                if let Some(alias) = alias {
                    try_walk!(alias.walk(visitor));
                }
            }
            TableReference::Subquery {
                subquery,
                alias,
                pivot,
                unpivot,
                ..
            } => {
                try_walk!(subquery.walk(visitor));
                if let Some(alias) = alias {
                    try_walk!(alias.walk(visitor));
                }
                if let Some(pivot) = pivot {
                    try_walk!(pivot.walk(visitor));
                }
                if let Some(unpivot) = unpivot {
                    try_walk!(unpivot.walk(visitor));
                }
            }
            TableReference::Join { join, .. } => {
                match &join.condition {
                    crate::ast::JoinCondition::On(expr) => try_walk!(expr.walk(visitor)),
                    crate::ast::JoinCondition::Using(idents) => {
                        for ident in idents {
                            try_walk!(ident.walk(visitor));
                        }
                    }
                    crate::ast::JoinCondition::Natural | crate::ast::JoinCondition::None => {}
                }
                try_walk!(join.left.walk(visitor));
                try_walk!(join.right.walk(visitor));
            }
            TableReference::Location { alias, .. } => {
                if let Some(alias) = alias {
                    try_walk!(alias.walk(visitor));
                }
            }
        }

        Ok(VisitControl::Continue)
    }
}

impl WalkMut for TableReference {
    fn walk_mut<V: VisitorMut + ?Sized>(
        &mut self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        try_control!(visitor.visit_table_reference(self));

        match self {
            TableReference::Table {
                table,
                alias,
                pivot,
                sample: _,
                temporal,
                unpivot,
                with_options: _,
                ..
            } => {
                if let Some(catalog) = &mut table.catalog {
                    try_walk!(catalog.walk_mut(visitor));
                }
                if let Some(database) = &mut table.database {
                    try_walk!(database.walk_mut(visitor));
                }
                try_walk!(table.table.walk_mut(visitor));
                if let Some(alias) = alias {
                    try_walk!(alias.walk_mut(visitor));
                }
                if let Some(pivot) = pivot {
                    try_walk!(pivot.walk_mut(visitor));
                }
                if let Some(temporal) = temporal {
                    try_walk!(temporal.walk_mut(visitor));
                }
                if let Some(unpivot) = unpivot {
                    try_walk!(unpivot.walk_mut(visitor));
                }
            }
            TableReference::TableFunction {
                name,
                params,
                named_params,
                alias,
                ..
            } => {
                try_walk!(name.walk_mut(visitor));
                for expr in params {
                    try_walk!(expr.walk_mut(visitor));
                }
                for (ident, expr) in named_params {
                    try_walk!(ident.walk_mut(visitor));
                    try_walk!(expr.walk_mut(visitor));
                }
                if let Some(alias) = alias {
                    try_walk!(alias.walk_mut(visitor));
                }
            }
            TableReference::Subquery {
                subquery,
                alias,
                pivot,
                unpivot,
                ..
            } => {
                try_walk!(subquery.walk_mut(visitor));
                if let Some(alias) = alias {
                    try_walk!(alias.walk_mut(visitor));
                }
                if let Some(pivot) = pivot {
                    try_walk!(pivot.walk_mut(visitor));
                }
                if let Some(unpivot) = unpivot {
                    try_walk!(unpivot.walk_mut(visitor));
                }
            }
            TableReference::Join { join, .. } => {
                match &mut join.condition {
                    crate::ast::JoinCondition::On(expr) => try_walk!(expr.walk_mut(visitor)),
                    crate::ast::JoinCondition::Using(idents) => {
                        for ident in idents {
                            try_walk!(ident.walk_mut(visitor));
                        }
                    }
                    crate::ast::JoinCondition::Natural | crate::ast::JoinCondition::None => {}
                }
                try_walk!(join.left.walk_mut(visitor));
                try_walk!(join.right.walk_mut(visitor));
            }
            TableReference::Location { alias, .. } => {
                if let Some(alias) = alias {
                    try_walk!(alias.walk_mut(visitor));
                }
            }
        }

        Ok(VisitControl::Continue)
    }
}

impl Walk for TemporalClause {
    fn walk<V: Visitor + ?Sized>(
        &self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        match self {
            TemporalClause::TimeTravel(point) => point.walk(visitor),
            TemporalClause::Changes(changes) => {
                try_walk!(changes.at_point.walk(visitor));
                if let Some(end_point) = &changes.end_point {
                    try_walk!(end_point.walk(visitor));
                }
                Ok(VisitControl::Continue)
            }
        }
    }
}

impl WalkMut for TemporalClause {
    fn walk_mut<V: VisitorMut + ?Sized>(
        &mut self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        match self {
            TemporalClause::TimeTravel(point) => point.walk_mut(visitor),
            TemporalClause::Changes(changes) => {
                try_walk!(changes.at_point.walk_mut(visitor));
                if let Some(end_point) = &mut changes.end_point {
                    try_walk!(end_point.walk_mut(visitor));
                }
                Ok(VisitControl::Continue)
            }
        }
    }
}

impl Walk for TimeTravelPoint {
    fn walk<V: Visitor + ?Sized>(
        &self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        match self {
            TimeTravelPoint::Snapshot(_) => {}
            TimeTravelPoint::Timestamp(expr) | TimeTravelPoint::Offset(expr) => {
                try_walk!(expr.walk(visitor));
            }
            TimeTravelPoint::Stream {
                catalog,
                database,
                name,
            } => {
                if let Some(catalog) = catalog {
                    try_walk!(catalog.walk(visitor));
                }
                if let Some(database) = database {
                    try_walk!(database.walk(visitor));
                }
                try_walk!(name.walk(visitor));
            }
            TimeTravelPoint::TableTag(name) => {
                try_walk!(name.walk(visitor));
            }
        }

        Ok(VisitControl::Continue)
    }
}

impl WalkMut for TimeTravelPoint {
    fn walk_mut<V: VisitorMut + ?Sized>(
        &mut self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        match self {
            TimeTravelPoint::Snapshot(_) => {}
            TimeTravelPoint::Timestamp(expr) | TimeTravelPoint::Offset(expr) => {
                try_walk!(expr.walk_mut(visitor));
            }
            TimeTravelPoint::Stream {
                catalog,
                database,
                name,
            } => {
                if let Some(catalog) = catalog {
                    try_walk!(catalog.walk_mut(visitor));
                }
                if let Some(database) = database {
                    try_walk!(database.walk_mut(visitor));
                }
                try_walk!(name.walk_mut(visitor));
            }
            TimeTravelPoint::TableTag(name) => {
                try_walk!(name.walk_mut(visitor));
            }
        }

        Ok(VisitControl::Continue)
    }
}
