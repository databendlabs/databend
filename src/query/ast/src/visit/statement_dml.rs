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

use crate::ast::*;
use crate::visit::VisitControl;
use crate::visit::Visitor;
use crate::visit::VisitorMut;
use crate::visit::Walk;
use crate::visit::WalkMut;

// FileFormatOptions is an opaque option bag. Traversal is intentionally a no-op
// and must stay explicit in visit/ rather than hidden inside derive(Walk)/derive(WalkMut).
impl_noop_walk!(FileFormatOptions);

impl Walk for CopyIntoTableStmt {
    fn walk<V: Visitor + ?Sized>(
        &self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        if let Some(with) = &self.with {
            try_walk!(with.walk(visitor));
        }
        match &self.src {
            CopyIntoTableSource::Location(_) => {}
            CopyIntoTableSource::Query {
                select_list,
                alias_name,
                ..
            } => {
                for target in select_list {
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
                                if let Indirection::Identifier(ident) = item {
                                    try_walk!(ident.walk(visitor));
                                }
                            }
                            match column_filter {
                                Some(ColumnFilter::Excludes(excludes)) => {
                                    for ident in excludes {
                                        try_walk!(ident.walk(visitor));
                                    }
                                }
                                Some(ColumnFilter::Lambda(lambda)) => {
                                    try_walk!(lambda.walk(visitor));
                                }
                                None => {}
                            }
                        }
                    }
                }
                if let Some(alias_name) = alias_name {
                    try_walk!(alias_name.walk(visitor));
                }
            }
        }
        try_walk!((&self.catalog, &self.database, &self.table).walk(visitor));
        if let Some(dst_columns) = &self.dst_columns {
            for ident in dst_columns {
                try_walk!(ident.walk(visitor));
            }
        }
        if let Some(hint) = &self.hints {
            try_walk!(hint.walk(visitor));
        }
        Ok(VisitControl::Continue)
    }
}

impl WalkMut for CopyIntoTableStmt {
    fn walk_mut<V: VisitorMut + ?Sized>(
        &mut self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        if let Some(with) = &mut self.with {
            try_walk!(with.walk_mut(visitor));
        }
        match &mut self.src {
            CopyIntoTableSource::Location(_) => {}
            CopyIntoTableSource::Query {
                select_list,
                alias_name,
                ..
            } => {
                for target in select_list {
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
                                if let Indirection::Identifier(ident) = item {
                                    try_walk!(ident.walk_mut(visitor));
                                }
                            }
                            match column_filter {
                                Some(ColumnFilter::Excludes(excludes)) => {
                                    for ident in excludes {
                                        try_walk!(ident.walk_mut(visitor));
                                    }
                                }
                                Some(ColumnFilter::Lambda(lambda)) => {
                                    try_walk!(lambda.walk_mut(visitor));
                                }
                                None => {}
                            }
                        }
                    }
                }
                if let Some(alias_name) = alias_name {
                    try_walk!(alias_name.walk_mut(visitor));
                }
            }
        }
        try_walk!((&mut self.catalog, &mut self.database, &mut self.table).walk_mut(visitor));
        if let Some(dst_columns) = &mut self.dst_columns {
            for ident in dst_columns {
                try_walk!(ident.walk_mut(visitor));
            }
        }
        if let Some(hint) = &mut self.hints {
            try_walk!(hint.walk_mut(visitor));
        }
        Ok(VisitControl::Continue)
    }
}

impl Walk for CopyIntoLocationStmt {
    fn walk<V: Visitor + ?Sized>(
        &self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        if let Some(with) = &self.with {
            try_walk!(with.walk(visitor));
        }
        if let Some(hint) = &self.hints {
            try_walk!(hint.walk(visitor));
        }
        match &self.src {
            CopyIntoLocationSource::Query(query) => try_walk!(query.walk(visitor)),
            CopyIntoLocationSource::Table {
                catalog,
                database,
                table,
                ..
            } => try_walk!((catalog, database, table).walk(visitor)),
        }
        if let Some(partition_by) = &self.partition_by {
            try_walk!(partition_by.walk(visitor));
        }
        Ok(VisitControl::Continue)
    }
}

impl WalkMut for CopyIntoLocationStmt {
    fn walk_mut<V: VisitorMut + ?Sized>(
        &mut self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        if let Some(with) = &mut self.with {
            try_walk!(with.walk_mut(visitor));
        }
        if let Some(hint) = &mut self.hints {
            try_walk!(hint.walk_mut(visitor));
        }
        match &mut self.src {
            CopyIntoLocationSource::Query(query) => try_walk!(query.walk_mut(visitor)),
            CopyIntoLocationSource::Table {
                catalog,
                database,
                table,
                ..
            } => try_walk!((catalog, database, table).walk_mut(visitor)),
        }
        if let Some(partition_by) = &mut self.partition_by {
            try_walk!(partition_by.walk_mut(visitor));
        }
        Ok(VisitControl::Continue)
    }
}

impl Walk for InsertStmt {
    fn walk<V: Visitor + ?Sized>(
        &self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        if let Some(hint) = &self.hints {
            try_walk!(hint.walk(visitor));
        }
        if let Some(with) = &self.with {
            try_walk!(with.walk(visitor));
        }
        try_walk!(self.table.walk(visitor));
        for ident in &self.columns {
            try_walk!(ident.walk(visitor));
        }
        try_walk!(self.source.walk(visitor));
        Ok(VisitControl::Continue)
    }
}

impl WalkMut for InsertStmt {
    fn walk_mut<V: VisitorMut + ?Sized>(
        &mut self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        if let Some(hint) = &mut self.hints {
            try_walk!(hint.walk_mut(visitor));
        }
        if let Some(with) = &mut self.with {
            try_walk!(with.walk_mut(visitor));
        }
        try_walk!(self.table.walk_mut(visitor));
        for ident in &mut self.columns {
            try_walk!(ident.walk_mut(visitor));
        }
        try_walk!(self.source.walk_mut(visitor));
        Ok(VisitControl::Continue)
    }
}

impl Walk for MergeIntoStmt {
    fn walk<V: Visitor + ?Sized>(
        &self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        if let Some(hint) = &self.hints {
            try_walk!(hint.walk(visitor));
        }
        try_walk!((&self.catalog, &self.database, &self.table_ident).walk(visitor));
        try_walk!(self.source.walk(visitor));
        if let Some(alias) = &self.target_alias {
            try_walk!(alias.walk(visitor));
        }
        try_walk!(self.join_expr.walk(visitor));
        for option in &self.merge_options {
            match option {
                MergeOption::Match(clause) => {
                    if let Some(selection) = &clause.selection {
                        try_walk!(selection.walk(visitor));
                    }
                    if let MatchOperation::Update { update_list, .. } = &clause.operation {
                        for update_expr in update_list {
                            try_walk!(update_expr.walk(visitor));
                        }
                    }
                }
                MergeOption::Unmatch(clause) => {
                    if let Some(selection) = &clause.selection {
                        try_walk!(selection.walk(visitor));
                    }
                    if let Some(columns) = &clause.insert_operation.columns {
                        for ident in columns {
                            try_walk!(ident.walk(visitor));
                        }
                    }
                    for value in &clause.insert_operation.values {
                        try_walk!(value.walk(visitor));
                    }
                }
            }
        }
        Ok(VisitControl::Continue)
    }
}

impl WalkMut for MergeIntoStmt {
    fn walk_mut<V: VisitorMut + ?Sized>(
        &mut self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        if let Some(hint) = &mut self.hints {
            try_walk!(hint.walk_mut(visitor));
        }
        try_walk!((&mut self.catalog, &mut self.database, &mut self.table_ident).walk_mut(visitor));
        try_walk!(self.source.walk_mut(visitor));
        if let Some(alias) = &mut self.target_alias {
            try_walk!(alias.walk_mut(visitor));
        }
        try_walk!(self.join_expr.walk_mut(visitor));
        for option in &mut self.merge_options {
            match option {
                MergeOption::Match(clause) => {
                    if let Some(selection) = &mut clause.selection {
                        try_walk!(selection.walk_mut(visitor));
                    }
                    if let MatchOperation::Update { update_list, .. } = &mut clause.operation {
                        for update_expr in update_list {
                            try_walk!(update_expr.walk_mut(visitor));
                        }
                    }
                }
                MergeOption::Unmatch(clause) => {
                    if let Some(selection) = &mut clause.selection {
                        try_walk!(selection.walk_mut(visitor));
                    }
                    if let Some(columns) = &mut clause.insert_operation.columns {
                        for ident in columns {
                            try_walk!(ident.walk_mut(visitor));
                        }
                    }
                    for value in &mut clause.insert_operation.values {
                        try_walk!(value.walk_mut(visitor));
                    }
                }
            }
        }
        Ok(VisitControl::Continue)
    }
}
