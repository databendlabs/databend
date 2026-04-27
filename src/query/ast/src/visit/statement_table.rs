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

impl Walk for CreateTableSource {
    fn walk<V: Visitor + ?Sized>(
        &self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        match self {
            CreateTableSource::Columns {
                columns,
                opt_table_indexes,
                opt_column_constraints,
                opt_table_constraints,
            } => {
                for column in columns {
                    try_walk!(column.walk(visitor));
                }
                if let Some(indexes) = opt_table_indexes {
                    for index in indexes {
                        try_walk!(index.index_name.walk(visitor));
                        for ident in &index.columns {
                            try_walk!(ident.walk(visitor));
                        }
                    }
                }
                if let Some(constraints) = opt_column_constraints {
                    for constraint in constraints {
                        try_walk!(constraint.walk(visitor));
                    }
                }
                if let Some(constraints) = opt_table_constraints {
                    for constraint in constraints {
                        try_walk!(constraint.walk(visitor));
                    }
                }
            }
            CreateTableSource::Like {
                catalog,
                database,
                table,
            } => try_walk!((catalog, database, table).walk(visitor)),
        }
        Ok(VisitControl::Continue)
    }
}

impl WalkMut for CreateTableSource {
    fn walk_mut<V: VisitorMut + ?Sized>(
        &mut self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        match self {
            CreateTableSource::Columns {
                columns,
                opt_table_indexes,
                opt_column_constraints,
                opt_table_constraints,
            } => {
                for column in columns {
                    try_walk!(column.walk_mut(visitor));
                }
                if let Some(indexes) = opt_table_indexes {
                    for index in indexes {
                        try_walk!(index.index_name.walk_mut(visitor));
                        for ident in &mut index.columns {
                            try_walk!(ident.walk_mut(visitor));
                        }
                    }
                }
                if let Some(constraints) = opt_column_constraints {
                    for constraint in constraints {
                        try_walk!(constraint.walk_mut(visitor));
                    }
                }
                if let Some(constraints) = opt_table_constraints {
                    for constraint in constraints {
                        try_walk!(constraint.walk_mut(visitor));
                    }
                }
            }
            CreateTableSource::Like {
                catalog,
                database,
                table,
            } => try_walk!((catalog, database, table).walk_mut(visitor)),
        }
        Ok(VisitControl::Continue)
    }
}

impl Walk for AttachTableStmt {
    fn walk<V: Visitor + ?Sized>(
        &self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        try_walk!((&self.catalog, &self.database, &self.table).walk(visitor));
        if let Some(columns) = &self.columns_opt {
            for ident in columns {
                try_walk!(ident.walk(visitor));
            }
        }
        Ok(VisitControl::Continue)
    }
}

impl WalkMut for AttachTableStmt {
    fn walk_mut<V: VisitorMut + ?Sized>(
        &mut self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        try_walk!((&mut self.catalog, &mut self.database, &mut self.table).walk_mut(visitor));
        if let Some(columns) = &mut self.columns_opt {
            for ident in columns {
                try_walk!(ident.walk_mut(visitor));
            }
        }
        Ok(VisitControl::Continue)
    }
}

impl Walk for CreateTableStmt {
    fn walk<V: Visitor + ?Sized>(
        &self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        try_walk!((&self.catalog, &self.database, &self.table).walk(visitor));
        if let Some(source) = &self.source {
            try_walk!(source.walk(visitor));
        }
        if let Some(cluster_by) = &self.cluster_by {
            try_walk!(cluster_by.walk(visitor));
        }
        if let Some(partitions) = &self.iceberg_table_partition {
            for ident in partitions {
                try_walk!(ident.walk(visitor));
            }
        }
        if let Some(as_query) = &self.as_query {
            try_walk!(as_query.walk(visitor));
        }
        Ok(VisitControl::Continue)
    }
}

impl WalkMut for CreateTableStmt {
    fn walk_mut<V: VisitorMut + ?Sized>(
        &mut self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        try_walk!((&mut self.catalog, &mut self.database, &mut self.table).walk_mut(visitor));
        if let Some(source) = &mut self.source {
            try_walk!(source.walk_mut(visitor));
        }
        if let Some(cluster_by) = &mut self.cluster_by {
            try_walk!(cluster_by.walk_mut(visitor));
        }
        if let Some(partitions) = &mut self.iceberg_table_partition {
            for ident in partitions {
                try_walk!(ident.walk_mut(visitor));
            }
        }
        if let Some(as_query) = &mut self.as_query {
            try_walk!(as_query.walk_mut(visitor));
        }
        Ok(VisitControl::Continue)
    }
}

impl Walk for AlterTableStmt {
    fn walk<V: Visitor + ?Sized>(
        &self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        try_walk!(self.table_reference.walk(visitor));
        match &self.action {
            AlterTableAction::RenameTable { new_table }
            | AlterTableAction::SwapWith {
                target_table: new_table,
            }
            | AlterTableAction::DropConstraint {
                constraint_name: new_table,
            }
            | AlterTableAction::DropRowAccessPolicy { policy: new_table }
            | AlterTableAction::DropColumn { column: new_table }
            | AlterTableAction::DropTableBranch {
                branch_name: new_table,
            }
            | AlterTableAction::UndropTableBranch {
                branch_name: new_table,
                ..
            }
            | AlterTableAction::DropTableTag {
                tag_name: new_table,
            } => {
                try_walk!(new_table.walk(visitor));
            }
            AlterTableAction::AddColumn { column, option, .. } => {
                try_walk!(column.walk(visitor));
                if let AddColumnOption::After(ident) = option {
                    try_walk!(ident.walk(visitor));
                }
            }
            AlterTableAction::RenameColumn {
                old_column,
                new_column,
            } => {
                try_walk!(old_column.walk(visitor));
                try_walk!(new_column.walk(visitor));
            }
            AlterTableAction::ModifyTableComment { .. }
            | AlterTableAction::DropTableClusterKey
            | AlterTableAction::RefreshTableCache
            | AlterTableAction::SetOptions { .. }
            | AlterTableAction::ModifyConnection { .. }
            | AlterTableAction::DropAllRowAccessPolicies => {}
            AlterTableAction::ModifyColumn { action } => match action {
                ModifyColumnAction::SetMaskingPolicy(column, _, using_columns) => {
                    try_walk!(column.walk(visitor));
                    if let Some(using_columns) = using_columns {
                        for ident in using_columns {
                            try_walk!(ident.walk(visitor));
                        }
                    }
                }
                ModifyColumnAction::UnsetMaskingPolicy(column)
                | ModifyColumnAction::ConvertStoredComputedColumn(column) => {
                    try_walk!(column.walk(visitor));
                }
                ModifyColumnAction::SetDataType(columns) => {
                    for column in columns {
                        try_walk!(column.walk(visitor));
                    }
                }
                ModifyColumnAction::Comment(columns) => {
                    for column in columns {
                        try_walk!(column.name.walk(visitor));
                    }
                }
            },
            AlterTableAction::AddConstraint { constraint } => {
                try_walk!(constraint.walk(visitor));
            }
            AlterTableAction::AddRowAccessPolicy { columns, policy } => {
                for column in columns {
                    try_walk!(column.walk(visitor));
                }
                try_walk!(policy.walk(visitor));
            }
            AlterTableAction::AlterTableClusterKey { cluster_by } => {
                try_walk!(cluster_by.walk(visitor));
            }
            AlterTableAction::ReclusterTable { selection, .. } => {
                if let Some(selection) = selection {
                    try_walk!(selection.walk(visitor));
                }
            }
            AlterTableAction::FlashbackTo { point } => {
                try_walk!(point.walk(visitor));
            }
            AlterTableAction::UnsetOptions { targets } => {
                for target in targets {
                    try_walk!(target.walk(visitor));
                }
            }
            AlterTableAction::CreateTableBranch { spec }
            | AlterTableAction::CreateTableTag { spec } => {
                try_walk!(spec.name.walk(visitor));
                if let Some(travel_point) = &spec.travel_point {
                    try_walk!(travel_point.walk(visitor));
                }
            }
        }
        Ok(VisitControl::Continue)
    }
}

impl WalkMut for AlterTableStmt {
    fn walk_mut<V: VisitorMut + ?Sized>(
        &mut self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        try_walk!(self.table_reference.walk_mut(visitor));
        match &mut self.action {
            AlterTableAction::RenameTable { new_table }
            | AlterTableAction::SwapWith {
                target_table: new_table,
            }
            | AlterTableAction::DropConstraint {
                constraint_name: new_table,
            }
            | AlterTableAction::DropRowAccessPolicy { policy: new_table }
            | AlterTableAction::DropColumn { column: new_table }
            | AlterTableAction::DropTableBranch {
                branch_name: new_table,
            }
            | AlterTableAction::UndropTableBranch {
                branch_name: new_table,
                ..
            }
            | AlterTableAction::DropTableTag {
                tag_name: new_table,
            } => {
                try_walk!(new_table.walk_mut(visitor));
            }
            AlterTableAction::AddColumn { column, option, .. } => {
                try_walk!(column.walk_mut(visitor));
                if let AddColumnOption::After(ident) = option {
                    try_walk!(ident.walk_mut(visitor));
                }
            }
            AlterTableAction::RenameColumn {
                old_column,
                new_column,
            } => {
                try_walk!(old_column.walk_mut(visitor));
                try_walk!(new_column.walk_mut(visitor));
            }
            AlterTableAction::ModifyTableComment { .. }
            | AlterTableAction::DropTableClusterKey
            | AlterTableAction::RefreshTableCache
            | AlterTableAction::SetOptions { .. }
            | AlterTableAction::ModifyConnection { .. }
            | AlterTableAction::DropAllRowAccessPolicies => {}
            AlterTableAction::ModifyColumn { action } => match action {
                ModifyColumnAction::SetMaskingPolicy(column, _, using_columns) => {
                    try_walk!(column.walk_mut(visitor));
                    if let Some(using_columns) = using_columns {
                        for ident in using_columns {
                            try_walk!(ident.walk_mut(visitor));
                        }
                    }
                }
                ModifyColumnAction::UnsetMaskingPolicy(column)
                | ModifyColumnAction::ConvertStoredComputedColumn(column) => {
                    try_walk!(column.walk_mut(visitor));
                }
                ModifyColumnAction::SetDataType(columns) => {
                    for column in columns {
                        try_walk!(column.walk_mut(visitor));
                    }
                }
                ModifyColumnAction::Comment(columns) => {
                    for column in columns {
                        try_walk!(column.name.walk_mut(visitor));
                    }
                }
            },
            AlterTableAction::AddConstraint { constraint } => {
                try_walk!(constraint.walk_mut(visitor));
            }
            AlterTableAction::AddRowAccessPolicy { columns, policy } => {
                for column in columns {
                    try_walk!(column.walk_mut(visitor));
                }
                try_walk!(policy.walk_mut(visitor));
            }
            AlterTableAction::AlterTableClusterKey { cluster_by } => {
                try_walk!(cluster_by.walk_mut(visitor));
            }
            AlterTableAction::ReclusterTable { selection, .. } => {
                if let Some(selection) = selection {
                    try_walk!(selection.walk_mut(visitor));
                }
            }
            AlterTableAction::FlashbackTo { point } => {
                try_walk!(point.walk_mut(visitor));
            }
            AlterTableAction::UnsetOptions { targets } => {
                for target in targets {
                    try_walk!(target.walk_mut(visitor));
                }
            }
            AlterTableAction::CreateTableBranch { spec }
            | AlterTableAction::CreateTableTag { spec } => {
                try_walk!(spec.name.walk_mut(visitor));
                if let Some(travel_point) = &mut spec.travel_point {
                    try_walk!(travel_point.walk_mut(visitor));
                }
            }
        }
        Ok(VisitControl::Continue)
    }
}

impl Walk for CreateTableIndexStmt {
    fn walk<V: Visitor + ?Sized>(
        &self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        try_walk!(self.index_name.walk(visitor));
        try_walk!(self.table.walk(visitor));
        for column in &self.columns {
            try_walk!(column.walk(visitor));
        }
        Ok(VisitControl::Continue)
    }
}

impl WalkMut for CreateTableIndexStmt {
    fn walk_mut<V: VisitorMut + ?Sized>(
        &mut self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        try_walk!(self.index_name.walk_mut(visitor));
        try_walk!(self.table.walk_mut(visitor));
        for column in &mut self.columns {
            try_walk!(column.walk_mut(visitor));
        }
        Ok(VisitControl::Continue)
    }
}

impl Walk for CreateDictionaryStmt {
    fn walk<V: Visitor + ?Sized>(
        &self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        try_walk!((&self.catalog, &self.database, &self.dictionary_name).walk(visitor));
        for column in &self.columns {
            try_walk!(column.walk(visitor));
        }
        for primary_key in &self.primary_keys {
            try_walk!(primary_key.walk(visitor));
        }
        try_walk!(self.source_name.walk(visitor));
        Ok(VisitControl::Continue)
    }
}

impl WalkMut for CreateDictionaryStmt {
    fn walk_mut<V: VisitorMut + ?Sized>(
        &mut self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        try_walk!(
            (
                &mut self.catalog,
                &mut self.database,
                &mut self.dictionary_name
            )
                .walk_mut(visitor)
        );
        for column in &mut self.columns {
            try_walk!(column.walk_mut(visitor));
        }
        for primary_key in &mut self.primary_keys {
            try_walk!(primary_key.walk_mut(visitor));
        }
        try_walk!(self.source_name.walk_mut(visitor));
        Ok(VisitControl::Continue)
    }
}

impl Walk for CreateDynamicTableStmt {
    fn walk<V: Visitor + ?Sized>(
        &self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        try_walk!((&self.catalog, &self.database, &self.table).walk(visitor));
        if let Some(source) = &self.source {
            try_walk!(source.walk(visitor));
        }
        if let Some(cluster_by) = &self.cluster_by {
            try_walk!(cluster_by.walk(visitor));
        }
        try_walk!(self.as_query.walk(visitor));
        Ok(VisitControl::Continue)
    }
}

impl WalkMut for CreateDynamicTableStmt {
    fn walk_mut<V: VisitorMut + ?Sized>(
        &mut self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        try_walk!((&mut self.catalog, &mut self.database, &mut self.table).walk_mut(visitor));
        if let Some(source) = &mut self.source {
            try_walk!(source.walk_mut(visitor));
        }
        if let Some(cluster_by) = &mut self.cluster_by {
            try_walk!(cluster_by.walk_mut(visitor));
        }
        try_walk!(self.as_query.walk_mut(visitor));
        Ok(VisitControl::Continue)
    }
}

impl Walk for RefreshIndexStmt {
    fn walk<V: Visitor + ?Sized>(
        &self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        try_walk!(self.index.walk(visitor));
        Ok(VisitControl::Continue)
    }
}

impl WalkMut for RefreshIndexStmt {
    fn walk_mut<V: VisitorMut + ?Sized>(
        &mut self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        try_walk!(self.index.walk_mut(visitor));
        Ok(VisitControl::Continue)
    }
}

impl Walk for RefreshTableIndexStmt {
    fn walk<V: Visitor + ?Sized>(
        &self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        try_walk!(self.index_name.walk(visitor));
        try_walk!(self.table.walk(visitor));
        Ok(VisitControl::Continue)
    }
}

impl WalkMut for RefreshTableIndexStmt {
    fn walk_mut<V: VisitorMut + ?Sized>(
        &mut self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        try_walk!(self.index_name.walk_mut(visitor));
        try_walk!(self.table.walk_mut(visitor));
        Ok(VisitControl::Continue)
    }
}

impl Walk for OptimizeTableStmt {
    fn walk<V: Visitor + ?Sized>(
        &self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        try_walk!(self.table_ref.walk(visitor));
        try_walk!(self.action.walk(visitor));
        Ok(VisitControl::Continue)
    }
}

impl WalkMut for OptimizeTableStmt {
    fn walk_mut<V: VisitorMut + ?Sized>(
        &mut self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        try_walk!(self.table_ref.walk_mut(visitor));
        try_walk!(self.action.walk_mut(visitor));
        Ok(VisitControl::Continue)
    }
}

impl Walk for VacuumTableOption {
    fn walk<V: Visitor + ?Sized>(
        &self,
        _visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        Ok(VisitControl::Continue)
    }
}

impl WalkMut for VacuumTableOption {
    fn walk_mut<V: VisitorMut + ?Sized>(
        &mut self,
        _visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        Ok(VisitControl::Continue)
    }
}

impl Walk for VacuumDropTableOption {
    fn walk<V: Visitor + ?Sized>(
        &self,
        _visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        Ok(VisitControl::Continue)
    }
}

impl WalkMut for VacuumDropTableOption {
    fn walk_mut<V: VisitorMut + ?Sized>(
        &mut self,
        _visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        Ok(VisitControl::Continue)
    }
}

impl Walk for RefreshVirtualColumnStmt {
    fn walk<V: Visitor + ?Sized>(
        &self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        try_walk!((&self.catalog, &self.database, &self.table).walk(visitor));
        if let Some(selection) = &self.selection {
            try_walk!(selection.walk(visitor));
        }
        Ok(VisitControl::Continue)
    }
}

impl WalkMut for RefreshVirtualColumnStmt {
    fn walk_mut<V: VisitorMut + ?Sized>(
        &mut self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        try_walk!((&mut self.catalog, &mut self.database, &mut self.table).walk_mut(visitor));
        if let Some(selection) = &mut self.selection {
            try_walk!(selection.walk_mut(visitor));
        }
        Ok(VisitControl::Continue)
    }
}
