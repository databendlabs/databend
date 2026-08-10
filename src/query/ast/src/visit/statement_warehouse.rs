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

impl Walk for AddWarehouseClusterStmt {
    fn walk<V: Visitor + ?Sized>(
        &self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        try_walk!(self.warehouse.walk(visitor));
        try_walk!(self.cluster.walk(visitor));
        Ok(VisitControl::Continue)
    }
}

impl WalkMut for AddWarehouseClusterStmt {
    fn walk_mut<V: VisitorMut + ?Sized>(
        &mut self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        try_walk!(self.warehouse.walk_mut(visitor));
        try_walk!(self.cluster.walk_mut(visitor));
        Ok(VisitControl::Continue)
    }
}

impl Walk for CreateWarehouseStmt {
    fn walk<V: Visitor + ?Sized>(
        &self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        try_walk!(self.warehouse.walk(visitor));
        Ok(VisitControl::Continue)
    }
}

impl WalkMut for CreateWarehouseStmt {
    fn walk_mut<V: VisitorMut + ?Sized>(
        &mut self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        try_walk!(self.warehouse.walk_mut(visitor));
        Ok(VisitControl::Continue)
    }
}

impl Walk for AssignWarehouseNodesStmt {
    fn walk<V: Visitor + ?Sized>(
        &self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        try_walk!(self.warehouse.walk(visitor));
        for (cluster, _, _) in &self.node_list {
            try_walk!(cluster.walk(visitor));
        }
        Ok(VisitControl::Continue)
    }
}

impl WalkMut for AssignWarehouseNodesStmt {
    fn walk_mut<V: VisitorMut + ?Sized>(
        &mut self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        try_walk!(self.warehouse.walk_mut(visitor));
        for (cluster, _, _) in &mut self.node_list {
            try_walk!(cluster.walk_mut(visitor));
        }
        Ok(VisitControl::Continue)
    }
}

impl Walk for UnassignWarehouseNodesStmt {
    fn walk<V: Visitor + ?Sized>(
        &self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        try_walk!(self.warehouse.walk(visitor));
        for (cluster, _, _) in &self.node_list {
            try_walk!(cluster.walk(visitor));
        }
        Ok(VisitControl::Continue)
    }
}

impl WalkMut for UnassignWarehouseNodesStmt {
    fn walk_mut<V: VisitorMut + ?Sized>(
        &mut self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        try_walk!(self.warehouse.walk_mut(visitor));
        for (cluster, _, _) in &mut self.node_list {
            try_walk!(cluster.walk_mut(visitor));
        }
        Ok(VisitControl::Continue)
    }
}
