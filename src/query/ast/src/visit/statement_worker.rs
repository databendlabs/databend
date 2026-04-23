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

impl Walk for CreateWorkerStmt {
    fn walk<V: Visitor + ?Sized>(
        &self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        try_walk!(self.name.walk(visitor));
        Ok(VisitControl::Continue)
    }
}

impl WalkMut for CreateWorkerStmt {
    fn walk_mut<V: VisitorMut + ?Sized>(
        &mut self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        try_walk!(self.name.walk_mut(visitor));
        Ok(VisitControl::Continue)
    }
}

impl Walk for AlterWorkerStmt {
    fn walk<V: Visitor + ?Sized>(
        &self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        try_walk!(self.name.walk(visitor));
        match &self.action {
            AlterWorkerAction::SetTag { tags } => {
                for tag in tags {
                    try_walk!(tag.tag_name.walk(visitor));
                }
            }
            AlterWorkerAction::UnsetTag { tags }
            | AlterWorkerAction::UnsetOptions { options: tags } => {
                for tag in tags {
                    try_walk!(tag.walk(visitor));
                }
            }
            AlterWorkerAction::SetOptions { .. }
            | AlterWorkerAction::Suspend
            | AlterWorkerAction::Resume => {}
        }
        Ok(VisitControl::Continue)
    }
}

impl WalkMut for AlterWorkerStmt {
    fn walk_mut<V: VisitorMut + ?Sized>(
        &mut self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        try_walk!(self.name.walk_mut(visitor));
        match &mut self.action {
            AlterWorkerAction::SetTag { tags } => {
                for tag in tags {
                    try_walk!(tag.tag_name.walk_mut(visitor));
                }
            }
            AlterWorkerAction::UnsetTag { tags }
            | AlterWorkerAction::UnsetOptions { options: tags } => {
                for tag in tags {
                    try_walk!(tag.walk_mut(visitor));
                }
            }
            AlterWorkerAction::SetOptions { .. }
            | AlterWorkerAction::Suspend
            | AlterWorkerAction::Resume => {}
        }
        Ok(VisitControl::Continue)
    }
}
