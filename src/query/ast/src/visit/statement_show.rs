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

impl Walk for ShowOptions {
    fn walk<V: Visitor + ?Sized>(
        &self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        if let Some(limit) = &self.show_limit {
            try_walk!(limit.walk(visitor));
        }
        Ok(VisitControl::Continue)
    }
}

impl WalkMut for ShowOptions {
    fn walk_mut<V: VisitorMut + ?Sized>(
        &mut self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        if let Some(limit) = &mut self.show_limit {
            try_walk!(limit.walk_mut(visitor));
        }
        Ok(VisitControl::Continue)
    }
}

impl Walk for ShowTagsStmt {
    fn walk<V: Visitor + ?Sized>(
        &self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        if let Some(filter) = &self.filter {
            try_walk!(filter.walk(visitor));
        }
        Ok(VisitControl::Continue)
    }
}

impl WalkMut for ShowTagsStmt {
    fn walk_mut<V: VisitorMut + ?Sized>(
        &mut self,
        visitor: &mut V,
    ) -> Result<VisitControl<V::Break>, V::Error> {
        if let Some(filter) = &mut self.filter {
            try_walk!(filter.walk_mut(visitor));
        }
        Ok(VisitControl::Continue)
    }
}
