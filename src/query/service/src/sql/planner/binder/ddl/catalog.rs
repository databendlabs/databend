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

use common_ast::ast::ShowCatalogsStmt;
use common_ast::ast::ShowLimit;
use common_exception::Result;

use crate::sql::plans::Plan;
use crate::sql::BindContext;
use crate::sql::Binder;

impl<'a> Binder {
    pub(in crate::sql::planner::binder) async fn bind_show_catalogs(
        &mut self,
        bind_context: &BindContext,
        stmt: &ShowCatalogsStmt<'a>,
    ) -> Result<Plan> {
        let ShowCatalogsStmt { limit } = stmt;
        match limit {
            Some(ShowLimit::Like { pattern }) => {
                todo!()
            }
            _ => todo!(),
        }
    }
}
