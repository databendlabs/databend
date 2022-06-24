// Copyright 2021 Datafuse Labs.
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

use std::collections::HashMap;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::PlanNode;
use common_planners::RenameTableEntity;
use common_planners::RenameTablePlan;
use common_tracing::tracing;
use sqlparser::ast::ObjectName;

use crate::sessions::QueryContext;
use crate::sql::statements::AnalyzableStatement;
use crate::sql::statements::AnalyzedResult;

#[derive(Debug, Clone, PartialEq)]
pub struct DfRenameTable {
    pub name_map: HashMap<ObjectName, ObjectName>,
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfRenameTable {
    #[tracing::instrument(level = "debug", skip(self, ctx), fields(ctx.id = ctx.get_id().as_str()))]
    async fn analyze(&self, ctx: Arc<QueryContext>) -> Result<AnalyzedResult> {
        let tenant = ctx.get_tenant();
        let mut entities = Vec::new();
        for (k, v) in &self.name_map {
            let (catalog, database, table) = super::resolve_table(&ctx, k, "RENAME TABLE")?;
            let (new_catalog, new_database, new_table) =
                super::resolve_table(&ctx, v, "RENAME TABLE")?;

            // TODO if catalog != new_catalog, then throws Error
            if new_catalog != catalog {
                return Err(ErrorCode::BadArguments(
                    "alter catalog not allowed while reanme table",
                ));
            }

            entities.push(RenameTableEntity {
                if_exists: false,
                catalog,
                database,
                table,
                new_database,
                new_table,
            })
        }

        Ok(AnalyzedResult::SimpleQuery(Box::new(
            PlanNode::RenameTable(RenameTablePlan { tenant, entities }),
        )))
    }
}
