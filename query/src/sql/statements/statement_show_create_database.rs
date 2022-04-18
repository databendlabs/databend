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

use std::sync::Arc;

use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_planners::PlanNode;
use common_planners::ShowCreateDatabasePlan;
use sqlparser::ast::ObjectName;

use crate::sessions::QueryContext;
use crate::sql::statements::AnalyzableStatement;
use crate::sql::statements::AnalyzedResult;

#[derive(Debug, Clone, PartialEq)]
pub struct DfShowCreateDatabase {
    pub name: ObjectName,
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfShowCreateDatabase {
    async fn analyze(&self, ctx: Arc<QueryContext>) -> Result<AnalyzedResult> {
        let (catalog, db) = Self::resolve_database(&ctx, &self.name)?;
        Ok(AnalyzedResult::SimpleQuery(Box::new(
            PlanNode::ShowCreateDatabase(ShowCreateDatabasePlan {
                catalog,
                db,
                schema: Self::schema(),
            }),
        )))
    }
}

impl DfShowCreateDatabase {
    fn schema() -> DataSchemaRef {
        DataSchemaRefExt::create(vec![
            DataField::new("Database", Vu8::to_data_type()),
            DataField::new("Create Database", Vu8::to_data_type()),
        ])
    }

    pub fn resolve_database(ctx: &QueryContext, name: &ObjectName) -> Result<(String, String)> {
        let idents = &name.0;
        match idents.len() {
            0 => Err(ErrorCode::SyntaxException("database name is empty")),
            1 => Ok((ctx.get_current_catalog(), idents[0].value.clone())),
            2 => Ok((idents[0].value.clone(), idents[1].value.clone())),
            _ => Err(ErrorCode::SyntaxException(
                "database name must be [`catalog`].db`",
            )),
        }
    }
}
