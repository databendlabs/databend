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
    async fn analyze(&self, _ctx: Arc<QueryContext>) -> Result<AnalyzedResult> {
        Ok(AnalyzedResult::SimpleQuery(Box::new(
            PlanNode::ShowCreateDatabase(ShowCreateDatabasePlan {
                db: self.database_name()?,
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

    fn database_name(&self) -> Result<String> {
        if self.name.0.is_empty() {
            return Err(ErrorCode::SyntaxException(
                "Show create database name is empty",
            ));
        }

        Ok(self.name.0[0].value.clone())
    }
}
