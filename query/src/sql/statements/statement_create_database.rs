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
use common_planners::CreateDatabasePlan;
use common_planners::PlanNode;
use common_tracing::tracing;
use sqlparser::ast::ObjectName;
use sqlparser::ast::SqlOption;

use crate::sessions::QueryContext;
use crate::sql::statements::AnalyzableStatement;
use crate::sql::statements::AnalyzedResult;

#[derive(Debug, Clone, PartialEq)]
pub struct DfCreateDatabase {
    pub if_not_exists: bool,
    pub name: ObjectName,
    pub engine: String,
    pub options: Vec<SqlOption>,
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfCreateDatabase {
    #[tracing::instrument(level = "info", skip(self, _ctx), fields(ctx.id = _ctx.get_id().as_str()))]
    async fn analyze(&self, _ctx: Arc<QueryContext>) -> Result<AnalyzedResult> {
        let db = self.database_name()?;
        let engine = self.database_engine()?;
        let options = self.database_options();
        let if_not_exists = self.if_not_exists;

        Ok(AnalyzedResult::SimpleQuery(Box::new(
            PlanNode::CreateDatabase(CreateDatabasePlan {
                db,
                engine,
                options,
                if_not_exists,
            }),
        )))
    }
}

impl DfCreateDatabase {
    fn database_name(&self) -> Result<String> {
        if self.name.0.is_empty() {
            return Result::Err(ErrorCode::SyntaxException("Create database name is empty"));
        }

        Ok(self.name.0[0].value.clone())
    }

    fn database_engine(&self) -> Result<String> {
        Ok(self.engine.clone())
    }

    fn database_options(&self) -> HashMap<String, String> {
        self.options
            .iter()
            .map(|option| (option.name.value.to_lowercase(), option.value.to_string()))
            .collect()
    }
}
