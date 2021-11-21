// Copyright 2020 Datafuse Labs.
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

use common_datavalues::DataSchemaRefExt;
use common_exception::Result;
use common_planners::CopyPlan;
use common_planners::PlanNode;
use sqlparser::ast::Ident;
use sqlparser::ast::ObjectName;
use sqlparser::ast::SqlOption;

use crate::sessions::DatabendQueryContextRef;
use crate::sql::statements::AnalyzableStatement;
use crate::sql::statements::AnalyzedResult;

#[derive(Debug, Clone, PartialEq)]
pub struct DfCopy {
    pub name: ObjectName,
    pub columns: Vec<Ident>,
    pub location: String,
    pub format: String,
    pub options: Vec<SqlOption>,
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfCopy {
    async fn analyze(&self, ctx: DatabendQueryContextRef) -> Result<AnalyzedResult> {
        let mut db_name = ctx.get_current_database();
        let mut tbl_name = self.name.0[0].value.clone();

        if self.name.0.len() > 1 {
            db_name = tbl_name;
            tbl_name = self.name.0[1].value.clone();
        }

        let table = ctx.get_table(&db_name, &tbl_name).await?;
        let mut schema = table.schema();
        let tbl_id = table.get_id();

        if !self.columns.is_empty() {
            let fields = self
                .columns
                .iter()
                .map(|ident| schema.field_with_name(&ident.value).map(|v| v.clone()))
                .collect::<Result<Vec<_>>>()?;

            schema = DataSchemaRefExt::create(fields);
        }

        let mut options = HashMap::new();
        for p in self.options.iter() {
            options.insert(
                p.name.value.to_lowercase(),
                p.value
                    .to_string()
                    .trim_matches(|s| s == '\'' || s == '"')
                    .to_string(),
            );
        }

        let plan_node = CopyPlan {
            db_name,
            tbl_name,
            tbl_id,
            schema,
            location: self.location.clone(),
            format: self.format.clone(),
            options,
        };

        Ok(AnalyzedResult::SimpleQuery(PlanNode::Copy(plan_node)))
    }
}
