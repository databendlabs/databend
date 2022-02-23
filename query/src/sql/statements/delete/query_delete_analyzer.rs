//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use sqlparser::ast::ObjectName;

use crate::sessions::QueryContext;
use crate::sql::statements::query::JoinedSchema;
use crate::sql::statements::statement_delete::DfDeleteStatement;

pub struct DeleteSchemaAnalyzer {
    ctx: Arc<QueryContext>,
}

impl DeleteSchemaAnalyzer {
    pub fn create(ctx: Arc<QueryContext>) -> DeleteSchemaAnalyzer {
        DeleteSchemaAnalyzer { ctx }
    }

    pub async fn analyze(&self, query: &DfDeleteStatement) -> Result<JoinedSchema> {
        let (database, table) = self.resolve_table(&query.from)?;
        let read_table = self.ctx.get_table(&database, &table).await?;
        let name_prefix = vec![database, table];
        JoinedSchema::from_table(read_table, name_prefix)
    }

    fn resolve_table(&self, name: &ObjectName) -> Result<(String, String)> {
        match name.0.len() {
            0 => Err(ErrorCode::SyntaxException("Table name is empty")),
            1 => Ok((self.ctx.get_current_database(), name.0[0].value.clone())),
            2 => Ok((name.0[0].value.clone(), name.0[1].value.clone())),
            _ => Err(ErrorCode::SyntaxException(
                "Table name must be [`db`].`table`",
            )),
        }
    }
}
