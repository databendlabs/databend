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
//

use std::collections::HashMap;
use std::sync::Arc;

use common_datavalues::DataField;
use common_datavalues::DataSchemaRef;
use common_datavalues::DataSchemaRefExt;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::TableMeta;
use common_planners::CreateTablePlan;
use common_planners::PlanNode;
use common_tracing::tracing;
use sqlparser::ast::ColumnDef;
use sqlparser::ast::ColumnOption;
use sqlparser::ast::ObjectName;

use super::analyzer_expr::ExpressionAnalyzer;
use crate::catalogs::Catalog;
use crate::sessions::QueryContext;
use crate::sql::statements::AnalyzableStatement;
use crate::sql::statements::AnalyzedResult;
use crate::sql::statements::DfQueryStatement;
use crate::sql::DfStatement;
use crate::sql::PlanParser;
use crate::sql::SQLCommon;

#[derive(Debug, Clone, PartialEq)]
pub struct DfCreateView {
    pub if_not_exists: bool,
    /// View Name
    pub name: ObjectName,
    /// Original SQL String, store in meta service
    pub subquery: String,
    /// Check and Analyze Select query
    pub query: DfQueryStatement,
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfCreateView {
    async fn analyze(&self, ctx: Arc<QueryContext>) -> Result<AnalyzedResult> {
        // check if query is ok
        self.query.analyze(ctx)?;
        // 
        let (db, table) = Self::resolve_table(ctx.clone(), &self.name)?;
        todo!()
    }
}

impl DfCreateView {
    fn resolve_table(ctx: Arc<QueryContext>, table_name: &ObjectName) -> Result<(String, String)> {
        let idents = &table_name.0;
        match idents.len() {
            0 => Err(ErrorCode::SyntaxException("Create table name is empty")),
            1 => Ok((ctx.get_current_database(), idents[0].value.clone())),
            2 => Ok((idents[0].value.clone(), idents[1].value.clone())),
            _ => Err(ErrorCode::SyntaxException(
                "Create table name must be [`db`].`table`",
            )),
        }
    }
}
