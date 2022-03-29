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
    /// Original SQL, store in meta service
    pub subquery: String,
}

#[async_trait::async_trait]
impl AnalyzableStatement for DfCreateView {
    async fn analyze(&self, _ctx: Arc<QueryContext>) -> Result<AnalyzedResult> {
        todo!()
    }
}
