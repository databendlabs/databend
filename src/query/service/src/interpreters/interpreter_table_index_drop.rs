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

use std::sync::Arc;

use databend_common_ast::ast;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::DropTableIndexReq;
use databend_common_meta_app::schema::TableIndexType;
use databend_common_meta_app::schema::TableMeta;
use databend_common_sql::plans::DropTableIndexPlan;

use crate::interpreters::Interpreter;
use crate::interpreters::common::cluster_key_referenced_columns;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContextTableAccess;

pub struct DropTableIndexInterpreter {
    ctx: Arc<QueryContext>,
    plan: DropTableIndexPlan,
}

impl DropTableIndexInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: DropTableIndexPlan) -> Result<Self> {
        Ok(DropTableIndexInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for DropTableIndexInterpreter {
    fn name(&self) -> &str {
        "DropTableIndexInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let index_name = self.plan.index_name.clone();
        let table_id = self.plan.table_id;
        let catalog = self.ctx.get_catalog(&self.plan.catalog).await?;
        let index_type = match self.plan.index_type {
            ast::TableIndexType::Aggregating => {
                return Err(ErrorCode::InvalidArgument(
                    "Aggregating Index does not belong to Table Index",
                ));
            }
            ast::TableIndexType::Inverted => TableIndexType::Inverted,
            ast::TableIndexType::Ngram => TableIndexType::Ngram,
            ast::TableIndexType::Vector => TableIndexType::Vector,
            ast::TableIndexType::Spatial => TableIndexType::Spatial,
        };

        if matches!(index_type, TableIndexType::Vector)
            && let Some(table_meta) = catalog.get_table_meta_by_id(table_id).await?
        {
            validate_drop_vector_index(&table_meta.data, &index_name)?;
        }

        let drop_index_req = DropTableIndexReq {
            index_type,
            tenant: self.ctx.get_tenant(),
            if_exists: self.plan.if_exists,
            table_id,
            name: index_name,
        };

        let _ = catalog.drop_table_index(drop_index_req).await?;

        Ok(PipelineBuildResult::create())
    }
}

fn validate_drop_vector_index(table_meta: &TableMeta, index_name: &str) -> Result<()> {
    let Some(cluster_key) = table_meta.cluster_key_str() else {
        return Ok(());
    };
    let referenced_columns = cluster_key_referenced_columns(cluster_key)?;
    if referenced_columns.is_empty() {
        return Ok(());
    }

    let Some(index) = table_meta.indexes.get(index_name) else {
        return Ok(());
    };
    for column_id in &index.column_ids {
        let field = table_meta.schema.field_of_column_id(*column_id)?;
        if referenced_columns.contains(field.name()) {
            return Err(ErrorCode::AlterTableError(format!(
                "Cannot drop vector index `{}` because vector column `{}` is referenced by cluster key {}, drop or alter the cluster key first",
                index.name,
                field.name(),
                cluster_key
            )));
        }
    }

    Ok(())
}
