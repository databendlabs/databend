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
use databend_common_catalog::table::TableExt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::TableIndexType;
use databend_common_sql::plans::RefreshTableIndexPlan;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::TableContext;
use databend_common_storages_fuse::operations::do_refresh_table_index;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

pub struct RefreshTableIndexInterpreter {
    ctx: Arc<QueryContext>,
    plan: RefreshTableIndexPlan,
}

impl RefreshTableIndexInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: RefreshTableIndexPlan) -> Result<Self> {
        Ok(RefreshTableIndexInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for RefreshTableIndexInterpreter {
    fn name(&self) -> &str {
        "RefreshTableIndexInterpreter"
    }

    fn is_ddl(&self) -> bool {
        false
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        let table = self
            .ctx
            .get_table(&self.plan.catalog, &self.plan.database, &self.plan.table)
            .await?;
        // check mutability
        table.check_mutable()?;

        let index_name = self.plan.index_name.clone();
        let segment_locs = self.plan.segment_locs.clone();
        let table_meta = &table.get_table_info().meta;
        let Some(index) = table_meta.indexes.get(&index_name) else {
            return Err(ErrorCode::RefreshIndexError(format!(
                "{} index {} does not exist",
                self.plan.index_type, index_name
            )));
        };
        let table_schema = &table_meta.schema;
        let mut field_indices = Vec::with_capacity(index.column_ids.len());
        for column_id in &index.column_ids {
            for (index, field) in table_schema.fields.iter().enumerate() {
                if field.column_id() == *column_id {
                    field_indices.push(index);
                    break;
                }
            }
        }
        if field_indices.len() != index.column_ids.len() {
            return Err(ErrorCode::RefreshIndexError(format!(
                "{} index {} is invalid",
                self.plan.index_type, index_name
            )));
        }
        let index_version = index.version.clone();
        let index_schema = table_schema.project(&field_indices);

        let mut build_res = PipelineBuildResult::create();
        let fuse_table = FuseTable::try_from_table(table.as_ref())?;

        let index_type = match self.plan.index_type {
            ast::TableIndexType::Inverted => TableIndexType::Inverted,
            ast::TableIndexType::Ngram => TableIndexType::Ngram,
            ast::TableIndexType::Vector => TableIndexType::Vector,
            ast::TableIndexType::Aggregating => unreachable!(),
        };

        match self.plan.index_type {
            ast::TableIndexType::Inverted => {
                // TODO: Refactor refresh inverted index
                fuse_table
                    .do_refresh_inverted_index(
                        self.ctx.clone(),
                        index_name,
                        index_version,
                        &index.options,
                        index_schema.into(),
                        segment_locs,
                        &mut build_res.main_pipeline,
                    )
                    .await?;
            }
            _ => {
                assert!(segment_locs.is_none());
                do_refresh_table_index(
                    fuse_table,
                    self.ctx.clone(),
                    index_name,
                    index_type,
                    index_schema.into(),
                    &mut build_res.main_pipeline,
                )
                .await?;
            }
        }

        Ok(build_res)
    }
}
