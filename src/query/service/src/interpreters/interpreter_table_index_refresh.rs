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

use databend_common_catalog::table::TableExt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::TableSchemaRefExt;
use databend_common_license::license::Feature;
use databend_common_license::license_manager::get_license_manager;
use databend_common_meta_app::schema::UpdateTableMetaReq;
use databend_common_meta_types::MatchSeq;
use databend_common_sql::plans::RefreshTableIndexPlan;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::TableContext;
use databend_common_storages_share::save_share_table_info;
use databend_enterprise_inverted_index::get_inverted_index_handler;
use databend_storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION;

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
        let license_manager = get_license_manager();
        license_manager
            .manager
            .check_enterprise_enabled(self.ctx.get_license_key(), Feature::InvertedIndex)?;

        let index_name = self.plan.index_name.clone();
        let catalog = self.ctx.get_catalog(&self.plan.catalog).await?;

        let table = self
            .ctx
            .get_table(&self.plan.catalog, &self.plan.database, &self.plan.table)
            .await?;
        // check mutability
        table.check_mutable()?;

        let table_meta = &table.get_table_info().meta;
        let Some(index) = table_meta.indexes.get(&index_name) else {
            return Err(ErrorCode::RefreshIndexError(format!(
                "Inverted index {} does not exist",
                index_name
            )));
        };
        let mut index_fields = Vec::with_capacity(index.column_ids.len());
        for column_id in &index.column_ids {
            for field in &table_meta.schema.fields {
                if field.column_id() == *column_id {
                    index_fields.push(field.clone());
                    break;
                }
            }
        }
        if index_fields.len() != index.column_ids.len() {
            return Err(ErrorCode::RefreshIndexError(format!(
                "Inverted index {} is invalid",
                index_name
            )));
        }
        let index_schema = TableSchemaRefExt::create(index_fields);

        let fuse_table = FuseTable::try_from_table(table.as_ref())?;

        // build index for segments and add new index info location to snapshot
        let handler = get_inverted_index_handler();
        let Some(new_snapshot_location) = handler
            .do_refresh_index(fuse_table, self.ctx.clone(), index_name, index_schema, None)
            .await?
        else {
            return Ok(PipelineBuildResult::create());
        };

        // generate new table meta with new snapshot location
        let mut new_table_meta = table.get_table_info().meta.clone();

        new_table_meta.options.insert(
            OPT_KEY_SNAPSHOT_LOCATION.to_owned(),
            new_snapshot_location.clone(),
        );

        let table_info = table.get_table_info();
        let table_id = table_info.ident.table_id;
        let table_version = table_info.ident.seq;

        let req = UpdateTableMetaReq {
            table_id,
            seq: MatchSeq::Exact(table_version),
            new_table_meta,
            copied_files: None,
            deduplicated_label: None,
            update_stream_meta: vec![],
        };

        let res = catalog.update_table_meta(table_info, req).await?;

        if let Some(share_table_info) = res.share_table_info {
            save_share_table_info(
                self.ctx.get_tenant().name(),
                self.ctx.get_data_operator()?.operator(),
                share_table_info,
            )
            .await?;
        }

        Ok(PipelineBuildResult::create())
    }
}
