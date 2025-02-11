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

use std::collections::HashMap;
use std::sync::Arc;

use databend_common_ast::ast::Engine;
use databend_common_catalog::table::TableExt;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::UpsertTableOptionReq;
use databend_common_meta_types::MatchSeq;
use databend_common_sql::plans::SetOptionsPlan;
use databend_common_storages_fuse::TableContext;
use databend_storages_common_table_meta::table::OPT_KEY_CHANGE_TRACKING;
use databend_storages_common_table_meta::table::OPT_KEY_CHANGE_TRACKING_BEGIN_VER;
use databend_storages_common_table_meta::table::OPT_KEY_CLUSTER_TYPE;
use databend_storages_common_table_meta::table::OPT_KEY_DATABASE_ID;
use databend_storages_common_table_meta::table::OPT_KEY_SEGMENT_FORMAT;
use databend_storages_common_table_meta::table::OPT_KEY_STORAGE_FORMAT;
use databend_storages_common_table_meta::table::OPT_KEY_TEMP_PREFIX;
use log::error;

use crate::interpreters::common::table_option_validation::is_valid_block_per_segment;
use crate::interpreters::common::table_option_validation::is_valid_bloom_index_columns;
use crate::interpreters::common::table_option_validation::is_valid_create_opt;
use crate::interpreters::common::table_option_validation::is_valid_data_retention_period;
use crate::interpreters::common::table_option_validation::is_valid_row_per_block;
use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

pub struct SetOptionsInterpreter {
    ctx: Arc<QueryContext>,
    plan: SetOptionsPlan,
}

impl SetOptionsInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: SetOptionsPlan) -> Result<Self> {
        Ok(SetOptionsInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for SetOptionsInterpreter {
    fn name(&self) -> &str {
        "SetOptionsInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    async fn execute2(&self) -> Result<PipelineBuildResult> {
        // valid_options_check and do request to meta_srv
        let mut options_map = HashMap::new();
        // check block_per_segment
        is_valid_block_per_segment(&self.plan.set_options)?;
        // check row_per_block
        is_valid_row_per_block(&self.plan.set_options)?;
        // check data_retention_period
        is_valid_data_retention_period(&self.plan.set_options)?;

        // check storage_format
        let error_str = "invalid opt for fuse table in alter table statement";
        if self.plan.set_options.contains_key(OPT_KEY_STORAGE_FORMAT) {
            error!("{}", &error_str);
            return Err(ErrorCode::TableOptionInvalid(format!(
                "can't change {} for alter table statement",
                OPT_KEY_STORAGE_FORMAT
            )));
        }

        // check segment_format
        if self.plan.set_options.contains_key(OPT_KEY_SEGMENT_FORMAT) {
            error!("{}", &error_str);
            return Err(ErrorCode::TableOptionInvalid(format!(
                "can't change {} for alter table statement",
                OPT_KEY_SEGMENT_FORMAT
            )));
        }

        if self.plan.set_options.contains_key(OPT_KEY_DATABASE_ID) {
            error!("{}", &error_str);
            return Err(ErrorCode::TableOptionInvalid(format!(
                "can't change {} for alter table statement",
                OPT_KEY_DATABASE_ID
            )));
        }
        if self.plan.set_options.contains_key(OPT_KEY_TEMP_PREFIX) {
            error!("{}", &error_str);
            return Err(ErrorCode::TableOptionInvalid(format!(
                "can't change {} for alter table statement",
                OPT_KEY_TEMP_PREFIX
            )));
        }
        if self.plan.set_options.contains_key(OPT_KEY_CLUSTER_TYPE) {
            error!("{}", &error_str);
            return Err(ErrorCode::TableOptionInvalid(format!(
                "can't change {} for alter table statement",
                OPT_KEY_CLUSTER_TYPE
            )));
        }
        let catalog = self.ctx.get_catalog(self.plan.catalog.as_str()).await?;
        let database = self.plan.database.as_str();
        let table_name = self.plan.table.as_str();
        let table = catalog
            .get_table(&self.ctx.get_tenant(), database, table_name)
            .await?;

        for table_option in self.plan.set_options.iter() {
            let key = table_option.0.to_lowercase();
            let engine = Engine::from(table.engine());
            if !is_valid_create_opt(&key, &engine) {
                error!("{}", &error_str);
                return Err(ErrorCode::TableOptionInvalid(format!(
                    "table option {key} is invalid for alter table statement",
                )));
            }
            options_map.insert(key, Some(table_option.1.clone()));
        }

        let table_version = table.get_table_info().ident.seq;
        if let Some(value) = self.plan.set_options.get(OPT_KEY_CHANGE_TRACKING) {
            let change_tracking = value.to_lowercase().parse::<bool>()?;
            if table.change_tracking_enabled() != change_tracking {
                let begin_version = if change_tracking {
                    Some(table_version.to_string())
                } else {
                    None
                };
                options_map.insert(OPT_KEY_CHANGE_TRACKING_BEGIN_VER.to_string(), begin_version);
            }
        }

        // check mutability
        table.check_mutable()?;

        // check bloom_index_columns.
        is_valid_bloom_index_columns(&self.plan.set_options, table.schema())?;

        let req = UpsertTableOptionReq {
            table_id: table.get_id(),
            seq: MatchSeq::Exact(table_version),
            options: options_map,
        };

        let _resp = catalog
            .upsert_table_option(&self.ctx.get_tenant(), database, req)
            .await?;
        Ok(PipelineBuildResult::create())
    }
}
