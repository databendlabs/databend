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
use databend_common_license::license::Feature;
use databend_common_license::license_manager::LicenseManagerSwitch;
use databend_common_meta_app::schema::DropTableIndexReq;
use databend_common_meta_app::schema::TableIndexType;
use databend_common_sql::plans::DropTableIndexPlan;
use databend_common_storages_fuse::TableContext;
use databend_enterprise_table_index::get_table_index_handler;

use crate::interpreters::Interpreter;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;

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
    async fn build_pipeline(&self) -> Result<PipelineBuildResult> {
        match self.plan.index_type {
            ast::TableIndexType::Inverted => {
                LicenseManagerSwitch::instance()
                    .check_enterprise_enabled(self.ctx.get_license_key(), Feature::InvertedIndex)?;
            }
            ast::TableIndexType::Ngram => {
                LicenseManagerSwitch::instance()
                    .check_enterprise_enabled(self.ctx.get_license_key(), Feature::NgramIndex)?;
            }
            ast::TableIndexType::Vector => {
                LicenseManagerSwitch::instance()
                    .check_enterprise_enabled(self.ctx.get_license_key(), Feature::VectorIndex)?;
            }
            ast::TableIndexType::Aggregating => (),
        }

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
        };

        let drop_index_req = DropTableIndexReq {
            index_type,
            tenant: self.ctx.get_tenant(),
            if_exists: self.plan.if_exists,
            table_id,
            name: index_name,
        };

        let handler = get_table_index_handler();
        let _ = handler.do_drop_table_index(catalog, drop_index_req).await?;

        Ok(PipelineBuildResult::create())
    }
}
