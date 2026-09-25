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
use databend_common_exception::Result;
use databend_common_meta_app::schema::CreateTableIndexReq;
use databend_common_meta_app::schema::TableIndexType;
use databend_common_sql::plans::CreateTableIndexPlan;
use databend_common_storages_fuse::FuseTable;
use databend_common_storages_fuse::io::InvertedIndexUserDictionary;
use databend_storages_common_table_meta::table::INVERTED_INDEX_OPT_USER_DICTIONARY_LOCATION;

use crate::interpreters::Interpreter;
use crate::interpreters::common::check_materialized_view_license;
use crate::pipelines::PipelineBuildResult;
use crate::sessions::QueryContext;
use crate::sessions::TableContextTableAccess;

pub struct CreateTableIndexInterpreter {
    ctx: Arc<QueryContext>,
    plan: CreateTableIndexPlan,
}

impl CreateTableIndexInterpreter {
    pub fn try_create(ctx: Arc<QueryContext>, plan: CreateTableIndexPlan) -> Result<Self> {
        Ok(CreateTableIndexInterpreter { ctx, plan })
    }
}

#[async_trait::async_trait]
impl Interpreter for CreateTableIndexInterpreter {
    fn name(&self) -> &str {
        "CreateTableIndexInterpreter"
    }

    fn is_ddl(&self) -> bool {
        true
    }

    #[async_backtrace::framed]
    fn execute2(&self) -> futures::future::BoxFuture<'_, Result<PipelineBuildResult>> {
        Box::pin(async move {
            let index_name = self.plan.index_name.clone();
            let column_ids = self.plan.column_ids.clone();
            let sync_creation = self.plan.sync_creation;
            let table_id = self.plan.table_id;
            let catalog = self.ctx.get_catalog(&self.plan.catalog).await?;
            let tenant = self.ctx.get_tenant();
            if let Some(table_meta) = catalog.get_table_meta_by_id(table_id).await? {
                check_materialized_view_license(&self.ctx, &table_meta.data.engine)?;
            }
            let index_type = match self.plan.index_type {
                ast::TableIndexType::Inverted => TableIndexType::Inverted,
                ast::TableIndexType::Ngram => TableIndexType::Ngram,
                ast::TableIndexType::Vector => TableIndexType::Vector,
                ast::TableIndexType::Spatial => TableIndexType::Spatial,
            };

            // The dictionary is written to the table storage before the index is registered.
            let mut options = self.plan.index_options.clone();
            if let Some(user_dictionary) = &self.plan.user_dictionary {
                let content = user_dictionary.read().await?;
                let dictionary = InvertedIndexUserDictionary::try_new(content)?;
                let table = self
                    .ctx
                    .get_table(&self.plan.catalog, &self.plan.database, &self.plan.table)
                    .await?;
                let fuse_table = FuseTable::try_from_table(table.as_ref())?;
                let location = dictionary.upload(fuse_table).await?;
                options.insert(
                    INVERTED_INDEX_OPT_USER_DICTIONARY_LOCATION.to_string(),
                    location,
                );
            }

            let create_index_req = CreateTableIndexReq {
                create_option: self.plan.create_option,
                index_type,
                tenant,
                table_id,
                name: index_name,
                column_ids,
                sync_creation,
                options,
            };

            catalog.create_table_index(create_index_req).await?;
            Ok(PipelineBuildResult::create())
        })
    }
}
