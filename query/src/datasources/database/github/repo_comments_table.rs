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

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_meta_types::CreateTableReq;
use common_meta_types::TableIdent;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;
use common_planners::ReadDataSourcePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::catalogs::Table;
use crate::datasources::context::DataSourceContext;
use crate::datasources::database::github::database::REPO_COMMENTS_ENGINE;
use crate::sessions::DatabendQueryContextRef;

const RELATED_ISSUE: &str = "related_issue";
const USER: &str = "user";
const CREATED_AT: &str = "created_at";
const BODY: &str = "body";

pub struct RepoCommentsTable {
    table_info: TableInfo,
}

impl RepoCommentsTable {
    pub async fn create(ctx: DataSourceContext, owner: String, repo: String) -> Result<()> {
        let mut options = HashMap::new();
        options.insert("owner".to_string(), owner.clone());
        options.insert("repo".to_string(), repo.clone());

        let req = CreateTableReq {
            if_not_exists: false,
            db: owner,
            table: repo + "_comments",
            table_meta: TableMeta {
                schema: RepoCommentsTable::schema(),
                engine: REPO_COMMENTS_ENGINE.into(),
                options: options,
            },
        };
        ctx.meta.create_table(req).await?;
        Ok(())
    }

    fn schema() -> Arc<DataSchema> {
        let fields = vec![
            DataField::new(RELATED_ISSUE, DataType::UInt32, false),
            DataField::new(USER, DataType::String, true),
            DataField::new(CREATED_AT, DataType::String, true),
            DataField::new(BODY, DataType::String, true),
        ];

        Arc::new(DataSchema::new(fields))
    }

    pub fn build_table(table_info: &TableInfo) -> Box<dyn Table> {
        Box::new(Self {
            table_info: table_info.clone(),
        })
    }
}

#[async_trait::async_trait]
impl Table for RepoCommentsTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    async fn read(
        &self,
        ctx: DatabendQueryContextRef,
        _plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        unimplemented!()
    }
}
