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
use crate::datasources::database::github::database::REPO_INFO_ENGINE;
use crate::sessions::DatabendQueryContextRef;

const REPOSITORY: &str = "reposiroty";
const LANGUAGE: &str = "language";
const LICENSE: &str = "license";
const STAR_COUNT: &str = "star_count";
const FORKS_COUNT: &str = "forks_count";
const WATCHERS_COUNT: &str = "watchers_count";
const OPEN_ISSUES_COUNT: &str = "open_issues_count";
const SUBSCRIBERS_COUNT: &str = "subscribers_count";

pub struct RepoInfoTable {
    table_info: TableInfo,
}

impl RepoInfoTable {
    pub async fn create(ctx: DataSourceContext, owner: String, repo: String) -> Result<()> {
        let mut options = HashMap::new();
        options.insert("owner".to_string(), owner.clone());
        options.insert("repo".to_string(), repo.clone());

        let req = CreateTableReq {
            if_not_exists: false,
            db: owner.clone(),
            table: repo.clone(),
            table_meta: TableMeta {
                schema: RepoInfoTable::schema(),
                engine: REPO_INFO_ENGINE.into(),
                options: options,
            },
        };
        ctx.meta.create_table(req).await?;
        Ok(())
    }

    fn schema() -> Arc<DataSchema> {
        let fields = vec![
            DataField::new(REPOSITORY, DataType::String, false),
            DataField::new(LANGUAGE, DataType::String, true),
            DataField::new(LICENSE, DataType::String, true),
            DataField::new(STAR_COUNT, DataType::UInt32, true),
            DataField::new(FORKS_COUNT, DataType::UInt32, true),
            DataField::new(WATCHERS_COUNT, DataType::UInt32, true),
            DataField::new(OPEN_ISSUES_COUNT, DataType::UInt32, true),
            DataField::new(SUBSCRIBERS_COUNT, DataType::UInt32, true),
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
impl Table for RepoInfoTable {
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
