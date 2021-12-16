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

use std::any::Any;
use std::collections::HashMap;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::Result;
use common_meta_types::CreateTableReq;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;
use common_planners::ReadDataSourcePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::sessions::QueryContext;
use crate::storages::github::github_client::create_github_client;
use crate::storages::github::github_client::get_own_repo_from_table_info;
use crate::storages::github::GITHUB_REPO_INFO_TABLE_ENGINE;
use crate::storages::github::OWNER;
use crate::storages::github::REPO;
use crate::storages::StorageContext;
use crate::storages::Table;

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
    pub fn try_create(_ctx: StorageContext, table_info: TableInfo) -> Result<Box<dyn Table>> {
        Ok(Box::new(RepoInfoTable { table_info }))
    }

    pub async fn create(ctx: StorageContext, owner: String, repo: String) -> Result<()> {
        let mut options = HashMap::new();
        options.insert(OWNER.to_string(), owner.clone());
        options.insert(REPO.to_string(), repo.clone());

        let req = CreateTableReq {
            if_not_exists: false,
            db: owner.clone(),
            table: repo.clone(),
            table_meta: TableMeta {
                schema: RepoInfoTable::schema(),
                engine: GITHUB_REPO_INFO_TABLE_ENGINE.into(),
                options,
                ..Default::default()
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

    async fn get_data_from_github(&self) -> Result<Vec<Series>> {
        let (owner, repo) = get_own_repo_from_table_info(&self.table_info)?;
        let instance = create_github_client()?;

        let repo = instance.repos(owner, repo).get().await?;

        let repo_name_array: Vec<Vec<u8>> = vec![repo.name.clone().into()];
        let language_array: Vec<Vec<u8>> = vec![repo
            .language
            .as_ref()
            .and_then(|l| l.as_str())
            .unwrap_or("No Language")
            .as_bytes()
            .to_vec()];
        let license_array: Vec<Vec<u8>> = vec![repo
            .license
            .as_ref()
            .map(|l| l.key.as_str())
            .unwrap_or("No license")
            .as_bytes()
            .to_vec()];
        let star_count_array: Vec<u32> = vec![repo.stargazers_count.unwrap_or(0)];
        let forks_count_array: Vec<u32> = vec![repo.forks_count.unwrap_or(0)];
        let watchers_count_array: Vec<u32> = vec![repo.watchers_count.unwrap_or(0)];
        let open_issues_count_array: Vec<u32> = vec![repo.open_issues_count.unwrap_or(0)];
        let subscribers_count_array: Vec<u32> = vec![repo.subscribers_count.unwrap_or(0) as u32];

        Ok(vec![
            Series::new(repo_name_array),
            Series::new(language_array),
            Series::new(license_array),
            Series::new(star_count_array),
            Series::new(forks_count_array),
            Series::new(watchers_count_array),
            Series::new(open_issues_count_array),
            Series::new(subscribers_count_array),
        ])
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
        _ctx: Arc<QueryContext>,
        _plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        let arrays = self.get_data_from_github().await?;
        let block = DataBlock::create_by_array(self.table_info.schema(), arrays);

        Ok(Box::pin(DataBlockStream::create(
            self.table_info.schema(),
            None,
            vec![block],
        )))
    }
}
