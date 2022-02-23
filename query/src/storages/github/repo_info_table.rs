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

use std::sync::Arc;

use common_datavalues::prelude::*;
use common_exception::Result;
use common_meta_types::CreateTableReq;
use common_meta_types::TableMeta;

use crate::storages::github::github_client::create_github_client;
use crate::storages::github::GithubDataGetter;
use crate::storages::github::GithubTableType;
use crate::storages::github::RepoTableOptions;
use crate::storages::StorageContext;

const REPOSITORY: &str = "reposiroty";
const LANGUAGE: &str = "language";
const LICENSE: &str = "license";
const STAR_COUNT: &str = "star_count";
const FORKS_COUNT: &str = "forks_count";
const WATCHERS_COUNT: &str = "watchers_count";
const OPEN_ISSUES_COUNT: &str = "open_issues_count";
const SUBSCRIBERS_COUNT: &str = "subscribers_count";

pub struct RepoInfoTable {
    options: RepoTableOptions,
}

impl RepoInfoTable {
    pub fn create(options: RepoTableOptions) -> Box<dyn GithubDataGetter> {
        Box::new(RepoInfoTable { options })
    }

    pub async fn create_table(
        ctx: StorageContext,
        tenant: &str,
        options: RepoTableOptions,
    ) -> Result<()> {
        let mut options = options;
        options.table_type = GithubTableType::Info.to_string();
        let req = CreateTableReq {
            if_not_exists: false,
            tenant: tenant.to_string(),
            db: options.owner.clone(),
            table: options.repo.clone(),
            table_meta: TableMeta {
                schema: RepoInfoTable::schema(),
                engine: "GITHUB".into(),
                engine_options: options.into(),
                ..Default::default()
            },
        };
        ctx.meta.create_table(req).await?;
        Ok(())
    }

    fn schema() -> Arc<DataSchema> {
        let fields = vec![
            DataField::new(REPOSITORY, Vu8::to_data_type()),
            DataField::new(LANGUAGE, Vu8::to_data_type()),
            DataField::new(LICENSE, Vu8::to_data_type()),
            DataField::new(STAR_COUNT, u32::to_data_type()),
            DataField::new(FORKS_COUNT, u32::to_data_type()),
            DataField::new(WATCHERS_COUNT, u32::to_data_type()),
            DataField::new(OPEN_ISSUES_COUNT, u32::to_data_type()),
            DataField::new(SUBSCRIBERS_COUNT, u32::to_data_type()),
        ];

        Arc::new(DataSchema::new(fields))
    }
}

#[async_trait::async_trait]
impl GithubDataGetter for RepoInfoTable {
    async fn get_data_from_github(&self) -> Result<Vec<ColumnRef>> {
        let RepoTableOptions {
            ref repo,
            ref owner,
            ref token,
            ..
        } = self.options;
        let instance = create_github_client(token)?;

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
            Series::from_data(repo_name_array),
            Series::from_data(language_array),
            Series::from_data(license_array),
            Series::from_data(star_count_array),
            Series::from_data(forks_count_array),
            Series::from_data(watchers_count_array),
            Series::from_data(open_issues_count_array),
            Series::from_data(subscribers_count_array),
        ])
    }
}
