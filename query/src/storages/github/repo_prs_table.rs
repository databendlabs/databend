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
use octocrab::models;
use octocrab::params;

use crate::storages::github::github_client::create_github_client;
use crate::storages::github::GithubDataGetter;
use crate::storages::github::GithubTableType;
use crate::storages::github::RepoTableOptions;
use crate::storages::StorageContext;

const NUMBER: &str = "number";
const TITLE: &str = "title";
const STATE: &str = "state";
const USER: &str = "user";
const LABELS: &str = "labels";
const ASSIGNESS: &str = "assigness";
const CREATED_AT: &str = "created_at";
const UPDATED_AT: &str = "updated_at";
const CLOSED_AT: &str = "closed_at";

pub struct RepoPRsTable {
    options: RepoTableOptions,
}

impl RepoPRsTable {
    pub fn create(options: RepoTableOptions) -> Box<dyn GithubDataGetter> {
        Box::new(RepoPRsTable { options })
    }

    pub async fn create_table(
        ctx: StorageContext,
        tenant: &str,
        options: RepoTableOptions,
    ) -> Result<()> {
        let mut options = options;
        options.table_type = GithubTableType::PullRequests.to_string();
        let req = CreateTableReq {
            if_not_exists: false,
            tenant: tenant.to_string(),
            db: options.owner.clone(),
            table: format!("{}_{}", options.repo.clone(), "prs"),
            table_meta: TableMeta {
                schema: RepoPRsTable::schema(),
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
            DataField::new(NUMBER, u64::to_data_type()),
            DataField::new(TITLE, Vu8::to_data_type()),
            DataField::new(STATE, Vu8::to_data_type()),
            DataField::new(USER, Vu8::to_data_type()),
            DataField::new(LABELS, Vu8::to_data_type()),
            DataField::new(ASSIGNESS, Vu8::to_data_type()),
            DataField::new_nullable(CREATED_AT, DateTime32Type::arc(None)),
            DataField::new_nullable(UPDATED_AT, DateTime32Type::arc(None)),
            DataField::new_nullable(CLOSED_AT, DateTime32Type::arc(None)),
        ];

        Arc::new(DataSchema::new(fields))
    }
}

#[async_trait::async_trait]
impl GithubDataGetter for RepoPRsTable {
    async fn get_data_from_github(&self) -> Result<Vec<ColumnRef>> {
        // init array
        let mut issue_numer_array: Vec<u64> = Vec::new();
        let mut title_array: Vec<Vec<u8>> = Vec::new();
        // let mut body_array: Vec<Vec<u8>> = Vec::new();
        let mut state_array: Vec<Vec<u8>> = Vec::new();
        let mut user_array: Vec<Vec<u8>> = Vec::new();
        let mut labels_array: Vec<Vec<u8>> = Vec::new();
        let mut assigness_array: Vec<Vec<u8>> = Vec::new();
        let mut created_at_array: Vec<Option<u32>> = Vec::new();
        let mut updated_at_array: Vec<Option<u32>> = Vec::new();
        let mut closed_at_array: Vec<Option<u32>> = Vec::new();

        let RepoTableOptions {
            ref owner,
            ref repo,
            ref token,
            ..
        } = self.options;
        let instance = create_github_client(token)?;

        #[allow(unused_mut)]
        let mut page = instance
            .pulls(owner, repo)
            .list()
            // Optional Parameters
            .state(params::State::All)
            .per_page(100)
            .send()
            .await?;

        let prs = instance
            .all_pages::<models::pulls::PullRequest>(page)
            .await?;
        for pr in prs {
            issue_numer_array.push(pr.number);
            title_array.push(
                pr.title
                    .clone()
                    .unwrap_or_else(|| "".to_string())
                    .as_bytes()
                    .to_vec(),
            );
            state_array.push(
                pr.state
                    .clone()
                    .and_then(|state| match state {
                        models::IssueState::Closed => Some("Closed"),
                        models::IssueState::Open => Some("Open"),
                        _ => None,
                    })
                    .unwrap_or("Unknown")
                    .as_bytes()
                    .to_vec(),
            );
            user_array.push(
                pr.user
                    .clone()
                    .map(|user| user.login.clone())
                    .unwrap_or_else(|| "".to_string())
                    .as_bytes()
                    .to_vec(),
            );
            let mut labels_str = pr
                .labels
                .map(|labels| {
                    let label_str = labels.iter().fold(Vec::new(), |mut content, label| {
                        content.extend_from_slice(label.name.clone().as_bytes());
                        content.push(b',');
                        content
                    });
                    label_str
                })
                .unwrap_or_else(|| vec![b' ']);
            labels_str.pop();
            labels_array.push(labels_str);
            let mut assignees_str = pr
                .assignees
                .map(|assignees| {
                    let assigness_str = assignees.iter().fold(Vec::new(), |mut content, user| {
                        content.extend_from_slice(user.login.clone().as_bytes());
                        content.push(b',');
                        content
                    });
                    assigness_str
                })
                .unwrap_or_else(|| vec![b' ']);
            assignees_str.pop();
            assigness_array.push(assignees_str);
            let created_at = pr
                .created_at
                .map(|created_at| (created_at.timestamp_millis() / 1000) as u32);
            created_at_array.push(created_at);
            let updated_at = pr
                .updated_at
                .map(|updated_at| (updated_at.timestamp_millis() / 1000) as u32);
            updated_at_array.push(updated_at);
            let closed_at = pr
                .closed_at
                .map(|closed_at| (closed_at.timestamp_millis() / 1000) as u32);
            closed_at_array.push(closed_at);
        }

        Ok(vec![
            Series::from_data(issue_numer_array),
            Series::from_data(title_array),
            Series::from_data(state_array),
            Series::from_data(user_array),
            Series::from_data(labels_array),
            Series::from_data(assigness_array),
            Series::from_data(created_at_array),
            Series::from_data(updated_at_array),
            Series::from_data(closed_at_array),
        ])
    }
}
