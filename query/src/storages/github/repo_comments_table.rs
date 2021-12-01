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
use octocrab::models;

use crate::sessions::QueryContext;
use crate::storages::github::github_client::create_github_client;
use crate::storages::github::github_client::get_own_repo_from_table_info;
use crate::storages::github::GITHUB_REPO_COMMENTS_ENGINE;
use crate::storages::github::OWNER;
use crate::storages::github::REPO;
use crate::storages::StorageContext;
use crate::storages::Table;

const COMMENT_ID: &str = "comment_id";
const USER: &str = "user";
const BODY: &str = "body";

pub struct RepoCommentsTable {
    table_info: TableInfo,
}

impl RepoCommentsTable {
    pub fn try_create(_ctx: StorageContext, table_info: TableInfo) -> Result<Box<dyn Table>> {
        Ok(Box::new(RepoCommentsTable { table_info }))
    }

    pub async fn create(ctx: StorageContext, owner: String, repo: String) -> Result<()> {
        let mut options = HashMap::new();
        options.insert(OWNER.to_string(), owner.clone());
        options.insert(REPO.to_string(), repo.clone());

        let req = CreateTableReq {
            if_not_exists: false,
            db: owner,
            table: repo + "_comments",
            table_meta: TableMeta {
                schema: RepoCommentsTable::schema(),
                engine: GITHUB_REPO_COMMENTS_ENGINE.into(),
                options,
            },
        };
        ctx.meta.create_table(req).await?;
        Ok(())
    }

    fn schema() -> Arc<DataSchema> {
        let fields = vec![
            DataField::new(COMMENT_ID, DataType::UInt64, false),
            DataField::new(USER, DataType::String, true),
            DataField::new(BODY, DataType::String, true),
        ];

        Arc::new(DataSchema::new(fields))
    }

    async fn get_data_from_github(&self) -> Result<Vec<Series>> {
        // init array
        let mut id_array: Vec<u64> = Vec::new();
        let mut user_array: Vec<Vec<u8>> = Vec::new();
        let mut body_array: Vec<Vec<u8>> = Vec::new();

        // get owner repo info from table meta
        let (owner, repo) = get_own_repo_from_table_info(&self.table_info)?;
        let instance = create_github_client()?;

        #[allow(unused_mut)]
        let mut page = instance
            .issues(owner, repo)
            .list_issue_comments()
            // Optional Parameters
            .per_page(100)
            .send()
            .await?;

        let comments = instance.all_pages::<models::issues::Comment>(page).await?;
        for comment in comments {
            id_array.push(comment.id.into_inner());
            user_array.push(comment.user.login.clone().into());
            body_array.push(
                comment
                    .body
                    .unwrap_or_else(|| "".to_string())
                    .as_bytes()
                    .to_vec(),
            )
        }

        Ok(vec![
            Series::new(id_array),
            Series::new(user_array),
            Series::new(body_array),
        ])
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
