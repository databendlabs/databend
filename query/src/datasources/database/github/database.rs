//  Copyright 2021 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

use std::collections::HashMap;
use std::sync::Arc;

use common_datavalues::DataField;
use common_datavalues::DataSchemaRefExt;
use common_datavalues::DataType;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_api::MetaApi;
use common_meta_types::CreateTableReq;
use common_meta_types::DropTableReply;
use common_meta_types::DropTableReq;
use common_meta_types::GetTableReq;
use common_meta_types::ListTableReq;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;
use common_tracing::tracing;
use octocrab::params;

use crate::catalogs::Database;
use crate::catalogs::Table;
use crate::datasources::context::DataSourceContext;
use crate::datasources::database::github::RepoCommentsTable;
use crate::datasources::database::github::RepoInfoTable;
use crate::datasources::database::github::RepoIssuesTable;
use crate::datasources::database::github::RepoPrsTable;

#[derive(Clone)]
pub struct GithubDatabase {
    db_name: String,
    ctx: DataSourceContext,
}

pub const REPO_INFO_ENGINE: &str = "REPO_INFO_ENGINE";
pub const REPO_ISSUES_ENGINE: &str = "REPO_ISSUES_ENGINE";
pub const REPO_PRS_ENGINE: &str = "REPO_PRS_ENGINE";
pub const REPO_COMMENTS_ENGINE: &str = "REPO_COMMENTS_ENGINE";

impl GithubDatabase {
    pub fn try_create(db_name: &str, ctx: DataSourceContext) -> Result<Box<dyn Database>> {
        Ok(Box::new(Self {
            db_name: db_name.to_string(),
            ctx,
        }))
    }

    fn build_table(&self, table_info: &TableInfo) -> Result<Arc<dyn Table>> {
        let engine = table_info.engine();
        match engine {
            REPO_INFO_ENGINE => {
                let tbl: Arc<dyn Table> = RepoInfoTable::build_table(table_info).into();
                Ok(tbl)
            }
            REPO_ISSUES_ENGINE => {
                let tbl: Arc<dyn Table> = RepoIssuesTable::build_table(table_info).into();
                Ok(tbl)
            }
            REPO_PRS_ENGINE => {
                let tbl: Arc<dyn Table> = RepoPrsTable::build_table(table_info).into();
                Ok(tbl)
            }
            REPO_COMMENTS_ENGINE => {
                let tbl: Arc<dyn Table> = RepoCommentsTable::build_table(table_info).into();
                Ok(tbl)
            }
            _ => Err(ErrorCode::UnknownTableEngine(format!(
                "unknown table engine {}",
                engine
            ))),
        }
    }
}

#[async_trait::async_trait]
impl Database for GithubDatabase {
    fn name(&self) -> &str {
        &self.db_name
    }

    async fn init(&self) -> Result<()> {
        // 1. get all repos in this organization
        let repos = octocrab::instance()
            .orgs(&self.db_name)
            .list_repos()
            .repo_type(params::repos::Type::Sources)
            .sort(params::repos::Sort::Pushed)
            .direction(params::Direction::Descending)
            .per_page(100)
            .send()
            .await?;

        // 2. create all tables in need
        let mut iter = repos.items.iter();
        for repo in &mut iter {
            tracing::error!("creating {} related repo", &repo.name);
            // Create default db
            RepoInfoTable::create(self.ctx.clone(), self.db_name.clone(), repo.name.clone())
                .await?;

            RepoIssuesTable::create(self.ctx.clone(), self.db_name.clone(), repo.name.clone())
                .await?;

            RepoPrsTable::create(self.ctx.clone(), self.db_name.clone(), repo.name.clone()).await?;

            RepoCommentsTable::create(self.ctx.clone(), self.db_name.clone(), repo.name.clone())
                .await?;
        }

        Ok(())
    }

    async fn get_table(
        &self,
        db_name: &str,
        table_name: &str,
    ) -> common_exception::Result<Arc<dyn Table>> {
        let table_info = self
            .ctx
            .meta
            .get_table(GetTableReq::new(db_name, table_name))
            .await?;
        self.build_table(table_info.as_ref())
    }

    async fn list_tables(&self, db_name: &str) -> Result<Vec<Arc<dyn Table>>> {
        let table_infos = self
            .ctx
            .meta
            .list_tables(ListTableReq::new(db_name))
            .await?;

        table_infos.iter().try_fold(vec![], |mut acc, item| {
            let tbl = self.build_table(item.as_ref())?;
            acc.push(tbl);
            Ok(acc)
        })
    }

    async fn create_table(&self, req: CreateTableReq) -> Result<()> {
        Err(ErrorCode::UnImplement(
            "Cannot create GITHUB database table",
        ))
    }

    async fn drop_table(&self, req: DropTableReq) -> Result<DropTableReply> {
        Err(ErrorCode::UnImplement("Cannot drop GITHUB database table"))
    }
}
