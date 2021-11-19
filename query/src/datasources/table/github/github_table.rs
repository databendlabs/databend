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
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::series::Series;
use common_datavalues::series::SeriesFrom;
use common_datavalues::DataField;
use common_datavalues::DataSchema;
use common_datavalues::DataType;
use common_exception::Result;
use common_meta_types::TableInfo;
use common_planners::ReadDataSourcePlan;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use super::data_accessor;
use crate::catalogs::Table;
use crate::datasources::context::TableContext;
use crate::sessions::DatabendQueryContextRef;

const REPOSITORY: &str = "reposiroty";
const LANGUAGE: &str = "language";
const LICENSE: &str = "license";
const STAR_COUNT: &str = "star_count";
const FORKS_COUNT: &str = "forks_count";
const WATCHERS_COUNT: &str = "watchers_count";
const OPEN_ISSUES_COUNT: &str = "open_issues_count";
const SUBSCRIBERS_COUNT: &str = "subscribers_count";
const CREATED_AT: &str = "created_at";

pub struct GithubTable {
    table_info: TableInfo,
}

impl GithubTable {
    pub fn try_create(table_info: TableInfo, _table_ctx: TableContext) -> Result<Box<dyn Table>> {
        let schema = GithubTable::init_schema();
        let table_info = table_info.set_schema(schema);
        Ok(Box::new(Self { table_info }))
    }

    fn init_schema() -> Arc<DataSchema> {
        let fields = vec![
            DataField::new(REPOSITORY, DataType::String, false),
            DataField::new(LANGUAGE, DataType::String, true),
            DataField::new(LICENSE, DataType::String, true),
            DataField::new(STAR_COUNT, DataType::UInt32, true),
            DataField::new(FORKS_COUNT, DataType::UInt32, true),
            DataField::new(WATCHERS_COUNT, DataType::UInt32, true),
            DataField::new(OPEN_ISSUES_COUNT, DataType::UInt32, true),
            DataField::new(SUBSCRIBERS_COUNT, DataType::UInt32, true),
            DataField::new(CREATED_AT, DataType::String, true),
        ];

        Arc::new(DataSchema::new(fields))
    }
}

#[async_trait::async_trait]
impl Table for GithubTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn get_table_info(&self) -> &TableInfo {
        &self.table_info
    }

    async fn read(
        &self,
        _ctx: DatabendQueryContextRef,
        _plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        let repo_details =
            data_accessor::get_owner_repositories_details(&self.table_info.name).await?;

        let mut repo_name_array: Vec<Vec<u8>> = Vec::new();
        let mut language_array: Vec<Vec<u8>> = Vec::new();
        let mut license_array: Vec<Vec<u8>> = Vec::new();
        let mut star_count_array: Vec<u32> = Vec::new();
        let mut forks_count_array: Vec<u32> = Vec::new();
        let mut watchers_count_array: Vec<u32> = Vec::new();
        let mut open_issues_count_array: Vec<u32> = Vec::new();
        let mut subscribers_count_array: Vec<u32> = Vec::new();
        let mut created_at_array: Vec<Vec<u8>> = Vec::new();

        let mut iter = repo_details.iter();
        for repo in &mut iter {
            repo_name_array.push(repo.name.clone().into());
            match &repo.language {
                Some(language) => language_array.push(language.clone().into()),
                None => language_array.push("".into()),
            }
            match &repo.license {
                Some(license) => license_array.push(license.key.clone().into()),
                None => license_array.push("No License".into()),
            }
            star_count_array.push(repo.stargazers_count);
            forks_count_array.push(repo.forks_count);
            watchers_count_array.push(repo.watchers_count);
            open_issues_count_array.push(repo.open_issues_count);
            subscribers_count_array.push(repo.subscribers_count);
            created_at_array.push(repo.created_at.clone().into());
        }

        let arrays: Vec<Series> = vec![
            Series::new(repo_name_array),
            Series::new(language_array),
            Series::new(license_array),
            Series::new(star_count_array),
            Series::new(forks_count_array),
            Series::new(watchers_count_array),
            Series::new(open_issues_count_array),
            Series::new(subscribers_count_array),
            Series::new(created_at_array),
        ];
        let block = DataBlock::create_by_array(self.table_info.schema(), arrays);

        Ok(Box::pin(DataBlockStream::create(
            self.table_info.schema(),
            None,
            vec![block],
        )))
    }
}
