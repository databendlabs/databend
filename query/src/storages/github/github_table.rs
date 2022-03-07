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
use std::fmt::Display;
use std::future::Future;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::TableInfo;
use common_planners::Extras;
use common_planners::PartitionsInfo;
use common_planners::ReadDataSourcePlan;
use common_planners::Statistics;
use common_streams::DataBlockStream;
use common_streams::SendableDataBlockStream;

use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::AsyncSource;
use crate::pipelines::new::processors::AsyncSourcer;
use crate::pipelines::new::NewPipe;
use crate::pipelines::new::NewPipeline;
use crate::sessions::QueryContext;
use crate::storages::github::RepoCommentsTable;
use crate::storages::github::RepoInfoTable;
use crate::storages::github::RepoIssuesTable;
use crate::storages::github::RepoPRsTable;
use crate::storages::github::RepoTableOptions;
use crate::storages::StorageContext;
use crate::storages::StorageDescription;
use crate::storages::Table;

pub enum GithubTableType {
    Comments,
    Info,
    Issues,
    PullRequests,
}

impl Display for GithubTableType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            GithubTableType::Comments => write!(f, "comments"),
            GithubTableType::Info => write!(f, "info"),
            GithubTableType::Issues => write!(f, "issues"),
            GithubTableType::PullRequests => write!(f, "pull_requests"),
        }
    }
}

pub struct GithubTable {
    table_info: TableInfo,
    options: RepoTableOptions,
}

impl GithubTable {
    pub fn try_create(_ctx: StorageContext, table_info: TableInfo) -> Result<Box<dyn Table>> {
        let engine_options = table_info.engine_options();
        Ok(Box::new(GithubTable {
            options: engine_options.try_into()?,
            table_info,
        }))
    }

    pub fn description() -> StorageDescription {
        StorageDescription {
            engine_name: "GITHUB".to_string(),
            comment: "GITHUB Storage Engine".to_string(),
        }
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

    async fn read_partitions(
        &self,
        _ctx: Arc<QueryContext>,
        _push_downs: Option<Extras>,
    ) -> Result<(Statistics, PartitionsInfo)> {
        Ok((Statistics::default(), vec![]))
    }

    async fn read(
        &self,
        _ctx: Arc<QueryContext>,
        _plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        let arrays = get_data_from_github(self.options.clone()).await?;
        let block = DataBlock::create(self.table_info.schema(), arrays);

        Ok(Box::pin(DataBlockStream::create(
            self.table_info.schema(),
            None,
            vec![block],
        )))
    }

    fn read2(
        &self,
        _: Arc<QueryContext>,
        _: &ReadDataSourcePlan,
        pipeline: &mut NewPipeline,
    ) -> Result<()> {
        let output = OutputPort::create();
        let options = self.options.clone();
        let schema = self.table_info.schema();
        pipeline.add_pipe(NewPipe::SimplePipe {
            inputs_port: vec![],
            outputs_port: vec![output.clone()],
            processors: vec![GithubSource::create(output, schema, options)?],
        });

        Ok(())
    }
}

#[async_trait::async_trait]
pub trait GithubDataGetter: Sync + Send {
    async fn get_data_from_github(&self) -> Result<Vec<ColumnRef>>;
}

fn get_table_type(options: &RepoTableOptions) -> Result<GithubTableType> {
    match options.table_type.as_str() {
        "comments" => Ok(GithubTableType::Comments),
        "issues" => Ok(GithubTableType::Issues),
        "pull_requests" => Ok(GithubTableType::PullRequests),
        "info" => Ok(GithubTableType::Info),
        table_type => Err(ErrorCode::UnexpectedError(format!(
            "Unsupported Github table type: {}",
            table_type
        ))),
    }
}

async fn get_data_from_github(options: RepoTableOptions) -> Result<Vec<ColumnRef>> {
    let table = match get_table_type(&options)? {
        GithubTableType::Comments => RepoCommentsTable::create(options),
        GithubTableType::Info => RepoInfoTable::create(options),
        GithubTableType::Issues => RepoIssuesTable::create(options),
        GithubTableType::PullRequests => RepoPRsTable::create(options),
    };
    table.get_data_from_github().await
}

struct GithubSource {
    finish: bool,
    schema: DataSchemaRef,
    options: RepoTableOptions,
}

impl GithubSource {
    pub fn create(
        output: Arc<OutputPort>,
        schema: DataSchemaRef,
        options: RepoTableOptions,
    ) -> Result<ProcessorPtr> {
        AsyncSourcer::create(output, GithubSource {
            schema,
            options,
            finish: false,
        })
    }
}

#[async_trait::async_trait]
impl AsyncSource for GithubSource {
    const NAME: &'static str = "GithubSource";

    type BlockFuture<'a>
    where Self: 'a
    = impl Future<Output = Result<Option<DataBlock>>>;

    fn generate(&mut self) -> Self::BlockFuture<'_> {
        async {
            if self.finish {
                return Ok(None);
            }

            self.finish = true;
            let arrays = get_data_from_github(self.options.clone()).await?;
            Ok(Some(DataBlock::create(self.schema.clone(), arrays)))
        }
    }
}
