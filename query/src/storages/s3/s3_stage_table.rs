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
use std::collections::VecDeque;
use std::sync::Arc;

use common_base::infallible::Mutex;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::TableInfo;
use common_planners::Extras;
use common_planners::Partitions;
use common_planners::ReadDataSourcePlan;
use common_planners::S3StageTableInfo;
use common_planners::Statistics;
use common_planners::TruncateTablePlan;
use common_streams::SendableDataBlockStream;

use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::NewPipeline;
use crate::pipelines::new::SourcePipeBuilder;
use crate::sessions::QueryContext;
use crate::storages::StageSource;
use crate::storages::Table;

pub struct S3StageTable {
    table_info: S3StageTableInfo,
    // This is no used but a placeholder.
    // But the Table trait need it:
    // fn get_table_info(&self) -> &TableInfo).
    table_info_placeholder: TableInfo,
}

impl S3StageTable {
    pub fn try_create(table_info: S3StageTableInfo) -> Result<Arc<dyn Table>> {
        let table_info_placeholder = TableInfo::default();
        Ok(Arc::new(Self {
            table_info,
            table_info_placeholder,
        }))
    }
}

#[async_trait::async_trait]
impl Table for S3StageTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    // S3 external has no table info yet.
    fn get_table_info(&self) -> &TableInfo {
        &self.table_info_placeholder
    }

    async fn read_partitions(
        &self,
        _ctx: Arc<QueryContext>,
        _push_downs: Option<Extras>,
    ) -> Result<(Statistics, Partitions)> {
        Ok((Statistics::default(), vec![]))
    }

    // S3 external only supported new pipeline.
    // TODO(bohu): Remove after new pipeline ready.
    async fn read(
        &self,
        _ctx: Arc<QueryContext>,
        _plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        Err(ErrorCode::UnImplement(
            "S3 external table not support read()!",
        ))
    }

    fn read2(
        &self,
        ctx: Arc<QueryContext>,
        _plan: &ReadDataSourcePlan,
        pipeline: &mut NewPipeline,
    ) -> Result<()> {
        let settings = ctx.get_settings();
        let mut builder = SourcePipeBuilder::create();
        let table_info = &self.table_info;
        let schema = table_info.schema.clone();
        let mut files_deque = VecDeque::with_capacity(table_info.files.len());
        for f in &table_info.files {
            files_deque.push_back(f.to_string());
        }
        let files = Arc::new(Mutex::new(files_deque));

        for _index in 0..settings.get_max_threads()? {
            let output = OutputPort::create();
            builder.add_source(
                output.clone(),
                StageSource::try_create(
                    ctx.clone(),
                    output,
                    schema.clone(),
                    table_info.clone(),
                    files.clone(),
                )?,
            );
        }

        pipeline.add_pipe(builder.finalize());
        Ok(())
    }

    // Write data to s3 file.
    async fn append_data(
        &self,
        _ctx: Arc<QueryContext>,
        _stream: SendableDataBlockStream,
    ) -> Result<SendableDataBlockStream> {
        Err(ErrorCode::UnImplement(
            "S3 external table append_data() unimplemented yet!",
        ))
    }

    // Truncate the s3 file.
    async fn truncate(
        &self,
        _ctx: Arc<QueryContext>,
        _truncate_plan: TruncateTablePlan,
    ) -> Result<()> {
        Err(ErrorCode::UnImplement(
            "S3 external table truncate() unimplemented yet!",
        ))
    }
}
