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
use std::sync::Arc;

use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::TableInfo;
use common_planners::ReadDataSourcePlan;
use common_planners::S3ExternalTableInfo;
use common_planners::TruncateTablePlan;
use common_streams::SendableDataBlockStream;

use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::NewPipe;
use crate::pipelines::new::NewPipeline;
use crate::sessions::QueryContext;
use crate::storages::ExternalSource;
use crate::storages::Table;

pub struct S3ExternalTable {
    table_info: S3ExternalTableInfo,
    // This is no used but a placeholder.
    // But the Table trait need it:
    // fn get_table_info(&self) -> &TableInfo).
    table_info_placeholder: TableInfo,
}

impl S3ExternalTable {
    pub fn try_create(table_info: S3ExternalTableInfo) -> Result<Arc<dyn Table>> {
        let table_info_placeholder = TableInfo::default();
        Ok(Arc::new(Self {
            table_info,
            table_info_placeholder,
        }))
    }
}

#[async_trait::async_trait]
impl Table for S3ExternalTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    // S3 external has no table info yet.
    fn get_table_info(&self) -> &TableInfo {
        &self.table_info_placeholder
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
        let table_info = &self.table_info;
        let schema = table_info.schema.clone();

        // Add ExternalSource Pipe to the pipeline.
        let output = OutputPort::create();
        pipeline.add_pipe(NewPipe::SimplePipe {
            inputs_port: vec![],
            outputs_port: vec![output.clone()],
            processors: vec![ExternalSource::try_create(
                ctx,
                output,
                schema,
                table_info.clone(),
            )?],
        });

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
