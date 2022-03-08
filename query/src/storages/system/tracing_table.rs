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
use std::fs::File;
use std::io::BufRead;
use std::io::BufReader;
use std::sync::Arc;

use common_datablocks::DataBlock;
use common_datavalues::prelude::*;
use common_exception::ErrorCode;
use common_exception::Result;
use common_meta_types::TableIdent;
use common_meta_types::TableInfo;
use common_meta_types::TableMeta;
use common_planners::Extras;
use common_planners::Partitions;
use common_planners::ReadDataSourcePlan;
use common_planners::Statistics;
use common_streams::SendableDataBlockStream;
use common_tracing::tracing;
use walkdir::WalkDir;

use crate::pipelines::new::processors::port::OutputPort;
use crate::pipelines::new::processors::processor::ProcessorPtr;
use crate::pipelines::new::processors::SyncSource;
use crate::pipelines::new::processors::SyncSourcer;
use crate::pipelines::new::NewPipe;
use crate::pipelines::new::NewPipeline;
use crate::sessions::QueryContext;
use crate::storages::system::tracing_table_stream::LogEntry;
use crate::storages::system::TracingTableStream;
use crate::storages::Table;

pub struct TracingTable {
    table_info: TableInfo,
}

impl TracingTable {
    pub fn create(table_id: u64) -> Self {
        // {"v":0,"name":"databend-query","msg":"Group by partial cost: 9.071158ms","level":20,"hostname":"databend","pid":56776,"time":"2021-06-24T02:17:28.679642889+00:00"}

        let schema = DataSchemaRefExt::create(vec![
            DataField::new("v", i64::to_data_type()),
            DataField::new("name", Vu8::to_data_type()),
            DataField::new("msg", Vu8::to_data_type()),
            DataField::new("level", i8::to_data_type()),
            DataField::new("hostname", Vu8::to_data_type()),
            DataField::new("pid", i64::to_data_type()),
            DataField::new("time", Vu8::to_data_type()),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'tracing'".to_string(),
            name: "tracing".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemTracing".to_string(),
                ..Default::default()
            },
        };

        TracingTable { table_info }
    }

    fn log_files(ctx: Arc<QueryContext>) -> Result<VecDeque<String>> {
        WalkDir::new(ctx.get_config().log.log_dir.as_str())
            .sort_by_key(|file| file.file_name().to_owned())
            .into_iter()
            .filter_map(|dir_entry| match dir_entry {
                Ok(entry) if entry.path().is_dir() => None,
                Ok(entry) => Some(Ok(entry.path().display().to_string())),
                Err(cause) => Some(Err(ErrorCode::UnknownException(format!("{}", cause)))),
            })
            .collect::<Result<VecDeque<String>>>()
    }
}

#[async_trait::async_trait]
impl Table for TracingTable {
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
    ) -> Result<(Statistics, Partitions)> {
        Ok((Statistics::default(), vec![]))
    }

    async fn read(
        &self,
        ctx: Arc<QueryContext>,
        plan: &ReadDataSourcePlan,
    ) -> Result<SendableDataBlockStream> {
        let log_files = Self::log_files(ctx)?;

        // Default limit.
        let mut limit = 100000000_usize;
        tracing::debug!("read push_down:{:?}", &plan.push_downs);

        if let Some(extras) = &plan.push_downs {
            if let Some(limit_push_down) = extras.limit {
                limit = limit_push_down;
            }
        }

        Ok(Box::pin(TracingTableStream::try_create(
            self.table_info.schema(),
            log_files,
            limit,
        )?))
    }

    fn read2(
        &self,
        ctx: Arc<QueryContext>,
        _: &ReadDataSourcePlan,
        pipeline: &mut NewPipeline,
    ) -> Result<()> {
        let settings = ctx.get_settings();

        let output = OutputPort::create();
        let log_files = Self::log_files(ctx)?;
        let schema = self.table_info.schema();
        let max_block_size = settings.get_max_block_size()? as usize;

        pipeline.add_pipe(NewPipe::SimplePipe {
            inputs_port: vec![],
            outputs_port: vec![output.clone()],
            processors: vec![TracingSource::create(
                output,
                max_block_size,
                log_files,
                schema,
            )?],
        });

        Ok(())
    }
}

struct TracingSource {
    rows_pre_block: usize,
    schema: DataSchemaRef,
    tracing_files: VecDeque<String>,
    data_blocks: VecDeque<DataBlock>,
}

impl TracingSource {
    pub fn create(
        output: Arc<OutputPort>,
        rows: usize,
        log_files: VecDeque<String>,
        schema: DataSchemaRef,
    ) -> Result<ProcessorPtr> {
        SyncSourcer::create(output, TracingSource {
            schema,
            rows_pre_block: rows,
            tracing_files: log_files,
            data_blocks: Default::default(),
        })
    }
}

impl SyncSource for TracingSource {
    const NAME: &'static str = "system.tracing";

    fn generate(&mut self) -> Result<Option<DataBlock>> {
        loop {
            if let Some(data_block) = self.data_blocks.pop_front() {
                return Ok(Some(data_block));
            }

            if self.tracing_files.is_empty() {
                return Ok(None);
            }

            if let Some(file_name) = self.tracing_files.pop_front() {
                let max_rows = self.rows_pre_block;
                let buffer = BufReader::new(File::open(file_name)?);

                let mut time_column = MutableStringColumn::with_capacity(max_rows);
                let mut host_column = MutableStringColumn::with_capacity(max_rows);
                let mut msg_column = MutableStringColumn::with_capacity(max_rows);
                let mut name_column = MutableStringColumn::with_capacity(max_rows);
                let mut level_column = MutablePrimitiveColumn::<i8>::with_capacity(max_rows);
                let mut pid_column = MutablePrimitiveColumn::<i64>::with_capacity(max_rows);
                let mut version_column = MutablePrimitiveColumn::<i64>::with_capacity(max_rows);

                for (index, line) in buffer.lines().enumerate() {
                    if index != 0 && index % max_rows == 0 {
                        self.data_blocks
                            .push_back(DataBlock::create(self.schema.clone(), vec![
                                Arc::new(version_column.finish()),
                                Arc::new(name_column.finish()),
                                Arc::new(msg_column.finish()),
                                Arc::new(level_column.finish()),
                                Arc::new(host_column.finish()),
                                Arc::new(pid_column.finish()),
                                Arc::new(time_column.finish()),
                            ]));

                        time_column = MutableStringColumn::with_capacity(max_rows);
                        host_column = MutableStringColumn::with_capacity(max_rows);
                        msg_column = MutableStringColumn::with_capacity(max_rows);
                        name_column = MutableStringColumn::with_capacity(max_rows);
                        level_column = MutablePrimitiveColumn::<i8>::with_capacity(max_rows);
                        pid_column = MutablePrimitiveColumn::<i64>::with_capacity(max_rows);
                        version_column = MutablePrimitiveColumn::<i64>::with_capacity(max_rows);
                    }

                    let entry: LogEntry = serde_json::from_str(line.unwrap().as_str())?;
                    pid_column.push(entry.pid);
                    version_column.push(entry.v);
                    level_column.push(entry.level);
                    msg_column.push(entry.msg.as_bytes());
                    name_column.push(entry.name.as_bytes());
                    time_column.push(entry.time.as_bytes());
                    host_column.push(entry.hostname.as_bytes());
                }

                if !pid_column.is_empty() {
                    self.data_blocks
                        .push_back(DataBlock::create(self.schema.clone(), vec![
                            Arc::new(version_column.finish()),
                            Arc::new(name_column.finish()),
                            Arc::new(msg_column.finish()),
                            Arc::new(level_column.finish()),
                            Arc::new(host_column.finish()),
                            Arc::new(pid_column.finish()),
                            Arc::new(time_column.finish()),
                        ]));
                }
            }
        }
    }
}
