// Copyright 2021 Datafuse Labs
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

use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_config::GlobalConfig;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::ColumnBuilder;
use databend_common_expression::DataBlock;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_sources::SyncSource;
use databend_common_pipeline_sources::SyncSourcer;
use log::debug;
use walkdir::WalkDir;

/// # TODO(xuanwo)
///
/// Users could store tracing log in different formats.
/// We should find a better way to support them.
pub struct TracingTable {
    table_info: TableInfo,
}

impl TracingTable {
    pub fn create(table_id: u64) -> Self {
        let schema =
            TableSchemaRefExt::create(vec![TableField::new("entry", TableDataType::String)]);

        let table_info = TableInfo {
            desc: "'system'.'tracing'".to_string(),
            name: "tracing".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemTracing".to_string(),
                ..Default::default()
            },
            ..Default::default()
        };

        TracingTable { table_info }
    }

    fn log_files() -> Result<VecDeque<String>> {
        debug!(
            "list log files from {:?}",
            std::fs::canonicalize(GlobalConfig::instance().log.file.dir.as_str())
        );
        WalkDir::new(GlobalConfig::instance().log.file.dir.as_str())
            // NOTE:(everpcpc) ignore log files in subdir with different format
            .max_depth(1)
            .sort_by_key(|file| file.file_name().to_owned())
            .into_iter()
            .filter_map(|dir_entry| match dir_entry {
                Ok(entry) if entry.path().is_dir() => None,
                Ok(entry) => Some(Ok(entry.path().display().to_string())),
                Err(cause) => Some(Err(ErrorCode::UnknownException(cause.to_string()))),
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

    #[async_backtrace::framed]
    async fn read_partitions(
        &self,
        _ctx: Arc<dyn TableContext>,
        _push_downs: Option<PushDownInfo>,
        _dry_run: bool,
    ) -> Result<(PartStatistics, Partitions)> {
        Ok((PartStatistics::default(), Partitions::default()))
    }

    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        _: &DataSourcePlan,
        pipeline: &mut Pipeline,
        _: bool,
    ) -> Result<()> {
        let settings = ctx.get_settings();

        let log_files = Self::log_files()?;
        debug!("listed log files: {:?}", log_files);
        let max_block_size = settings.get_max_block_size()? as usize;

        pipeline.add_source(
            |output| TracingSource::create(ctx.clone(), output, max_block_size, log_files.clone()),
            1,
        )?;

        Ok(())
    }
}

struct TracingSource {
    rows_pre_block: usize,
    tracing_files: VecDeque<String>,
    data_blocks: VecDeque<DataBlock>,
}

impl TracingSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        rows: usize,
        log_files: VecDeque<String>,
    ) -> Result<ProcessorPtr> {
        SyncSourcer::create(ctx, output, TracingSource {
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
                let buffer = BufReader::new(File::open(file_name.clone())?);

                let mut entry_column = ColumnBuilder::with_capacity(&DataType::String, max_rows);
                for (index, line) in buffer.lines().enumerate() {
                    if index != 0 && index % max_rows == 0 {
                        self.data_blocks
                            .push_back(DataBlock::new_from_columns(vec![entry_column.build()]));

                        entry_column = ColumnBuilder::with_capacity(&DataType::String, max_rows);
                    }
                    entry_column.push(Scalar::String(line.unwrap()).as_ref());
                }

                if entry_column.len() > 0 {
                    self.data_blocks
                        .push_back(DataBlock::new_from_columns(vec![entry_column.build()]));
                }
            }
        }
    }
}
