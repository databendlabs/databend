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
use std::sync::Arc;

use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PartitionsShuffleKind;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::Scalar;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::Value;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_core::query_spill_prefix;
use databend_common_pipeline_core::Pipeline;
use databend_common_pipeline_sources::AsyncSource;
use databend_common_pipeline_sources::AsyncSourcer;
use databend_common_pipeline_sources::EmptySource;
use databend_common_storage::DataOperator;
use futures::TryStreamExt;
use log::info;
use opendal::Lister;
use opendal::Metakey;
use opendal::Operator;

use crate::table::SystemTablePart;

pub struct TempFilesTable {
    table_info: TableInfo,
}

#[async_trait::async_trait]
impl Table for TempFilesTable {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn is_local(&self) -> bool {
        // When querying a memory table, we send the partition to one node for execution. The other nodes send empty partitions.
        false
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
        Ok((
            PartStatistics::default(),
            Partitions::create(PartitionsShuffleKind::Seq, vec![Arc::new(Box::new(
                SystemTablePart,
            ))]),
        ))
    }

    fn read_data(
        &self,
        ctx: Arc<dyn TableContext>,
        plan: &DataSourcePlan,
        pipeline: &mut Pipeline,
        _put_cache: bool,
    ) -> Result<()> {
        // avoid duplicate read in cluster mode.
        if plan.parts.partitions.is_empty() {
            pipeline.add_source(EmptySource::create, 1)?;
            return Ok(());
        }

        pipeline.add_source(
            |output| {
                TempFilesTableAsyncSource::create(ctx.clone(), output, plan.push_downs.clone())
            },
            1,
        )?;

        Ok(())
    }
}

impl TempFilesTable {
    pub fn create(table_id: u64) -> Arc<dyn Table> {
        let schema = TableSchemaRefExt::create(vec![
            TableField::new("file_type", TableDataType::String),
            TableField::new("file_name", TableDataType::String),
            TableField::new(
                "file_content_length",
                TableDataType::Number(NumberDataType::UInt64),
            ),
            TableField::new(
                "file_last_modified_time",
                TableDataType::Timestamp.wrap_nullable(),
            ),
        ]);

        let table_info = TableInfo {
            desc: "'system'.'temp_files'".to_string(),
            name: "temp_files".to_string(),
            ident: TableIdent::new(table_id, 0),
            meta: TableMeta {
                schema,
                engine: "SystemTempFilesTable".to_string(),

                ..Default::default()
            },
            ..Default::default()
        };

        Arc::new(Self { table_info })
    }
}

struct TempFilesTableAsyncSource {
    finished: bool,
    context: Arc<dyn TableContext>,
    operator: Operator,
    location_prefix: String,
    entries_processed: usize,
    limit: usize,
    lister: Option<Lister>,
}

impl TempFilesTableAsyncSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        push_downs: Option<PushDownInfo>,
    ) -> Result<ProcessorPtr> {
        let tenant = ctx.get_tenant();
        let location_prefix = format!("{}/", query_spill_prefix(tenant.tenant_name(), ""));
        let limit = push_downs
            .as_ref()
            .and_then(|x| x.limit)
            .unwrap_or(usize::MAX);

        AsyncSourcer::create(ctx.clone(), output, TempFilesTableAsyncSource {
            finished: false,
            context: ctx,
            operator: DataOperator::instance().operator(),
            location_prefix,
            entries_processed: 0,
            limit,
            lister: None,
        })
    }

    fn build_block(
        names: Vec<String>,
        file_lens: Vec<u64>,
        file_last_modifieds: Vec<Option<i64>>,
    ) -> DataBlock {
        let row_number = names.len();
        DataBlock::new(
            vec![
                BlockEntry::new(
                    DataType::String,
                    Value::Scalar(Scalar::String("Spill".to_string())),
                ),
                BlockEntry::new(
                    DataType::String,
                    Value::Column(StringType::from_data(names)),
                ),
                BlockEntry::new(
                    DataType::Number(NumberDataType::UInt64),
                    Value::Column(NumberType::from_data(file_lens)),
                ),
                BlockEntry::new(
                    DataType::Timestamp.wrap_nullable(),
                    Value::Column(TimestampType::from_opt_data(file_last_modifieds)),
                ),
            ],
            row_number,
        )
    }
}

const MAX_BATCH_SIZE: usize = 1000;

#[async_trait::async_trait]
impl AsyncSource for TempFilesTableAsyncSource {
    const NAME: &'static str = "system.temp_files";

    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        if self.finished || self.limit == 0 {
            return Ok(None);
        }

        let step_limit = {
            // - Initially, self.limit is larger than self.entries_processed (which is 0).
            // - In each step, self.entries_processed will increase by at most
            //   (self.limit - self.entries_processed).
            // - We will stop processing before/when self.entries_processed reached the self.limit.
            //
            // This ensures that the subtraction will not cause underflow or runtime panic.
            let left = self.limit - self.entries_processed;
            std::cmp::min(left, MAX_BATCH_SIZE)
        };

        if step_limit == 0 {
            self.finished = true;
            info!(
                "empty step, finishing temporary file listing. limit {}, number of entries listed {} ",
                self.limit, self.entries_processed
            );
            return Ok(None);
        }

        let mut temp_files_name: Vec<String> = Vec::with_capacity(MAX_BATCH_SIZE);
        let mut temp_files_content_length = Vec::with_capacity(MAX_BATCH_SIZE);
        let mut temp_files_last_modified = Vec::with_capacity(MAX_BATCH_SIZE);

        let lister = {
            if self.lister.is_none() {
                self.lister = Some(
                    self.operator
                        .lister_with(&self.location_prefix)
                        .recursive(true)
                        .metakey(Metakey::LastModified | Metakey::ContentLength)
                        .await?,
                );
            }
            self.lister.as_mut().unwrap()
        };

        let mut processed = 0;
        while let Some(entry) = lister.try_next().await? {
            let metadata = entry.metadata();
            if metadata.is_file() {
                temp_files_name.push(
                    entry
                        .path()
                        .trim_start_matches(&self.location_prefix)
                        .to_string(),
                );

                temp_files_last_modified
                    .push(metadata.last_modified().map(|x| x.timestamp_micros()));
                temp_files_content_length.push(metadata.content_length());
            }

            processed += 1;
            if processed == step_limit {
                break;
            }
        }

        self.entries_processed += processed;

        if processed <= step_limit {
            // All the items have been listed, or we have reached the step_limit; stop processing
            self.finished = true;
            info!(
                "finishing temporary file listing. limit {}, number of entries listed {} ",
                self.limit, self.entries_processed
            );
        }

        self.context
            .set_status_info(&format!("{} entries processed", self.entries_processed));

        let data_block = Self::build_block(
            temp_files_name,
            temp_files_content_length,
            temp_files_last_modified,
        );

        return Ok(Some(data_block));
    }
}
