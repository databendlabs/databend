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
use std::future::Future;
use std::sync::Arc;

use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::plan::PartStatistics;
use databend_common_catalog::plan::Partitions;
use databend_common_catalog::plan::PartitionsShuffleKind;
use databend_common_catalog::plan::PushDownInfo;
use databend_common_catalog::table::DistributionLevel;
use databend_common_catalog::table::Table;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::BlockEntry;
use databend_common_expression::DataBlock;
use databend_common_expression::FromData;
use databend_common_expression::SendableDataBlockStream;
use databend_common_expression::TableDataType;
use databend_common_expression::TableField;
use databend_common_expression::TableSchemaRefExt;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::types::NumberType;
use databend_common_expression::types::StringType;
use databend_common_expression::types::TimestampType;
use databend_common_meta_app::schema::TableIdent;
use databend_common_meta_app::schema::TableInfo;
use databend_common_meta_app::schema::TableMeta;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Pipeline;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::sources::EmptySource;
use databend_common_pipeline::sources::StreamSource;
use databend_common_storage::DataOperator;
use futures::StreamExt;
use futures::stream;
use futures::stream::Chunks;
use futures::stream::Take;
use opendal::Lister;
use opendal::Metadata;
use opendal::Operator;
use opendal::operator_futures::FutureLister;

use crate::table::SystemTablePart;

pub struct TempFilesTable {
    table_info: TableInfo,
}

#[async_trait::async_trait]
impl Table for TempFilesTable {
    fn distribution_level(&self) -> DistributionLevel {
        DistributionLevel::Cluster
    }

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
            |output| TempFilesTable::create_source(ctx.clone(), output, plan.push_downs.clone()),
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

    pub fn create_source(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        push_downs: Option<PushDownInfo>,
    ) -> Result<ProcessorPtr> {
        let tenant = ctx.get_tenant();
        let location_prefix = format!("_query_spill/{}/", tenant.tenant_name());
        let limit = push_downs.as_ref().and_then(|x| x.limit);

        let operator = DataOperator::instance().spill_operator();
        let lister = operator.lister_with(&location_prefix).recursive(true);

        let stream = {
            let prefix = location_prefix.clone();
            let mut counter = 0;
            let ctx = ctx.clone();
            let builder = ListerStreamSourceBuilder::with_lister_fut(operator, lister);
            builder
                .limit_opt(limit)
                .chunk_size(MAX_BATCH_SIZE)
                .build(move |entries| {
                    counter += entries.len();
                    let block = Self::block_from_entries(&prefix, entries)?;
                    ctx.set_status_info(format!("{} entries processed", counter).as_str());
                    Ok(block)
                })?
        };

        StreamSource::create(ctx.get_scan_progress(), Some(stream), output)
    }

    fn build_block(
        names: Vec<String>,
        file_lens: Vec<u64>,
        file_last_modifieds: Vec<Option<i64>>,
    ) -> DataBlock {
        let num_rows = names.len();
        DataBlock::new(
            vec![
                BlockEntry::new_const_column_arg::<StringType>("Spill".to_string(), num_rows),
                StringType::from_data(names).into(),
                NumberType::from_data(file_lens).into(),
                TimestampType::from_opt_data(file_last_modifieds).into(),
            ],
            num_rows,
        )
    }

    fn block_from_entries(
        location_prefix: &str,
        entries: Vec<(String, Metadata)>,
    ) -> Result<DataBlock> {
        let num_items = entries.len();
        let mut temp_files_name: Vec<String> = Vec::with_capacity(num_items);
        let mut temp_files_content_length = Vec::with_capacity(num_items);
        let mut temp_files_last_modified = Vec::with_capacity(num_items);
        for (path, metadata) in entries {
            if metadata.is_file() {
                temp_files_name.push(path.trim_start_matches(location_prefix).to_string());

                temp_files_last_modified
                    .push(metadata.last_modified().map(|x| x.timestamp_micros()));
                temp_files_content_length.push(metadata.content_length());
            }
        }

        let data_block = TempFilesTable::build_block(
            temp_files_name,
            temp_files_content_length,
            temp_files_last_modified,
        );
        Ok(data_block)
    }
}

const MAX_BATCH_SIZE: usize = 1000;

pub struct ListerStreamSourceBuilder<T>
where T: Future<Output = opendal::Result<Lister>> + Send + 'static
{
    op: Operator,
    lister_fut: FutureLister<T>,
    limit: Option<usize>,
    chunk_size: usize,
}

impl<T> ListerStreamSourceBuilder<T>
where T: Future<Output = opendal::Result<Lister>> + Send + 'static
{
    pub fn with_lister_fut(op: Operator, lister_fut: FutureLister<T>) -> Self {
        Self {
            op,
            lister_fut,
            limit: None,
            chunk_size: MAX_BATCH_SIZE,
        }
    }

    pub fn limit_opt(mut self, limit: Option<usize>) -> Self {
        self.limit = limit;
        self
    }

    pub fn chunk_size(mut self, chunk_size: usize) -> Self {
        self.chunk_size = chunk_size;
        self
    }

    pub fn build(
        self,
        block_builder: impl FnMut(Vec<(String, Metadata)>) -> Result<DataBlock> + Sync + Send + 'static,
    ) -> Result<SendableDataBlockStream> {
        stream_source_from_entry_lister_with_chunk_size(
            self.op.clone(),
            self.lister_fut,
            self.limit,
            self.chunk_size,
            block_builder,
        )
    }
}

fn stream_source_from_entry_lister_with_chunk_size<T>(
    op: Operator,
    lister_fut: FutureLister<T>,
    limit: Option<usize>,
    chunk_size: usize,
    block_builder: impl FnMut(Vec<(String, Metadata)>) -> Result<DataBlock> + Sync + Send + 'static,
) -> Result<SendableDataBlockStream>
where
    T: Future<Output = opendal::Result<Lister>> + Send + 'static,
{
    enum ListerState<U: Future<Output = opendal::Result<Lister>> + Send + 'static> {
        Uninitialized(FutureLister<U>),
        Initialized(Chunks<Take<Lister>>),
    }

    let state = ListerState::<T>::Uninitialized(lister_fut);

    let stream = stream::try_unfold((state, block_builder), move |(mut state, mut builder)| {
        let op = op.clone();
        async move {
            let mut lister = {
                match state {
                    ListerState::Uninitialized(fut) => {
                        let lister = fut.await?;
                        lister.take(limit.unwrap_or(usize::MAX)).chunks(chunk_size)
                    }
                    ListerState::Initialized(l) => l,
                }
            };
            if let Some(entries) = lister.next().await {
                let mut items = Vec::with_capacity(entries.len());
                for entry in entries {
                    let (path, mut metadata) = entry?.into_parts();
                    if metadata.is_file() && metadata.last_modified().is_none() {
                        metadata = op.stat(&path).await?;
                    }
                    items.push((path, metadata))
                }

                let data_block = builder(items)?;
                state = ListerState::Initialized(lister);
                Ok(Some((data_block, (state, builder))))
            } else {
                Ok(None)
            }
        }
    });

    Ok(stream.boxed())
}
