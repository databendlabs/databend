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
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use databend_common_base::base::Progress;
use databend_common_base::base::ProgressValues;
use databend_common_catalog::plan::DataSourcePlan;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_expression::DataSchemaRef;
use databend_common_pipeline::core::Event;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::Processor;
use databend_common_pipeline::core::ProcessorPtr;
use futures::StreamExt;
use paimon::CatalogFactory;
use paimon::Options;
use paimon::catalog::Catalog;
use paimon::table::ArrowRecordBatchStream;

use crate::PaimonPartInfo;
use crate::error::map_paimon_result;
use crate::predicate::apply_pushdowns;
use crate::predicate::can_push_limit;
use crate::table::PaimonTableDescriptor;
use crate::table::catalog_options_from_plan;
use crate::table::paimon_schema_to_databend;
use crate::table::parse_descriptor_from_plan;

static TABLE_LOAD_COUNT: AtomicUsize = AtomicUsize::new(0);

/// Count of `catalog.get_table` calls per source instance (for integration tests).
#[doc(hidden)]
pub fn table_load_count_for_test() -> usize {
    TABLE_LOAD_COUNT.load(Ordering::SeqCst)
}

#[doc(hidden)]
pub fn reset_table_load_count_for_test() {
    TABLE_LOAD_COUNT.store(0, Ordering::SeqCst);
}

pub struct PaimonTableSource {
    output: Arc<OutputPort>,
    ctx: Arc<dyn TableContext>,
    plan: DataSourcePlan,
    descriptor: PaimonTableDescriptor,
    catalog_options: std::collections::HashMap<String, String>,
    catalog: Option<Arc<dyn Catalog>>,
    readable_table: Option<paimon::Table>,
    output_schema: DataSchemaRef,
    scan_progress: Arc<Progress>,
    stream: Option<ArrowRecordBatchStream>,
    remaining_limit: Option<Option<usize>>,
    is_finished: bool,
}

impl PaimonTableSource {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        output: Arc<OutputPort>,
        plan: DataSourcePlan,
    ) -> Result<ProcessorPtr> {
        let descriptor = parse_descriptor_from_plan(&plan)?;
        let catalog_options = catalog_options_from_plan(&plan)?;
        let output_schema: DataSchemaRef = if let Some(projection) = plan
            .push_downs
            .as_ref()
            .and_then(|push_downs| push_downs.projection.as_ref())
        {
            Arc::new(DataSchema::from(
                &projection.project_schema(plan.source_info.schema().as_ref()),
            ))
        } else {
            Arc::new(DataSchema::from(plan.source_info.schema().as_ref()))
        };
        let scan_progress = ctx.get_scan_progress();
        Ok(ProcessorPtr::create(Box::new(Self {
            output,
            ctx,
            plan,
            descriptor,
            catalog_options,
            catalog: None,
            readable_table: None,
            output_schema,
            scan_progress,
            stream: None,
            remaining_limit: None,
            is_finished: false,
        })))
    }

    fn apply_remaining_limit(&mut self, mut block: DataBlock) -> DataBlock {
        let Some(remaining) = self
            .remaining_limit
            .as_mut()
            .and_then(|remaining| remaining.as_mut())
        else {
            return block;
        };
        if *remaining == 0 {
            block = DataBlock::new(vec![], 0);
            return block;
        }
        if block.num_rows() > *remaining {
            block = block.slice(0..*remaining);
        }
        let emitted = block.num_rows();
        *remaining = remaining.saturating_sub(emitted);
        block
    }

    async fn ensure_readable_table(&mut self) -> Result<&paimon::Table> {
        if self.readable_table.is_none() {
            if self.catalog.is_none() {
                let mut paimon_options = Options::new();
                for (key, value) in &self.catalog_options {
                    paimon_options.set(key, value.clone());
                }
                self.catalog = Some(map_paimon_result(
                    CatalogFactory::create(paimon_options).await,
                )?);
            }
            let catalog = self.catalog.as_ref().expect("paimon catalog must exist");
            TABLE_LOAD_COUNT.fetch_add(1, Ordering::SeqCst);
            let loaded = map_paimon_result(catalog.get_table(&self.descriptor.identifier).await)?;
            self.readable_table = Some(paimon::Table::new(
                loaded.file_io().clone(),
                loaded.identifier().clone(),
                loaded.location().to_string(),
                loaded.schema().clone(),
                loaded.rest_env().cloned(),
            ));
        }
        Ok(self
            .readable_table
            .as_ref()
            .expect("readable table must exist"))
    }

    async fn next_part_stream(&mut self) -> Result<Option<ArrowRecordBatchStream>> {
        let part = match self.ctx.get_partition() {
            Some(part) => part,
            None => return Ok(None),
        };
        let part = part
            .as_any()
            .downcast_ref::<PaimonPartInfo>()
            .ok_or_else(|| ErrorCode::Internal("invalid paimon partition type".to_string()))?;
        let push_downs = self.plan.push_downs.clone();
        let table = self.ensure_readable_table().await?.clone();
        let databend_schema = paimon_schema_to_databend(table.schema())?;
        let (read_builder, analysis) =
            apply_pushdowns(&table, push_downs.as_ref(), &databend_schema);
        if self.remaining_limit.is_none() {
            let remaining_limit = if can_push_limit(
                push_downs.as_ref().and_then(|push_downs| push_downs.limit),
                &analysis,
                &read_builder,
            ) {
                push_downs.as_ref().and_then(|push_downs| push_downs.limit)
            } else {
                None
            };
            self.remaining_limit = Some(remaining_limit);
        }
        if matches!(self.remaining_limit, Some(Some(0))) {
            return Ok(None);
        }
        let split = part.split.clone().try_into()?;
        let table_read = map_paimon_result(read_builder.new_read())?;
        let stream = map_paimon_result(table_read.to_arrow(&[split]))?;
        Ok(Some(stream))
    }
}

#[async_trait::async_trait]
impl Processor for PaimonTableSource {
    fn name(&self) -> String {
        "PaimonTableSource".to_string()
    }

    fn as_any(&mut self) -> &mut dyn Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        if self.is_finished {
            self.output.finish();
            return Ok(Event::Finished);
        }
        if self.output.is_finished() {
            return Ok(Event::Finished);
        }
        if !self.output.can_push() {
            return Ok(Event::NeedConsume);
        }
        if self.stream.is_none() {
            return Ok(Event::Async);
        }
        Ok(Event::Async)
    }

    fn process(&mut self) -> Result<()> {
        Ok(())
    }

    #[async_backtrace::framed]
    async fn async_process(&mut self) -> Result<()> {
        if matches!(self.remaining_limit, Some(Some(0))) {
            self.is_finished = true;
            return Ok(());
        }

        if self.stream.is_none() {
            self.stream = self.next_part_stream().await?;
            if self.stream.is_none() {
                self.is_finished = true;
                return Ok(());
            }
        }

        let stream = self.stream.as_mut().expect("stream must exist");
        match stream.next().await {
            Some(Ok(batch)) => {
                let mut block = if batch.num_columns() == 0 {
                    DataBlock::new(vec![], batch.num_rows())
                } else {
                    DataBlock::from_record_batch(self.output_schema.as_ref(), &batch)?
                };
                block = self.apply_remaining_limit(block);
                if block.is_empty() && matches!(self.remaining_limit, Some(Some(0))) {
                    self.stream = None;
                    self.is_finished = true;
                    return Ok(());
                }
                self.scan_progress.incr(&ProgressValues {
                    rows: block.num_rows(),
                    bytes: block.memory_size(),
                });
                self.output.push_data(Ok(block));
                if matches!(self.remaining_limit, Some(Some(0))) {
                    self.stream = None;
                    self.is_finished = true;
                }
                Ok(())
            }
            Some(Err(err)) => Err(ErrorCode::ReadTableDataError(format!(
                "read paimon batch failed: {err:?}"
            ))),
            None => {
                self.stream = None;
                Ok(())
            }
        }
    }
}
