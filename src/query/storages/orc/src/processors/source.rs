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

use std::mem;
use std::sync::Arc;

use databend_common_base::base::Progress;
use databend_common_base::base::ProgressValues;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_catalog::plan::Projection;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline_core::processors::OutputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_sources::AsyncSource;
use databend_common_pipeline_sources::AsyncSourcer;
use databend_common_storage::OperatorRegistry;
use databend_storages_common_stage::SingleFilePartition;
use orc_rust::async_arrow_reader::StripeFactory;
use orc_rust::projection::ProjectionMask;
use orc_rust::ArrowReaderBuilder;

use crate::chunk_reader_impl::OrcChunkReader;
use crate::strip::StripeInMemory;
use crate::utils::map_orc_error;

pub struct ORCSource {
    table_ctx: Arc<dyn TableContext>,
    op_registry: Arc<dyn OperatorRegistry>,
    pub(crate) reader: Option<(String, Box<StripeFactory<OrcChunkReader>>, usize)>,
    scan_progress: Arc<Progress>,

    arrow_schema: arrow_schema::SchemaRef,
    schema_from: Option<String>,
    projection: Projection,
}

impl ORCSource {
    pub fn try_create(
        output: Arc<OutputPort>,
        table_ctx: Arc<dyn TableContext>,
        op_registry: Arc<dyn OperatorRegistry>,
        arrow_schema: arrow_schema::SchemaRef,
        schema_from: Option<String>,
        projection: Projection,
    ) -> Result<ProcessorPtr> {
        let scan_progress = table_ctx.get_scan_progress();

        AsyncSourcer::create(table_ctx.clone(), output, ORCSource {
            table_ctx,
            op_registry,
            scan_progress,
            reader: None,
            arrow_schema,
            schema_from,
            projection,
        })
    }

    fn check_file_schema(&self, arrow_schema: arrow_schema::SchemaRef, path: &str) -> Result<()> {
        if self.arrow_schema.fields != arrow_schema.fields {
            return Err(ErrorCode::TableSchemaMismatch(format!(
                "{}get diff schema in file '{}'. Expected schema: {:?}, actual: {:?}",
                self.schema_from
                    .as_ref()
                    .map_or(String::new(), |schema_from| {
                        format!("infer schema from '{}', but ", schema_from)
                    }),
                path,
                self.arrow_schema,
                arrow_schema
            )));
        }
        Ok(())
    }

    async fn next_part(&mut self) -> Result<bool> {
        let part = match self.table_ctx.get_partition() {
            Some(part) => part,
            None => return Ok(false),
        };
        let file = SingleFilePartition::from_part(&part)?.clone();
        let size = file.size;

        let (operator, path) = self.op_registry.get_operator_path(&file.path)?;
        let file = OrcChunkReader {
            operator,
            size: file.size as u64,
            path: path.to_string(),
        };
        let builder = ArrowReaderBuilder::try_new_async(file)
            .await
            .map_err(|e| map_orc_error(e, path))?;
        let projection = if let Projection::Columns(projection) = &self.projection {
            ProjectionMask::roots(
                builder.file_metadata().root_data_type(),
                projection.iter().map(|index| index + 1),
            )
        } else {
            ProjectionMask::all()
        };
        let reader = builder.with_projection(projection).build_async();
        let (factory, schema) = reader.into_parts();
        let factory = factory.unwrap();
        self.check_file_schema(schema, path)?;

        self.reader = Some((path.to_string(), factory, size));
        Ok(true)
    }
}

#[async_trait::async_trait]
impl AsyncSource for ORCSource {
    const NAME: &'static str = "ORCSource";
    const SKIP_EMPTY_DATA_BLOCK: bool = false;

    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        loop {
            if self.reader.is_none() && !self.next_part().await? {
                return Ok(None);
            }
            if let Some((path, factory, size)) = mem::take(&mut self.reader) {
                let (factory, stripe) = factory
                    .read_next_stripe()
                    .await
                    .map_err(|e| ErrorCode::StorageOther(e.to_string()))?;
                match stripe {
                    None => {
                        self.reader = None;
                        let progress_values = ProgressValues {
                            rows: 0,
                            bytes: size,
                        };
                        self.scan_progress.incr(&progress_values);
                        Profile::record_usize_profile(ProfileStatisticsName::ScanBytes, size);
                        Profile::record_usize_profile(ProfileStatisticsName::ScanPartitions, 1);
                        continue;
                    }
                    Some(stripe) => {
                        let progress_values = ProgressValues {
                            rows: stripe.number_of_rows(),
                            bytes: 0,
                        };
                        self.scan_progress.incr(&progress_values);
                        self.reader = Some((path.clone(), Box::new(factory), size));
                        let meta = Box::new(StripeInMemory {
                            path,
                            stripe,
                            schema: None,
                        });
                        return Ok(Some(DataBlock::empty_with_meta(meta)));
                    }
                }
            } else {
                return Err(ErrorCode::Internal(
                    "Bug: ORCSource: should not be called with reader != None.",
                ));
            }
        }
    }
}
