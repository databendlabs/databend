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
use std::time::Instant;

use databend_common_base::base::Progress;
use databend_common_base::base::ProgressValues;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::sources::AsyncSource;
use databend_common_pipeline::sources::AsyncSourcer;
use databend_storages_common_stage::SingleFilePartition;
use opendal::Operator;
use orc_rust::ArrowReaderBuilder;

use crate::chunk_reader_impl::OrcChunkReader;
use crate::hashable_schema::HashableSchema;
use crate::processors::source::ReadingFile;
use crate::strip::StripeInMemory;
use crate::utils::map_orc_error;

pub struct ORCSourceForCopy {
    table_ctx: Arc<dyn TableContext>,
    scan_progress: Arc<Progress>,
    op: Operator,
    reader: Option<ReadingFile>,
}

impl ORCSourceForCopy {
    pub fn try_create(
        output: Arc<OutputPort>,
        table_ctx: Arc<dyn TableContext>,
        op: Operator,
    ) -> Result<ProcessorPtr> {
        let scan_progress = table_ctx.get_scan_progress();

        AsyncSourcer::create(scan_progress.clone(), output, ORCSourceForCopy {
            table_ctx,
            op,
            scan_progress,
            reader: None,
        })
    }

    async fn next_part(&mut self) -> Result<bool> {
        let part = match self.table_ctx.get_partition() {
            Some(part) => part,
            None => return Ok(false),
        };
        let file = SingleFilePartition::from_part(&part)?.clone();
        let path = file.path.clone();
        let size = file.size;

        let file = OrcChunkReader {
            operator: self.op.clone(),
            size: file.size as u64,
            path: file.path,
        };
        let builder = ArrowReaderBuilder::try_new_async(file)
            .await
            .map_err(|e| map_orc_error(e, &path))?;
        let reader = builder.build_async();
        let (factory, schema) = reader.into_parts();
        let stripe_factory = factory.expect("factory must has been created");
        let schema = HashableSchema::try_create(schema)?;

        self.reader = Some(ReadingFile {
            path: path.to_string(),
            stripe_factory,
            size,
            schema: Some(schema),
            rows: 0,
        });
        Ok(true)
    }
}

#[async_trait::async_trait]
impl AsyncSource for ORCSourceForCopy {
    const NAME: &'static str = "ORCSourceForCopy";
    const SKIP_EMPTY_DATA_BLOCK: bool = false;

    #[async_backtrace::framed]
    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        loop {
            if self.reader.is_none() && !self.next_part().await? {
                return Ok(None);
            }
            let start = Instant::now();
            if let Some(file) = mem::take(&mut self.reader) {
                let (factory, stripe) = file
                    .stripe_factory
                    .read_next_stripe()
                    .await
                    .map_err(|e| ErrorCode::StorageOther(e.to_string()))?;
                match stripe {
                    None => {
                        Profile::record_usize_profile(ProfileStatisticsName::ScanPartitions, 1);
                        self.reader = None;
                        continue;
                    }
                    Some(stripe) => {
                        let used = start.elapsed().as_secs_f32();
                        let rows = stripe.number_of_rows();

                        let bytes = stripe.stream_map().inner.values().map(|b| b.len()).sum();
                        let progress_values = ProgressValues { rows, bytes };
                        Profile::record_usize_profile(ProfileStatisticsName::ScanBytes, bytes);
                        log::info!(
                            "read new stripe of {} rows and {bytes} bytes from {}, use {} secs",
                            stripe.number_of_rows(),
                            file.path,
                            used
                        );
                        self.scan_progress.incr(&progress_values);

                        self.reader = Some(ReadingFile {
                            path: file.path.clone(),
                            stripe_factory: Box::new(factory),
                            size: file.size,
                            schema: file.schema.clone(),
                            rows: (rows as u64) + file.rows,
                        });
                        let meta = Box::new(StripeInMemory {
                            path: file.path.clone(),
                            stripe,
                            schema: file.schema,
                            start_row: file.rows,
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
