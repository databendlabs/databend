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

use std::sync::Arc;

use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_catalog::plan::PartInfoPtr;
use databend_common_catalog::runtime_filter_info::IndexRuntimeFilters;
use databend_common_catalog::runtime_filter_info::RuntimeFilterSource;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfoDowncast;
use databend_common_expression::DataBlock;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::OutputPort;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline_transforms::processors::AsyncTransform;
use databend_common_pipeline_transforms::processors::AsyncTransformer;
use databend_common_sql::IndexType;

use super::read_block_context::ReadBlockContext;
use crate::io::BlockReader;
use crate::operations::read::block_partition_meta::BlockPartitionMeta;
use crate::operations::read::data_source_with_meta::DataSourceWithMeta;

pub struct ReadDataTransform {
    block_reader: Arc<BlockReader>,
    read_block_context: Arc<ReadBlockContext>,
    rf_source: Option<Arc<RuntimeFilterSource>>,
}

impl ReadDataTransform {
    #[allow(clippy::too_many_arguments)]
    pub fn create(
        _scan_id: IndexType,
        _ctx: Arc<dyn databend_common_catalog::table_context::TableContext>,
        _table_schema: Arc<databend_common_expression::TableSchema>,
        block_reader: Arc<BlockReader>,
        read_block_context: Arc<ReadBlockContext>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        rf_source: Option<Arc<RuntimeFilterSource>>,
    ) -> Result<ProcessorPtr> {
        Ok(ProcessorPtr::create(AsyncTransformer::create(
            input,
            output,
            ReadDataTransform {
                block_reader,
                read_block_context,
                rf_source,
            },
        )))
    }

    async fn read_parts(&self, parts: Vec<PartInfoPtr>) -> Result<DataBlock> {
        let mut read_tasks = Vec::with_capacity(parts.len());
        let mut parts_to_read = Vec::with_capacity(parts.len());

        // Read index filters from the runtime filter source (built by build side)
        let index_filters: IndexRuntimeFilters = self
            .rf_source
            .as_ref()
            .map(|s| s.get_index_filters())
            .unwrap_or_default();

        let operator = self.block_reader.operator();

        'next_part: for part in parts {
            for filter in &index_filters {
                let index = filter.load_index(&part, &operator).await?;
                let index_ref = index.as_ref().map(|b| b.as_ref() as &dyn std::any::Any);
                if filter.prune(&part, index_ref)? {
                    Profile::record_usize_profile(
                        ProfileStatisticsName::RuntimeFilterPruneParts,
                        1,
                    );
                    continue 'next_part;
                }
            }

            parts_to_read.push(part.clone());
            let read_block_context = self.read_block_context.clone();

            read_tasks.push(async move {
                databend_common_base::runtime::spawn(async move {
                    read_block_context.read_data(part).await
                })
                .await
                .unwrap()
            });
        }

        Ok(DataBlock::empty_with_meta(DataSourceWithMeta::create(
            parts_to_read,
            futures::future::try_join_all(read_tasks).await?,
        )))
    }
}

#[async_trait::async_trait]
impl AsyncTransform for ReadDataTransform {
    const NAME: &'static str = "AsyncReadDataTransform";

    async fn transform(&mut self, data: DataBlock) -> Result<DataBlock> {
        let parts = data
            .get_meta()
            .and_then(BlockPartitionMeta::downcast_ref_from)
            .and_then(|meta| (!meta.part_ptr.is_empty()).then(|| meta.part_ptr.clone()))
            .ok_or_else(|| ErrorCode::Internal("AsyncReadDataTransform got wrong meta data"))?;

        self.read_parts(parts).await
    }
}
