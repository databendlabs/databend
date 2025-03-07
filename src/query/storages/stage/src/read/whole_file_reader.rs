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

use databend_common_base::base::ProgressValues;
use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::BlockMetaInfo;
use databend_common_expression::DataBlock;
use databend_common_pipeline_sources::PrefetchAsyncSource;
use databend_storages_common_stage::SingleFilePartition;
use log::debug;
use opendal::Operator;
use serde::Deserialize;
use serde::Serialize;

#[derive(Serialize, Deserialize, Debug)]
pub struct WholeFileData {
    pub data: Vec<u8>,
    pub path: String,
}

#[typetag::serde(name = "avro_file_data")]
impl BlockMetaInfo for WholeFileData {}

pub struct WholeFileReader {
    table_ctx: Arc<dyn TableContext>,
    op: Operator,
    io_size: usize,
    prefetch_num: usize,
}

impl WholeFileReader {
    pub fn try_create(table_ctx: Arc<dyn TableContext>, op: Operator) -> Result<Self> {
        // TODO: Use 8MiB as default IO size for now, we can extract as a new config.
        let io_size: usize = 8 * 1024 * 1024;
        let prefetch_num = 1;

        Ok(Self {
            table_ctx,
            op,
            io_size,
            prefetch_num,
        })
    }
}

#[async_trait::async_trait]
impl PrefetchAsyncSource for WholeFileReader {
    const NAME: &'static str = "BytesReader";

    const SKIP_EMPTY_DATA_BLOCK: bool = false;

    fn is_full(&self, prefetched: &[DataBlock]) -> bool {
        prefetched.len() >= self.prefetch_num
    }

    async fn generate(&mut self) -> Result<Option<DataBlock>> {
        let part = match self.table_ctx.get_partition() {
            Some(part) => part,
            None => return Ok(None),
        };
        let file = SingleFilePartition::from_part(&part)?.clone();

        let buffer = self
            .op
            .reader_with(&file.path)
            .chunk(self.io_size)
            // TODO: Use 4 concurrent for test, let's extract as a new setting.
            .concurrent(4)
            .await?
            .read(0..file.size as u64)
            .await?;

        let data = buffer.to_vec();
        Profile::record_usize_profile(ProfileStatisticsName::ScanBytes, data.len());
        debug!("read {} bytes from {}", file.size, file.path);
        self.table_ctx.get_scan_progress().incr(&ProgressValues {
            rows: 0,
            bytes: file.size,
        });

        let meta = std::boxed::Box::new(WholeFileData {
            data,
            path: file.path.clone(),
        });
        Ok(Some(DataBlock::empty_with_meta(meta)))
    }
}
