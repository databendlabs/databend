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

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::DataSchema;
use databend_common_meta_store::MetaStore;
use databend_common_storage::DataOperator;
use opendal::Operator;
use parquet::arrow::arrow_reader::ParquetRecordBatchReader;

use crate::result_cache::common::gen_result_cache_meta_key;
use crate::result_cache::common::ResultCacheValue;
use crate::result_cache::meta_manager::ResultCacheMetaManager;

pub struct ResultCacheReader {
    meta_mgr: ResultCacheMetaManager,
    meta_key: String,

    operator: Operator,
    /// To ensure the cache is valid.
    partitions_shas: Vec<String>,

    /// If true, the cache will be used even if it is inconsistent.
    /// In another word, `partitions_sha` will not be checked.
    tolerate_inconsistent: bool,
}

impl ResultCacheReader {
    pub fn create(
        ctx: Arc<dyn TableContext>,
        key: &str,
        kv_store: Arc<MetaStore>,
        tolerate_inconsistent: bool,
    ) -> Self {
        let tenant = ctx.get_tenant();
        let meta_key = gen_result_cache_meta_key(tenant.tenant_name(), key);
        let partitions_shas = ctx.get_partitions_shas();

        Self {
            meta_mgr: ResultCacheMetaManager::create(kv_store, 0),
            meta_key,
            partitions_shas,
            operator: DataOperator::instance().operator(),
            tolerate_inconsistent,
        }
    }

    pub fn get_meta_key(&self) -> String {
        self.meta_key.clone()
    }

    #[async_backtrace::framed]
    pub async fn check_cache(&self) -> Result<Option<ResultCacheValue>> {
        if let Some(v) = self.meta_mgr.get(self.meta_key.clone()).await? {
            if self.tolerate_inconsistent || v.partitions_shas == self.partitions_shas {
                return Ok(Some(v));
            }
        }
        Ok(None)
    }

    #[async_backtrace::framed]
    pub async fn try_read_cached_result(&self) -> Result<Option<Vec<DataBlock>>> {
        self.try_read_cached_result_with_meta_key(self.meta_key.clone())
            .await
    }

    #[async_backtrace::framed]
    pub async fn try_read_cached_result_with_meta_key(
        &self,
        meta_key: String,
    ) -> Result<Option<Vec<DataBlock>>> {
        match self.meta_mgr.get(meta_key).await? {
            Some(value) => {
                if self.tolerate_inconsistent || value.partitions_shas == self.partitions_shas {
                    if value.num_rows == 0 {
                        Ok(Some(vec![DataBlock::empty()]))
                    } else {
                        Ok(Some(self.read_result_from_cache(&value.location).await?))
                    }
                } else {
                    // The cache is invalid (due to data update or other reasons).
                    Ok(None)
                }
            }
            None => Ok(None),
        }
    }

    #[async_backtrace::framed]
    async fn read_result_from_cache(&self, location: &str) -> Result<Vec<DataBlock>> {
        let data = self.operator.read(location).await?;
        // TODO: improve this part by implement ChunkReader for opendal::Buffer.
        let chunk_reader = data.to_bytes();
        let reader = ParquetRecordBatchReader::try_new(chunk_reader, usize::MAX)?;
        let mut blocks = Vec::with_capacity(1);

        for record_batch in reader {
            let record_batch = record_batch?;
            let schema = DataSchema::try_from(record_batch.schema().as_ref())?;
            let (block, _) = DataBlock::from_record_batch(&schema, &record_batch)?;
            blocks.push(block);
        }

        Ok(blocks)
    }
}
