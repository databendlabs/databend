// Copyright 2023 Datafuse Labs.
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

use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::DataBlock;
use common_meta_store::MetaStore;
use common_meta_types::MatchSeq;
use common_meta_types::SeqV;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_sinks::AsyncMpscSink;
use common_pipeline_sinks::AsyncMpscSinker;
use common_storage::DataOperator;

use super::writer::ResultCacheWriter;
use crate::common::gen_result_cache_dir;
use crate::common::gen_result_cache_meta_key;
use crate::common::ResultCacheValue;
use crate::meta_manager::ResultCacheMetaManager;

pub struct WriteResultCacheSink {
    sql: String,
    partitions_shas: Vec<String>,

    meta_mgr: ResultCacheMetaManager,
    cache_writer: ResultCacheWriter,
}

#[async_trait::async_trait]
impl AsyncMpscSink for WriteResultCacheSink {
    const NAME: &'static str = "WriteResultCacheSink";

    #[async_trait::unboxed_simple]
    async fn consume(&mut self, block: DataBlock) -> Result<bool> {
        if !self.cache_writer.over_limit() {
            self.cache_writer.append_block(block);
            Ok(false)
        } else {
            // Finish the cache writing pipeline.
            Ok(true)
        }
    }

    async fn on_finish(&mut self) -> Result<()> {
        if self.cache_writer.over_limit() {
            return Ok(());
        }

        // 1. Write the result cache to the storage.
        let location = self.cache_writer.write_to_storage().await?;

        // 2. Set result cache key-value pair to meta.
        let now = SeqV::<()>::now_ms() / 1000;
        let ttl = self.meta_mgr.get_ttl();
        let expire_at = now + ttl;

        let value = ResultCacheValue {
            sql: self.sql.clone(),
            query_time: now,
            ttl,
            partitions_shas: self.partitions_shas.clone(),
            result_size: self.cache_writer.current_bytes(),
            num_rows: self.cache_writer.num_rows(),
            location,
        };
        self.meta_mgr.set(value, MatchSeq::GE(0), expire_at).await?;
        Ok(())
    }
}

impl WriteResultCacheSink {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        key: &str,
        inputs: Vec<Arc<InputPort>>,
        kv_store: Arc<MetaStore>,
    ) -> Result<ProcessorPtr> {
        let settings = ctx.get_settings();
        let max_bytes = settings.get_max_result_cache_bytes()?;
        let ttl = settings.get_result_cache_ttl()?;
        let tenant = ctx.get_tenant();
        let sql = ctx.get_query_str();
        let partitions_shas = ctx.get_partitions_shas();

        let meta_key = gen_result_cache_meta_key(&tenant, key);
        let location = gen_result_cache_dir(key);

        let operator = DataOperator::instance().operator();
        let cache_writer = ResultCacheWriter::create(location, operator, max_bytes);

        Ok(ProcessorPtr::create(Box::new(AsyncMpscSinker::create(
            inputs,
            WriteResultCacheSink {
                sql,
                partitions_shas,
                meta_mgr: ResultCacheMetaManager::create(kv_store, meta_key, ttl),
                cache_writer,
            },
        ))))
    }
}
