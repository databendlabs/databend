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
use databend_common_expression::TableSchemaRef;
use databend_common_meta_store::MetaStore;
use databend_common_meta_types::MatchSeq;
use databend_common_meta_types::SeqV;
use databend_common_pipeline_core::processors::InputPort;
use databend_common_pipeline_core::processors::ProcessorPtr;
use databend_common_pipeline_sinks::AsyncMpscSink;
use databend_common_pipeline_sinks::AsyncMpscSinker;
use databend_common_storage::DataOperator;

use super::writer::ResultCacheWriter;
use crate::common::gen_result_cache_dir;
use crate::common::gen_result_cache_meta_key;
use crate::common::ResultCacheValue;
use crate::meta_manager::ResultCacheMetaManager;

pub struct WriteResultCacheSink {
    ctx: Arc<dyn TableContext>,
    sql: String,
    partitions_shas: Vec<String>,

    meta_mgr: ResultCacheMetaManager,
    meta_key: String,
    cache_writer: ResultCacheWriter,
}

#[async_trait::async_trait]
impl AsyncMpscSink for WriteResultCacheSink {
    const NAME: &'static str = "WriteResultCacheSink";

    #[async_trait::unboxed_simple]
    #[async_backtrace::framed]
    async fn consume(&mut self, block: DataBlock) -> Result<bool> {
        if !self.cache_writer.over_limit() {
            self.cache_writer.append_block(block);
            Ok(false)
        } else {
            // Finish the cache writing pipeline.
            Ok(true)
        }
    }

    #[async_backtrace::framed]
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
            query_id: self.ctx.get_id(),
            query_time: now,
            ttl,
            partitions_shas: self.partitions_shas.clone(),
            result_size: self.cache_writer.current_bytes(),
            num_rows: self.cache_writer.num_rows(),
            location,
        };
        self.meta_mgr
            .set(self.meta_key.clone(), value, MatchSeq::GE(0), expire_at)
            .await?;
        self.ctx
            .set_query_id_result_cache(self.ctx.get_id(), self.meta_key.clone());
        Ok(())
    }
}

impl WriteResultCacheSink {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        key: &str,
        schema: TableSchemaRef,
        inputs: Vec<Arc<InputPort>>,
        kv_store: Arc<MetaStore>,
    ) -> Result<ProcessorPtr> {
        let settings = ctx.get_settings();
        let max_bytes = settings.get_query_result_cache_max_bytes()?;
        let ttl = settings.get_query_result_cache_ttl_secs()?;
        let tenant = ctx.get_tenant();
        let sql = ctx.get_query_str();
        let partitions_shas = ctx.get_partitions_shas();

        let meta_key = gen_result_cache_meta_key(&tenant, key);
        let location = gen_result_cache_dir(key);

        let operator = DataOperator::instance().operator();
        let cache_writer =
            ResultCacheWriter::create(schema, location, operator, max_bytes, ctx.clone());

        Ok(ProcessorPtr::create(Box::new(AsyncMpscSinker::create(
            inputs,
            WriteResultCacheSink {
                ctx,
                sql,
                partitions_shas,
                meta_mgr: ResultCacheMetaManager::create(kv_store, ttl),
                meta_key,
                cache_writer,
            },
        ))))
    }
}
