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
use std::time::Duration;
use std::time::SystemTime;

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_common_expression::TableSchemaRef;
use databend_common_meta_store::MetaStore;
use databend_common_pipeline::core::InputPort;
use databend_common_pipeline::core::ProcessorPtr;
use databend_common_pipeline::sinks::AsyncMpscSink;
use databend_common_pipeline::sinks::AsyncMpscSinker;
use databend_common_storage::DataOperator;
use databend_meta_types::MatchSeq;
use tokio::time::Instant;

use super::writer::ResultCacheWriter;
use crate::result_cache::common::ResultCacheValue;
use crate::result_cache::common::gen_result_cache_dir;
use crate::result_cache::common::gen_result_cache_meta_key;
use crate::result_cache::meta_manager::ResultCacheMetaManager;

pub struct WriteResultCacheSink {
    ctx: Arc<dyn TableContext>,
    sql: String,
    partitions_shas: Vec<String>,

    meta_mgr: ResultCacheMetaManager,
    meta_key: String,
    cache_writer: ResultCacheWriter,

    // The time when the sink is created.
    create_time: Instant,

    // A flag indicates at least one block has been consumed.
    consumed_one_block: bool,
    terminated: bool,
}

#[async_trait::async_trait]
impl AsyncMpscSink for WriteResultCacheSink {
    const NAME: &'static str = "WriteResultCacheSink";

    #[async_backtrace::framed]
    async fn consume(&mut self, block: DataBlock) -> Result<bool> {
        if self.terminated {
            return Ok(true);
        }

        if !self.consumed_one_block {
            if self.cache_writer.not_over_time(&self.create_time) {
                // Skip the cache writing if the query execution time is less than the min_execute_secs.
                self.terminated = true;
                return Ok(true);
            }
            self.consumed_one_block = true;
        }

        if !self.cache_writer.over_limit() {
            self.cache_writer.append_block(block);
            Ok(false)
        } else {
            self.terminated = true;
            // Finish the cache writing pipeline.
            Ok(true)
        }
    }

    #[async_backtrace::framed]
    async fn on_finish(&mut self) -> Result<()> {
        if self.terminated || self.cache_writer.num_rows() == 0 {
            return Ok(());
        }

        // Skip cache writing if there are no cache invalidation keys.
        // Without invalidation keys, we cannot detect data changes and the cache
        // may return stale results. This can happen when queries are pure constants
        // (like SELECT 1) that don't depend on any table data.
        // Note: Queries with DummyTableScan that depend on table statistics should
        // have invalidation keys added via DummyTableScan.source_table_indexes.
        if self.partitions_shas.is_empty() {
            return Ok(());
        }

        // 1. Write the result cache to the storage, blocks must be !empty.
        let location = self.cache_writer.write_to_storage().await?;

        // 2. Set result cache key-value pair to meta.
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let ttl_sec = self.meta_mgr.get_ttl();
        let ttl_interval = Duration::from_secs(ttl_sec);

        let value = ResultCacheValue {
            sql: self.sql.clone(),
            query_id: self.ctx.get_id(),
            query_time: now,
            ttl: ttl_sec,
            partitions_shas: self.partitions_shas.clone(),
            result_size: self.cache_writer.current_bytes(),
            num_rows: self.cache_writer.num_rows(),
            location,
            cache_key_extras: self.ctx.get_cache_key_extras(),
        };
        self.meta_mgr
            .set(self.meta_key.clone(), value, MatchSeq::GE(0), ttl_interval)
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
        let min_execute_secs = settings.get_query_result_cache_min_execute_secs()?;
        let ttl = settings.get_query_result_cache_ttl_secs()?;
        let tenant = ctx.get_tenant();
        let sql = ctx.get_query_str();
        let partitions_shas = ctx.get_partitions_shas();

        let meta_key = gen_result_cache_meta_key(tenant.tenant_name(), key);
        let location = gen_result_cache_dir(key);

        let operator = DataOperator::instance().operator();
        let cache_writer =
            ResultCacheWriter::create(schema, location, operator, max_bytes, min_execute_secs);

        Ok(ProcessorPtr::create(AsyncMpscSinker::create(
            inputs,
            WriteResultCacheSink {
                ctx,
                sql,
                partitions_shas,
                meta_mgr: ResultCacheMetaManager::create(kv_store, ttl),
                meta_key,
                cache_writer,
                create_time: Instant::now(),
                consumed_one_block: false,
                terminated: false,
            },
        )))
    }
}
