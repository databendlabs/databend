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
use common_meta_store::MetaStore;
use common_meta_types::MatchSeq;
use common_meta_types::SeqV;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;
use common_storage::DataOperator;

use super::writer::ResultCacheWriter;
use crate::common::gen_result_cache_meta_key;
use crate::common::ResultCacheValue;
use crate::meta_manager::ResultCacheMetaManager;

pub struct TransformWriteResultCache {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    called_on_finish: bool,

    sql: String,
    partitions_sha: String,

    meta_mgr: ResultCacheMetaManager,
    cache_writer: ResultCacheWriter,
}

// The logic is similar to `Transformers`, but use async to handle `on_finish`.
#[async_trait::async_trait]
impl Processor for TransformWriteResultCache {
    fn name(&self) -> String {
        "TransformWriteResultCache".to_string()
    }

    fn as_any(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn event(&mut self) -> Result<Event> {
        match self.output.is_finished() {
            true => self.finish_input(),
            false if !self.output.can_push() => self.not_need_data(),
            false => self.pull_data(),
        }
    }

    async fn async_process(&mut self) -> Result<()> {
        // 1. Write the result cache to the storage.
        let location = self.cache_writer.write_to_storage().await?;

        // 2. Set result calue key-value pair to meta.
        let now = SeqV::<()>::now_ms();
        let ttl = self.meta_mgr.get_ttl();
        let expire_at = now + ttl;

        let value = ResultCacheValue {
            sql: self.sql.clone(),
            query_time: now,
            ttl,
            partitions_sha: self.partitions_sha.clone(),
            result_size: self.cache_writer.current_bytes(),
            num_rows: self.cache_writer.num_rows(),
            location,
        };
        self.meta_mgr.set(value, MatchSeq::GE(0), expire_at).await?;

        // 3. Finish
        self.called_on_finish = true;

        Ok(())
    }
}

impl TransformWriteResultCache {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        kv_store: Arc<MetaStore>,
    ) -> Result<ProcessorPtr> {
        let settings = ctx.get_settings();
        let max_bytes = settings.get_max_result_cache_bytes()?;
        let ttl = settings.get_result_cache_ttl()?;
        let tenant = ctx.get_tenant();
        let sql = ctx.get_query_str();
        let key = gen_result_cache_meta_key(&tenant, &sql);
        let partitions_sha = ctx.get_partitions_sha().unwrap();

        let operator = DataOperator::instance().operator();
        let cache_writer = ResultCacheWriter::create(operator, max_bytes);

        Ok(ProcessorPtr::create(Box::new(TransformWriteResultCache {
            input,
            output,
            called_on_finish: false,
            sql,
            partitions_sha,
            meta_mgr: ResultCacheMetaManager::create(kv_store, key, ttl),
            cache_writer,
        })))
    }

    fn pull_data(&mut self) -> Result<Event> {
        if self.input.has_data() {
            let data = self.input.pull_data().unwrap()?;
            if !self.cache_writer.over_limit() {
                self.cache_writer.append_block(data.clone());
            }
            self.output.push_data(Ok(data));
            return Ok(Event::NeedConsume);
        }

        if self.input.is_finished() {
            return self.finish_input();
        }

        self.input.set_need_data();
        Ok(Event::NeedData)
    }

    fn not_need_data(&mut self) -> Result<Event> {
        self.input.set_not_need_data();
        Ok(Event::NeedConsume)
    }

    fn finish_input(&mut self) -> Result<Event> {
        match !self.called_on_finish {
            true if !self.cache_writer.over_limit() => Ok(Event::Async),
            _ => {
                self.input.finish();
                Ok(Event::Finished)
            }
        }
    }
}
