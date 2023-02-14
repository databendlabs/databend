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

use common_exception::Result;
use common_meta_kvapi::kvapi;
use common_meta_kvapi::kvapi::UpsertKVReq;
use common_meta_types::KVAppError;
use common_meta_types::KVMeta;
use common_meta_types::MatchSeq;
use common_meta_types::Operation;
use common_meta_types::SeqV;
use common_pipeline_core::processors::port::InputPort;
use common_pipeline_core::processors::port::OutputPort;
use common_pipeline_core::processors::processor::Event;
use common_pipeline_core::processors::processor::ProcessorPtr;
use common_pipeline_core::processors::Processor;
use common_storage::DataOperator;
use common_users::UserApiProvider;

use super::cache_writer::ResultCacheWriter;
use crate::common::ResultCacheValue;

pub struct TransformWriteResultCache {
    input: Arc<InputPort>,
    output: Arc<OutputPort>,
    called_on_finish: bool,

    sql: String,
    key: String,
    ttl: u64,

    kvapi: Arc<dyn kvapi::KVApi<Error = KVAppError>>,
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
        let expire_at = now + self.ttl;
        let value = ResultCacheValue {
            sql: self.sql.clone(),
            query_time: now,
            ttl: self.ttl,
            result_size: self.cache_writer.current_bytes(),
            num_rows: self.cache_writer.num_rows(),
            location,
        };
        let value = serde_json::to_vec(&value)?;
        let _ = self
            .kvapi
            .upsert_kv(UpsertKVReq {
                key: self.key.clone(),
                seq: MatchSeq::GE(0),
                value: Operation::Update(value),
                value_meta: Some(KVMeta {
                    expire_at: Some(expire_at),
                }),
            })
            .await?;

        // 3. Finish
        self.called_on_finish = true;

        Ok(())
    }
}

impl TransformWriteResultCache {
    pub fn create(
        input: Arc<InputPort>,
        output: Arc<OutputPort>,
        sql: String,
        key: String,
        ttl: u64,
        max_bytes: usize,
    ) -> ProcessorPtr {
        let kvapi = UserApiProvider::instance().get_meta_store_client();
        let operator = DataOperator::instance().operator();
        let cache_writer = ResultCacheWriter::create(operator, max_bytes);

        ProcessorPtr::create(Box::new(TransformWriteResultCache {
            input,
            output,
            called_on_finish: false,
            sql,
            key,
            ttl,
            kvapi,
            cache_writer,
        }))
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
