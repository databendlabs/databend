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

use async_trait::async_trait;
use databend_common_meta_types::SeqV;
use databend_common_meta_types::errors;
use futures_util::StreamExt;
use futures_util::TryStreamExt;
use log::debug;

use crate::kvapi::KVApi;

/// Extend the `KVApi` trait with auto implemented handy methods.
#[async_trait]
pub trait KvApiExt: KVApi {
    /// Get single key-value record by key.
    async fn get_kv(&self, key: &str) -> Result<Option<SeqV>, Self::Error> {
        let mut strm = self.get_kv_stream(&[key.to_string()]).await?;

        let strm_item = strm
            .next()
            .await
            .ok_or_else(|| errors::IncompleteStream::new(1, 0).context(" while get_kv"))??;

        let reply = strm_item.value.map(SeqV::from);

        Ok(reply)
    }

    /// Get several key-values by keys.
    async fn mget_kv(&self, keys: &[String]) -> Result<Vec<Option<SeqV>>, Self::Error> {
        let n = keys.len();
        let strm = self.get_kv_stream(keys).await?;

        let seq_values: Vec<Option<SeqV>> = strm
            .map_ok(|item| item.value.map(SeqV::from))
            .try_collect()
            .await?;

        if seq_values.len() != n {
            return Err(
                errors::IncompleteStream::new(n as u64, seq_values.len() as u64)
                    .context(" while mget_kv")
                    .into(),
            );
        }

        debug!("mget: keys: {:?}; values: {:?}", keys, seq_values);

        Ok(seq_values)
    }

    /// List key-value starting with the specified prefix and return a [`Vec`]
    ///
    /// Same as [`Self::list_kv`] but return a [`Vec`] instead of a stream.
    async fn list_kv_collect(
        &self,
        prefix: &str,
        limit: Option<u64>,
    ) -> Result<Vec<(String, SeqV)>, Self::Error> {
        let now = std::time::Instant::now();

        let strm = self.list_kv(prefix, limit).await?;

        debug!("list_kv() took {:?}", now.elapsed());

        let key_seqv_list = strm
            .map_ok(|stream_item| {
                // Safe unwrap(): list_kv() does not return None value
                (stream_item.key, SeqV::from(stream_item.value.unwrap()))
            })
            .try_collect::<Vec<_>>()
            .await?;

        Ok(key_seqv_list)
    }
}

impl<T: KVApi + ?Sized> KvApiExt for T {}
