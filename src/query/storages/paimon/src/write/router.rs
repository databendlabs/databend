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

use std::sync::atomic::AtomicU64;
use std::sync::atomic::Ordering;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

static NEXT_WRITER_LANE_ID: AtomicU64 = AtomicU64::new(1);

pub fn next_writer_lane_id() -> u64 {
    NEXT_WRITER_LANE_ID.fetch_add(1, Ordering::Relaxed)
}
use paimon::spec::CoreOptions;
use paimon::spec::TableSchema;

use crate::error::map_paimon_error;

/// Length-prefixed partition bytes + big-endian bucket id.
///
/// Keeps the partition/bucket boundary unambiguous so Exchange can hash the
/// opaque key without decoding Paimon partition encoding.
pub fn encode_route_key(partition: &[u8], bucket: i32) -> Vec<u8> {
    let mut key = Vec::with_capacity(4 + partition.len() + 4);
    key.extend_from_slice(&(partition.len() as u32).to_be_bytes());
    key.extend_from_slice(partition);
    key.extend_from_slice(&bucket.to_be_bytes());
    key
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PaimonWriteRoute {
    pub partition: Vec<u8>,
    pub bucket: i32,
    pub key: Vec<u8>,
}

pub struct PaimonWriteRouter {
    fields: Vec<paimon::spec::DataField>,
    partition_indices: Vec<usize>,
    bucket_key_indices: Vec<usize>,
    bucket_count: i32,
}

impl PaimonWriteRouter {
    pub fn try_create(schema: &TableSchema) -> Result<Self> {
        let core_options = CoreOptions::new(schema.options());
        let bucket_count = core_options.bucket();
        let partition_keys = schema.partition_keys();
        let bucket_keys = schema.bucket_keys();
        let config_hint = format!(
            "bucket={bucket_count}, partition_keys={partition_keys:?}, bucket_keys={bucket_keys:?}, primary_keys={:?}",
            schema.primary_keys()
        );

        if schema.primary_keys().is_empty() {
            return Err(ErrorCode::BadArguments(format!(
                "PaimonWriteRouter requires a primary-key table ({config_hint})"
            )));
        }
        if bucket_count < 1 {
            return Err(ErrorCode::BadArguments(format!(
                "PaimonWriteRouter requires fixed bucket >= 1 ({config_hint})"
            )));
        }
        if bucket_keys.is_empty() {
            return Err(ErrorCode::BadArguments(format!(
                "PaimonWriteRouter requires non-empty bucket keys ({config_hint})"
            )));
        }

        let fields = schema.fields().to_vec();
        let partition_indices =
            resolve_field_indices(&fields, partition_keys, "partition", &config_hint)?;
        let bucket_key_indices =
            resolve_field_indices(&fields, &bucket_keys, "bucket key", &config_hint)?;

        Ok(Self {
            fields,
            partition_indices,
            bucket_key_indices,
            bucket_count,
        })
    }

    pub fn route_batch(&self, batch: &arrow_array::RecordBatch) -> Result<Vec<PaimonWriteRoute>> {
        let partitions =
            paimon::spec::batch_to_serialized_bytes(batch, &self.partition_indices, &self.fields)
                .map_err(map_paimon_error)?;
        let hashes = paimon::spec::batch_hash_codes(batch, &self.bucket_key_indices, &self.fields)
            .map_err(map_paimon_error)?;
        Ok(partitions
            .into_iter()
            .zip(hashes)
            .map(|(partition, hash)| {
                let bucket = (hash % self.bucket_count).wrapping_abs();
                let key = encode_route_key(&partition, bucket);
                PaimonWriteRoute {
                    partition,
                    bucket,
                    key,
                }
            })
            .collect())
    }
}

fn resolve_field_indices(
    fields: &[paimon::spec::DataField],
    names: &[String],
    kind: &str,
    config_hint: &str,
) -> Result<Vec<usize>> {
    let mut indices = Vec::with_capacity(names.len());
    for name in names {
        match fields.iter().position(|f| f.name() == name) {
            Some(idx) => indices.push(idx),
            None => {
                return Err(ErrorCode::BadArguments(format!(
                    "PaimonWriteRouter cannot find {kind} field '{name}' ({config_hint})"
                )));
            }
        }
    }
    Ok(indices)
}
