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

use std::collections::HashMap;
use std::time::Instant;

use databend_common_catalog::sbbf::Sbbf;
use databend_common_exception::Result;
use databend_common_expression::Column;

use super::packet::BloomPayload;
use super::packet::JoinRuntimeFilterPacket;
use super::packet::RuntimeFilterPacket;
use super::packet::SerializableBloomFilter;
use super::packet::SerializableDomain;

pub fn merge_join_runtime_filter_packets(
    packets: Vec<JoinRuntimeFilterPacket>,
) -> Result<JoinRuntimeFilterPacket> {
    let instant = Instant::now();
    log::info!(
        "RUNTIME-FILTER: merge_join_runtime_filter_packets input: {:?}",
        packets
    );
    let total_build_rows: usize = packets.iter().map(|packet| packet.build_rows).sum();
    // Skip packets that `JoinRuntimeFilterPacket::packets` is `None`
    let packets = packets
        .into_iter()
        .filter_map(|packet| packet.packets)
        .collect::<Vec<_>>();

    if packets.is_empty() {
        return Ok(JoinRuntimeFilterPacket {
            packets: None,
            build_rows: total_build_rows,
        });
    }

    let mut result = HashMap::new();
    for id in packets[0].keys() {
        result.insert(*id, RuntimeFilterPacket {
            id: *id,
            inlist: merge_inlist(&packets, *id)?,
            min_max: merge_min_max(&packets, *id),
            bloom: merge_bloom(&packets, *id),
        });
    }

    log::info!(
        "RUNTIME-FILTER: merge_join_runtime_filter_packets output: {:?}, elapsed: {:?}",
        result,
        instant.elapsed()
    );
    Ok(JoinRuntimeFilterPacket {
        packets: Some(result),
        build_rows: total_build_rows,
    })
}

fn merge_inlist(
    packets: &[HashMap<usize, RuntimeFilterPacket>],
    rf_id: usize,
) -> Result<Option<Column>> {
    if packets
        .iter()
        .any(|packet| packet.get(&rf_id).unwrap().inlist.is_none())
    {
        return Ok(None);
    }
    let columns = packets
        .iter()
        .map(|packet| packet.get(&rf_id).unwrap().inlist.clone().unwrap())
        .collect::<Vec<_>>();
    let column = Column::concat_columns(columns.into_iter())?;
    Ok(Some(column))
}

fn merge_min_max(
    packets: &[HashMap<usize, RuntimeFilterPacket>],
    rf_id: usize,
) -> Option<SerializableDomain> {
    if packets
        .iter()
        .any(|packet| packet.get(&rf_id).unwrap().min_max.is_none())
    {
        return None;
    }
    let min = packets
        .iter()
        .map(|packet| {
            packet
                .get(&rf_id)
                .unwrap()
                .min_max
                .as_ref()
                .unwrap()
                .min
                .clone()
        })
        .min()
        .unwrap();
    let max = packets
        .iter()
        .map(|packet| {
            packet
                .get(&rf_id)
                .unwrap()
                .min_max
                .as_ref()
                .unwrap()
                .max
                .clone()
        })
        .max()
        .unwrap();
    Some(SerializableDomain { min, max })
}

fn merge_bloom(
    packets: &[HashMap<usize, RuntimeFilterPacket>],
    rf_id: usize,
) -> Option<BloomPayload> {
    if packets
        .iter()
        .any(|packet| packet.get(&rf_id).unwrap().bloom.is_none())
    {
        return None;
    }

    let first = packets[0].get(&rf_id).unwrap().bloom.as_ref().unwrap();
    match first {
        BloomPayload::Hashes(_) => {
            // Local merge path: concatenate hashes
            let mut merged = match first {
                BloomPayload::Hashes(hashes) => hashes.clone(),
                _ => unreachable!(),
            };

            for packet in packets.iter().skip(1) {
                let other = packet.get(&rf_id).unwrap().bloom.as_ref().unwrap();
                match other {
                    BloomPayload::Hashes(hashes) => merged.extend_from_slice(hashes),
                    BloomPayload::Filter(_) => {
                        // Mixed variants are not expected today. Fallback to disabling bloom.
                        log::warn!(
                            "RUNTIME-FILTER: mixed bloom payload variants detected for id {}, disabling bloom merge",
                            rf_id
                        );
                        return None;
                    }
                }
            }
            Some(BloomPayload::Hashes(merged))
        }
        BloomPayload::Filter(_) => {
            // Global merge path: union serialized bloom filters
            let mut base_bytes = match first {
                BloomPayload::Filter(f) => f.data.clone(),
                _ => unreachable!(),
            };

            let mut base = match Sbbf::from_bytes(&base_bytes) {
                Ok(bf) => bf,
                Err(e) => {
                    log::warn!(
                        "RUNTIME-FILTER: failed to deserialize bloom filter for id {}: {}",
                        rf_id,
                        e
                    );
                    return None;
                }
            };

            for packet in packets.iter().skip(1) {
                let other = packet.get(&rf_id).unwrap().bloom.as_ref().unwrap();
                let bytes = match other {
                    BloomPayload::Filter(f) => &f.data,
                    BloomPayload::Hashes(_) => {
                        log::warn!(
                            "RUNTIME-FILTER: mixed bloom payload variants detected for id {}, disabling bloom merge",
                            rf_id
                        );
                        return None;
                    }
                };

                let other_bf = match Sbbf::from_bytes(bytes) {
                    Ok(bf) => bf,
                    Err(e) => {
                        log::warn!(
                            "RUNTIME-FILTER: failed to deserialize bloom filter for id {}: {}",
                            rf_id,
                            e
                        );
                        return None;
                    }
                };

                base.union(&other_bf);
            }

            base_bytes = base.to_bytes();
            Some(BloomPayload::Filter(SerializableBloomFilter {
                data: base_bytes,
            }))
        }
    }
}
