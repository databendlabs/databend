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

use databend_common_exception::Result;
use databend_common_expression::Column;

use super::packet::JoinRuntimeFilterPacket;
use super::packet::RuntimeFilterPacket;
use super::packet::SerializableDomain;

pub fn merge_join_runtime_filter_packets(
    packets: Vec<JoinRuntimeFilterPacket>,
) -> Result<JoinRuntimeFilterPacket> {
    log::info!(
        "RUNTIME-FILTER: merge_join_runtime_filter_packets input: {:?}",
        packets
    );
    let total_build_rows: usize = packets.iter().map(|packet| packet.build_rows).sum();

    // If any packet is incomplete (disable_all_due_to_spill), the merged result is also incomplete
    if packets.iter().any(|packet| packet.disable_all_due_to_spill) {
        return Ok(JoinRuntimeFilterPacket::disable_all(total_build_rows));
    }

    let packets = packets
        .into_iter()
        .filter_map(|packet| packet.packets)
        .collect::<Vec<_>>();

    // Skip packets that `JoinRuntimeFilterPacket::packets` is `None`
    if packets.is_empty() {
        return Ok(JoinRuntimeFilterPacket::complete_without_filters(
            total_build_rows,
        ));
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
        "RUNTIME-FILTER: merge_join_runtime_filter_packets output: {:?}",
        result
    );

    if result.is_empty() {
        return Ok(JoinRuntimeFilterPacket::complete_without_filters(
            total_build_rows,
        ));
    }

    Ok(JoinRuntimeFilterPacket::complete(result, total_build_rows))
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

fn merge_bloom(packets: &[HashMap<usize, RuntimeFilterPacket>], rf_id: usize) -> Option<Vec<u64>> {
    if packets
        .iter()
        .any(|packet| packet.get(&rf_id).unwrap().bloom.is_none())
    {
        return None;
    }
    let mut bloom = packets[0]
        .get(&rf_id)
        .unwrap()
        .bloom
        .as_ref()
        .unwrap()
        .clone();
    for packet in packets.iter().skip(1) {
        let other = packet.get(&rf_id).unwrap().bloom.as_ref().unwrap();
        bloom.extend_from_slice(other);
    }
    Some(bloom)
}
