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
    inlist_threshold: usize,
    bloom_threshold: usize,
    min_max_threshold: usize,
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

    let should_merge_inlist = total_build_rows < inlist_threshold;
    let should_merge_bloom = total_build_rows < bloom_threshold;
    let should_merge_min_max = total_build_rows < min_max_threshold;

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
            inlist: if should_merge_inlist {
                merge_inlist(&packets, *id)?
            } else {
                None
            },
            min_max: if should_merge_min_max {
                merge_min_max(&packets, *id)
            } else {
                None
            },
            bloom: if should_merge_bloom {
                merge_bloom(&packets, *id)
            } else {
                None
            },
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

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use databend_common_expression::ColumnBuilder;
    use databend_common_expression::Scalar;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::NumberScalar;

    use super::*;

    fn int_column(values: &[i32]) -> Column {
        let data_type = DataType::Number(NumberDataType::Int32);
        let mut builder = ColumnBuilder::with_capacity(&data_type, values.len());
        for value in values {
            builder.push(Scalar::Number(NumberScalar::Int32(*value)).as_ref());
        }
        builder.build()
    }

    #[test]
    fn test_merge_short_circuit_all_types() -> Result<()> {
        let mut runtime_filters = HashMap::new();
        runtime_filters.insert(1, RuntimeFilterPacket {
            id: 1,
            inlist: Some(int_column(&[1, 2, 3])),
            min_max: Some(SerializableDomain {
                min: Scalar::Number(NumberScalar::Int32(1)),
                max: Scalar::Number(NumberScalar::Int32(3)),
            }),
            bloom: Some(vec![11, 22, 33]),
        });

        let packet = JoinRuntimeFilterPacket::complete(runtime_filters, 100);
        let merged = merge_join_runtime_filter_packets(vec![packet], 1, 1, 1)?;

        assert_eq!(merged.build_rows, 100);
        let packet = merged.packets.unwrap().remove(&1).unwrap();
        assert!(packet.inlist.is_none());
        assert!(packet.min_max.is_none());
        assert!(packet.bloom.is_none());
        assert!(!merged.disable_all_due_to_spill);
        Ok(())
    }

    #[test]
    fn test_merge_short_circuit_inlist_only() -> Result<()> {
        let mut runtime_filters_1 = HashMap::new();
        runtime_filters_1.insert(7, RuntimeFilterPacket {
            id: 7,
            inlist: Some(int_column(&[1, 2])),
            min_max: Some(SerializableDomain {
                min: Scalar::Number(NumberScalar::Int32(1)),
                max: Scalar::Number(NumberScalar::Int32(5)),
            }),
            bloom: Some(vec![1, 2]),
        });
        let mut runtime_filters_2 = HashMap::new();
        runtime_filters_2.insert(7, RuntimeFilterPacket {
            id: 7,
            inlist: Some(int_column(&[3, 4])),
            min_max: Some(SerializableDomain {
                min: Scalar::Number(NumberScalar::Int32(-1)),
                max: Scalar::Number(NumberScalar::Int32(8)),
            }),
            bloom: Some(vec![3, 4]),
        });

        let merged = merge_join_runtime_filter_packets(
            vec![
                JoinRuntimeFilterPacket::complete(runtime_filters_1, 6),
                JoinRuntimeFilterPacket::complete(runtime_filters_2, 5),
            ],
            10,
            100,
            100,
        )?;

        let packet = merged.packets.unwrap().remove(&7).unwrap();
        assert_eq!(merged.build_rows, 11);
        assert!(packet.inlist.is_none());
        assert_eq!(packet.bloom, Some(vec![1, 2, 3, 4]));
        assert_eq!(
            packet.min_max,
            Some(SerializableDomain {
                min: Scalar::Number(NumberScalar::Int32(-1)),
                max: Scalar::Number(NumberScalar::Int32(8)),
            })
        );
        Ok(())
    }

    #[test]
    fn test_merge_spill_priority() -> Result<()> {
        let merged = merge_join_runtime_filter_packets(
            vec![
                JoinRuntimeFilterPacket::disable_all(10),
                JoinRuntimeFilterPacket::complete_without_filters(5),
            ],
            usize::MAX,
            usize::MAX,
            usize::MAX,
        )?;
        assert!(merged.disable_all_due_to_spill);
        assert_eq!(merged.build_rows, 15);
        Ok(())
    }
}
