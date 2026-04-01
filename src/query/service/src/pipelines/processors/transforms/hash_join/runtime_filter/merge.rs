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

use databend_common_catalog::sbbf::Sbbf;
use databend_common_exception::Result;
use databend_common_expression::Column;

use super::packet::JoinRuntimeFilterPacket;
use super::packet::RuntimeFilterPacket;
use super::packet::SerializableDomain;
use super::packet::SpatialPacket;
use super::spatial::merge_rtrees_to_threshold;

pub fn merge_join_runtime_filter_packets(
    packets: Vec<JoinRuntimeFilterPacket>,
    inlist_threshold: usize,
    bloom_threshold: usize,
    min_max_threshold: usize,
    spatial_threshold: usize,
) -> Result<JoinRuntimeFilterPacket> {
    log::info!(
        "RUNTIME-FILTER: merge_join_runtime_filter_packets input: {:?}",
        packets
    );
    let total_build_rows: usize = packets.iter().map(|packet| packet.build_rows).sum();

    // If any packet is incomplete (disable_all_due_to_spill), the merged result is also incomplete
    if packets.iter().any(|packet| packet.disable_all_due_to_spill) {
        let result = JoinRuntimeFilterPacket::disable_all(total_build_rows);
        log::info!(
            "RUNTIME-FILTER: merge_join_runtime_filter_packets output: build_rows={}, disable_all_due_to_spill=true",
            total_build_rows
        );
        return Ok(result);
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
            spatial: merge_spatial(&packets, *id, spatial_threshold)?,
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

fn merge_bloom(packets: &[HashMap<usize, RuntimeFilterPacket>], rf_id: usize) -> Option<Vec<u32>> {
    if packets
        .iter()
        .any(|packet| packet.get(&rf_id).unwrap().bloom.is_none())
    {
        return None;
    }
    let first = packets[0].get(&rf_id).unwrap().bloom.clone().unwrap();
    let mut merged = Sbbf::from_u32s(first)?;
    for packet in packets.iter().skip(1) {
        let other_words = packet.get(&rf_id).unwrap().bloom.clone().unwrap();
        let other = Sbbf::from_u32s(other_words)?;
        merged.union(&other);
    }
    Some(merged.into_u32s())
}

fn merge_spatial(
    packets: &[HashMap<usize, RuntimeFilterPacket>],
    rf_id: usize,
    spatial_threshold: usize,
) -> Result<Option<SpatialPacket>> {
    let mut srid: Option<i32> = None;
    let mut rtrees = Vec::new();

    for packet in packets {
        let Some(entry) = packet.get(&rf_id) else {
            return Ok(None);
        };
        let Some(spatial) = entry.spatial.as_ref() else {
            return Ok(None);
        };
        if !spatial.valid {
            return Ok(None);
        }
        if let Some(entry_srid) = spatial.srid {
            if let Some(prev) = srid {
                if prev != entry_srid {
                    return Ok(None);
                }
            } else {
                srid = Some(entry_srid);
            }
        }
        if !spatial.rtrees.is_empty() {
            rtrees.push(spatial.rtrees.as_slice());
        }
    }

    let rtrees = merge_rtrees_to_threshold(rtrees, spatial_threshold)?;
    if rtrees.is_empty() {
        return Ok(None);
    }

    Ok(Some(SpatialPacket {
        valid: true,
        srid,
        rtrees,
    }))
}

/// Pairwise merge of two runtime filter packets without threshold checks.
/// Used for work-stealing incremental merge within a node.
pub fn merge_two_runtime_filter_packets(
    mut a: JoinRuntimeFilterPacket,
    mut b: JoinRuntimeFilterPacket,
) -> Result<JoinRuntimeFilterPacket> {
    let total_build_rows = a.build_rows + b.build_rows;
    let disable_all = a.disable_all_due_to_spill || b.disable_all_due_to_spill;

    if disable_all {
        return Ok(JoinRuntimeFilterPacket::disable_all(total_build_rows));
    }

    let (a_packets, b_packets) = match (a.packets.take(), b.packets.take()) {
        (None, None) => {
            return Ok(JoinRuntimeFilterPacket::complete_without_filters(
                total_build_rows,
            ));
        }
        (Some(p), None) | (None, Some(p)) => {
            return Ok(JoinRuntimeFilterPacket::complete(p, total_build_rows));
        }
        (Some(a), Some(b)) => (a, b),
    };

    let mut result = HashMap::new();
    for (id, mut a_pkt) in a_packets {
        if let Some(mut b_pkt) = b_packets.get(&id).cloned() {
            // Merge bloom via Sbbf::union
            let bloom = match (a_pkt.bloom.take(), b_pkt.bloom.take()) {
                (Some(a_words), Some(b_words)) => {
                    if let (Some(mut a_filter), Some(b_filter)) =
                        (Sbbf::from_u32s(a_words), Sbbf::from_u32s(b_words))
                    {
                        a_filter.union(&b_filter);
                        Some(a_filter.into_u32s())
                    } else {
                        None
                    }
                }
                _ => None,
            };

            // Merge inlist via concat
            let inlist = match (a_pkt.inlist.take(), b_pkt.inlist.take()) {
                (Some(a_col), Some(b_col)) => {
                    Some(Column::concat_columns([a_col, b_col].into_iter())?)
                }
                _ => None,
            };

            // Merge min_max
            let min_max = match (a_pkt.min_max.take(), b_pkt.min_max.take()) {
                (Some(a_mm), Some(b_mm)) => Some(SerializableDomain {
                    min: a_mm.min.min(b_mm.min),
                    max: a_mm.max.max(b_mm.max),
                }),
                _ => None,
            };

            // Merge spatial
            let spatial = match (a_pkt.spatial.take(), b_pkt.spatial.take()) {
                (Some(a_sp), Some(b_sp)) => {
                    if a_sp.valid && b_sp.valid && a_sp.srid == b_sp.srid {
                        let rtrees = merge_rtrees_to_threshold(
                            vec![a_sp.rtrees.as_slice(), b_sp.rtrees.as_slice()],
                            usize::MAX,
                        )?;
                        Some(SpatialPacket {
                            valid: true,
                            srid: a_sp.srid,
                            rtrees,
                        })
                    } else {
                        None
                    }
                }
                _ => None,
            };

            result.insert(id, RuntimeFilterPacket {
                id,
                bloom,
                inlist,
                min_max,
                spatial,
            });
        }
    }

    if result.is_empty() {
        return Ok(JoinRuntimeFilterPacket::complete_without_filters(
            total_build_rows,
        ));
    }

    Ok(JoinRuntimeFilterPacket::complete(result, total_build_rows))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use databend_common_catalog::sbbf::Sbbf;
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

    fn make_bloom(hashes: &[u64]) -> Vec<u32> {
        let mut filter = Sbbf::new_with_ndv_fpp(100, 0.01).unwrap();
        filter.insert_hash_batch(hashes);
        filter.into_u32s()
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
            bloom: Some(make_bloom(&[11, 22, 33])),
            spatial: None,
        });

        let packet = JoinRuntimeFilterPacket::complete(runtime_filters, 100);
        let merged = merge_join_runtime_filter_packets(vec![packet], 1, 1, 1, 1)?;

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
        let bloom1 = make_bloom(&[1, 2]);
        let bloom2 = make_bloom(&[3, 4]);

        let mut runtime_filters_1 = HashMap::new();
        runtime_filters_1.insert(7, RuntimeFilterPacket {
            id: 7,
            inlist: Some(int_column(&[1, 2])),
            min_max: Some(SerializableDomain {
                min: Scalar::Number(NumberScalar::Int32(1)),
                max: Scalar::Number(NumberScalar::Int32(5)),
            }),
            bloom: Some(bloom1.clone()),
            spatial: None,
        });
        let mut runtime_filters_2 = HashMap::new();
        runtime_filters_2.insert(7, RuntimeFilterPacket {
            id: 7,
            inlist: Some(int_column(&[3, 4])),
            min_max: Some(SerializableDomain {
                min: Scalar::Number(NumberScalar::Int32(-1)),
                max: Scalar::Number(NumberScalar::Int32(8)),
            }),
            bloom: Some(bloom2.clone()),
            spatial: None,
        });

        let merged = merge_join_runtime_filter_packets(
            vec![
                JoinRuntimeFilterPacket::complete(runtime_filters_1, 6),
                JoinRuntimeFilterPacket::complete(runtime_filters_2, 5),
            ],
            10,
            100,
            100,
            100,
        )?;

        let packet = merged.packets.unwrap().remove(&7).unwrap();
        assert_eq!(merged.build_rows, 11);
        assert!(packet.inlist.is_none());
        // Bloom should be a merged Sbbf containing all hashes
        let merged_filter = Sbbf::from_u32s(packet.bloom.unwrap()).unwrap();
        for h in &[1u64, 2, 3, 4] {
            assert!(merged_filter.check_hash(*h));
        }
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
            usize::MAX,
        )?;
        assert!(merged.disable_all_due_to_spill);
        assert_eq!(merged.build_rows, 15);
        Ok(())
    }
}
