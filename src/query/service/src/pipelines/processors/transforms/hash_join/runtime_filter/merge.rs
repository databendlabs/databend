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
use super::packet::SpatialPacket;
use super::spatial::merge_rtrees_to_threshold;
use crate::physical_plans::SpatialRuntimeFilterMode;

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

fn merge_spatial(
    packets: &[HashMap<usize, RuntimeFilterPacket>],
    rf_id: usize,
    spatial_threshold: usize,
) -> Result<Option<SpatialPacket>> {
    let mut srid: Option<i32> = None;
    let mut mode: Option<SpatialRuntimeFilterMode> = None;
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
        let entry_mode = spatial.mode.clone();
        if let Some(prev) = mode.as_ref() {
            if prev != &entry_mode {
                return Ok(None);
            }
        } else {
            mode = Some(entry_mode);
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
    let Some(mode) = mode else {
        return Ok(None);
    };

    let rtrees = merge_rtrees_to_threshold(rtrees, spatial_threshold)?;
    if rtrees.is_empty() {
        return Ok(None);
    }

    Ok(Some(SpatialPacket {
        valid: true,
        srid,
        mode,
        rtrees,
    }))
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use databend_common_expression::ColumnBuilder;
    use databend_common_expression::Scalar;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::NumberScalar;
    use geo_index::rtree::RTreeBuilder;
    use geo_index::rtree::sort::HilbertSort;

    use super::*;
    use crate::pipelines::processors::transforms::hash_join::runtime_filter::spatial::rtree_bounds_from_bytes;

    fn int_column(values: &[i32]) -> Column {
        let data_type = DataType::Number(NumberDataType::Int32);
        let mut builder = ColumnBuilder::with_capacity(&data_type, values.len());
        for value in values {
            builder.push(Scalar::Number(NumberScalar::Int32(*value)).as_ref());
        }
        builder.build()
    }

    fn spatial_packet(
        id: usize,
        bounds: [f64; 4],
        mode: SpatialRuntimeFilterMode,
    ) -> RuntimeFilterPacket {
        let mut builder = RTreeBuilder::<f64>::new(1);
        builder.add(bounds[0], bounds[1], bounds[2], bounds[3]);
        let rtrees = builder.finish::<HilbertSort>().into_inner();

        RuntimeFilterPacket {
            id,
            inlist: None,
            min_max: None,
            bloom: None,
            spatial: Some(SpatialPacket {
                valid: true,
                srid: Some(4326),
                mode,
                rtrees,
            }),
        }
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
        let mut runtime_filters_1 = HashMap::new();
        runtime_filters_1.insert(7, RuntimeFilterPacket {
            id: 7,
            inlist: Some(int_column(&[1, 2])),
            min_max: Some(SerializableDomain {
                min: Scalar::Number(NumberScalar::Int32(1)),
                max: Scalar::Number(NumberScalar::Int32(5)),
            }),
            bloom: Some(vec![1, 2]),
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
            bloom: Some(vec![3, 4]),
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
            usize::MAX,
        )?;
        assert!(merged.disable_all_due_to_spill);
        assert_eq!(merged.build_rows, 15);
        Ok(())
    }

    #[test]
    fn test_merge_spatial_preserves_distance_mode() -> Result<()> {
        let mode = SpatialRuntimeFilterMode::DistanceWithin(10.0);

        let merged = merge_join_runtime_filter_packets(
            vec![
                JoinRuntimeFilterPacket::complete(
                    HashMap::from([(3, spatial_packet(3, [0.0, 0.0, 1.0, 1.0], mode.clone()))]),
                    1,
                ),
                JoinRuntimeFilterPacket::complete(
                    HashMap::from([(3, spatial_packet(3, [5.0, 5.0, 6.0, 6.0], mode.clone()))]),
                    1,
                ),
            ],
            usize::MAX,
            usize::MAX,
            usize::MAX,
            usize::MAX,
        )?;

        let spatial = merged.packets.unwrap().remove(&3).unwrap().spatial.unwrap();
        assert_eq!(spatial.mode, mode);
        assert_eq!(
            rtree_bounds_from_bytes(&spatial.rtrees)?,
            Some([0.0, 0.0, 6.0, 6.0])
        );
        Ok(())
    }

    #[test]
    fn test_merge_spatial_drops_mismatched_modes() -> Result<()> {
        let merged = merge_join_runtime_filter_packets(
            vec![
                JoinRuntimeFilterPacket::complete(
                    HashMap::from([(
                        5,
                        spatial_packet(
                            5,
                            [0.0, 0.0, 1.0, 1.0],
                            SpatialRuntimeFilterMode::Intersects,
                        ),
                    )]),
                    1,
                ),
                JoinRuntimeFilterPacket::complete(
                    HashMap::from([(
                        5,
                        spatial_packet(
                            5,
                            [5.0, 5.0, 6.0, 6.0],
                            SpatialRuntimeFilterMode::DistanceWithin(10.0),
                        ),
                    )]),
                    1,
                ),
            ],
            usize::MAX,
            usize::MAX,
            usize::MAX,
            usize::MAX,
        )?;

        assert!(
            merged
                .packets
                .unwrap()
                .remove(&5)
                .unwrap()
                .spatial
                .is_none()
        );
        Ok(())
    }
}
