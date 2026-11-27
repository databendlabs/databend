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
use std::sync::Arc;

use databend_common_catalog::runtime_filter_info::RuntimeFilterInfo;
use databend_common_catalog::runtime_filter_info::RuntimeFilterReady;
use databend_common_exception::Result;
use parking_lot::Mutex;

use super::convert::build_runtime_filter_infos;
use super::merge::merge_join_runtime_filter_packets;
use super::packet::JoinRuntimeFilterPacket;
use super::packet::RuntimeFilterPacket;
use crate::physical_plans::PhysicalCteRuntimeFilter;
use crate::pipelines::processors::transforms::hash_join::desc::RuntimeFilterDesc;

/// The keys that one source join contributes to a CTE producer filter.
#[derive(Clone, Debug)]
pub enum CteFilterSourceKeys {
    /// The build side is empty.
    Empty,
    /// The keys are not known, e.g. the build side spilled or the join
    /// finished before it published its filters.
    Unknown,
    Known {
        packet: RuntimeFilterPacket,
        build_rows: usize,
    },
}

impl CteFilterSourceKeys {
    pub fn from_packet(packet: &JoinRuntimeFilterPacket, filter_id: usize) -> Self {
        if packet.disable_all_due_to_spill {
            return Self::Unknown;
        }
        match &packet.packets {
            None if packet.build_rows == 0 => Self::Empty,
            None => Self::Unknown,
            Some(packets) => match packets.get(&filter_id) {
                Some(packet_of_filter) => Self::Known {
                    packet: packet_of_filter.clone(),
                    build_rows: packet.build_rows,
                },
                None => Self::Unknown,
            },
        }
    }
}

#[derive(Clone, Copy)]
pub struct CteFilterThresholds {
    pub inlist: usize,
    pub bloom: usize,
    pub min_max: usize,
    pub selectivity: u64,
    pub max_threads: usize,
}

/// Collects the source filters of one CTE producer filter. When every source
/// has arrived, it installs their union on the producer scans and wakes them.
pub struct CteRuntimeFilterCollector {
    desc: RuntimeFilterDesc,
    source_filters: Vec<usize>,
    thresholds: CteFilterThresholds,
    ready: Vec<Arc<RuntimeFilterReady>>,
    received: Mutex<Option<HashMap<usize, CteFilterSourceKeys>>>,
}

impl CteRuntimeFilterCollector {
    pub fn create(
        filter: &PhysicalCteRuntimeFilter,
        thresholds: CteFilterThresholds,
    ) -> CteRuntimeFilterCollector {
        let desc = RuntimeFilterDesc::from(&filter.filter);
        let enable_statistics_pruning = (desc.enable_min_max_runtime_filter
            && thresholds.min_max > 0)
            || (desc.enable_inlist_runtime_filter && thresholds.inlist > 0);
        let ready = desc
            .probe_targets
            .iter()
            .map(|(probe_key, _)| {
                Arc::new(RuntimeFilterReady::for_statistics_probe_exprs(
                    enable_statistics_pruning,
                    [probe_key],
                ))
            })
            .collect();
        CteRuntimeFilterCollector {
            desc,
            source_filters: filter.source_filters.clone(),
            thresholds,
            ready,
            received: Mutex::new(Some(HashMap::new())),
        }
    }

    pub fn source_filters(&self) -> &[usize] {
        &self.source_filters
    }

    /// The readiness signal of each producer scan, by scan id.
    pub fn ready_by_scan(&self) -> impl Iterator<Item = (usize, &Arc<RuntimeFilterReady>)> {
        self.desc
            .probe_targets
            .iter()
            .map(|(_, scan_id)| *scan_id)
            .zip(self.ready.iter())
    }

    /// Records the keys of a source. Returns all keys once the last source
    /// arrives. Later calls for a source that already arrived are ignored.
    pub fn add_source(
        &self,
        filter_id: usize,
        keys: CteFilterSourceKeys,
    ) -> Option<HashMap<usize, CteFilterSourceKeys>> {
        if !self.source_filters.contains(&filter_id) {
            return None;
        }
        let mut received = self.received.lock();
        let sources = received.as_mut()?;
        sources.entry(filter_id).or_insert(keys);
        if sources.len() < self.source_filters.len() {
            return None;
        }
        received.take()
    }

    /// Builds the union of `sources` for the producer scans. Returns `None`
    /// if some keys are unknown, because then no producer row may be dropped.
    pub async fn build(
        &self,
        sources: HashMap<usize, CteFilterSourceKeys>,
    ) -> Result<Option<HashMap<usize, RuntimeFilterInfo>>> {
        let mut packets = Vec::with_capacity(sources.len());
        for keys in sources.into_values() {
            packets.push(match keys {
                CteFilterSourceKeys::Unknown => return Ok(None),
                CteFilterSourceKeys::Empty => JoinRuntimeFilterPacket::complete_without_filters(0),
                CteFilterSourceKeys::Known {
                    mut packet,
                    build_rows,
                } => {
                    packet.id = self.desc.id;
                    JoinRuntimeFilterPacket::complete(
                        HashMap::from([(self.desc.id, packet)]),
                        build_rows,
                    )
                }
            });
        }
        // Merging the packets of one filter from several nodes and merging
        // the packets of several joins both take the union of the keys.
        let union = merge_join_runtime_filter_packets(
            packets,
            self.thresholds.inlist,
            self.thresholds.bloom,
            self.thresholds.min_max,
        )?;
        let infos = build_runtime_filter_infos(
            union,
            HashMap::from([(self.desc.id, &self.desc)]),
            self.thresholds.selectivity,
            self.thresholds.max_threads,
        )
        .await?;
        Ok(Some(infos))
    }

    /// Wakes the producer scans. They read whatever filters are installed.
    pub fn notify_ready(&self) {
        for ready in &self.ready {
            let _ = ready.runtime_filter_watcher.send(Some(()));
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_common_expression::Column;
    use databend_common_expression::ColumnBuilder;
    use databend_common_expression::RemoteExpr;
    use databend_common_expression::Scalar;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::NumberDataType;
    use databend_common_expression::types::NumberScalar;

    use super::super::packet::SerializableDomain;
    use super::*;
    use crate::physical_plans::PhysicalRuntimeFilter;

    const PRODUCER_SCAN: usize = 3;

    fn int32() -> DataType {
        DataType::Number(NumberDataType::Int32)
    }

    fn collector(source_filters: Vec<usize>) -> CteRuntimeFilterCollector {
        let filter = PhysicalRuntimeFilter {
            id: 9,
            build_key: RemoteExpr::ColumnRef {
                span: None,
                id: 0,
                data_type: int32(),
                display_name: "k".to_string(),
            },
            probe_targets: vec![(
                RemoteExpr::ColumnRef {
                    span: None,
                    id: "k".to_string(),
                    data_type: int32(),
                    display_name: "k".to_string(),
                },
                PRODUCER_SCAN,
            )],
            build_table_rows: Some(1000),
            enable_bloom_runtime_filter: false,
            enable_inlist_runtime_filter: true,
            enable_min_max_runtime_filter: true,
        };
        CteRuntimeFilterCollector::create(
            &PhysicalCteRuntimeFilter {
                cte_name: "cte".to_string(),
                filter,
                source_filters,
            },
            CteFilterThresholds {
                inlist: 1024,
                bloom: 1024,
                min_max: 1024,
                selectivity: 10,
                max_threads: 1,
            },
        )
    }

    fn keys(source_id: usize, values: &[i32]) -> CteFilterSourceKeys {
        let mut builder = ColumnBuilder::with_capacity(&int32(), values.len());
        for value in values {
            builder.push(Scalar::Number(NumberScalar::Int32(*value)).as_ref());
        }
        let inlist: Column = builder.build();
        let bound = |value: i32| Scalar::Number(NumberScalar::Int32(value));
        CteFilterSourceKeys::Known {
            packet: RuntimeFilterPacket {
                id: source_id,
                inlist: Some(inlist),
                min_max: Some(SerializableDomain {
                    min: bound(*values.iter().min().unwrap()),
                    max: bound(*values.iter().max().unwrap()),
                }),
                bloom: None,
            },
            build_rows: values.len(),
        }
    }

    #[test]
    fn test_collector_waits_for_every_source() {
        let collector = collector(vec![1, 2]);
        assert!(collector.add_source(7, keys(7, &[1])).is_none());
        assert!(collector.add_source(1, keys(1, &[1])).is_none());
        // A source that already arrived keeps its first keys.
        assert!(collector.add_source(1, keys(1, &[5])).is_none());
        let sources = collector.add_source(2, keys(2, &[2])).unwrap();
        assert_eq!(sources.len(), 2);
        assert!(matches!(
            &sources[&1],
            CteFilterSourceKeys::Known { packet, .. } if packet.inlist.as_ref().unwrap().len() == 1
        ));
        // The filter is built once.
        assert!(collector.add_source(2, keys(2, &[2])).is_none());
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_collector_installs_union_of_sources() -> Result<()> {
        let collector = collector(vec![1, 2, 3]);
        collector.add_source(1, keys(1, &[10, 20]));
        collector.add_source(2, CteFilterSourceKeys::Empty);
        let sources = collector.add_source(3, keys(3, &[30])).unwrap();

        let filters = collector.build(sources).await?.unwrap();
        let entry = &filters[&PRODUCER_SCAN].filters[0];
        assert_eq!(entry.id, 9);
        assert_eq!(entry.inlist_value_count, 3);
        assert_eq!(entry.build_rows, 3);
        let min_max = entry.min_max.as_ref().unwrap().sql_display();
        assert!(
            min_max.contains("10") && min_max.contains("30"),
            "{min_max}"
        );
        Ok(())
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn test_collector_keeps_all_rows_when_keys_are_unknown() -> Result<()> {
        let collector = collector(vec![1, 2]);
        collector.add_source(1, keys(1, &[10]));
        let sources = collector
            .add_source(2, CteFilterSourceKeys::Unknown)
            .unwrap();
        assert!(collector.build(sources).await?.is_none());

        let spilled = JoinRuntimeFilterPacket::disable_all(10);
        assert!(matches!(
            CteFilterSourceKeys::from_packet(&spilled, 1),
            CteFilterSourceKeys::Unknown
        ));
        let empty = JoinRuntimeFilterPacket::complete_without_filters(0);
        assert!(matches!(
            CteFilterSourceKeys::from_packet(&empty, 1),
            CteFilterSourceKeys::Empty
        ));
        let without_filters = JoinRuntimeFilterPacket::complete_without_filters(10);
        assert!(matches!(
            CteFilterSourceKeys::from_packet(&without_filters, 1),
            CteFilterSourceKeys::Unknown
        ));
        Ok(())
    }
}
