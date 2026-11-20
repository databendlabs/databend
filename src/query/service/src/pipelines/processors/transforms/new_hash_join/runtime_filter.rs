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

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::PoisonError;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::AtomicUsize;
use std::sync::atomic::Ordering;

use databend_common_catalog::runtime_filter_info::RuntimeFilterReady;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::FunctionContext;

use crate::physical_plans::HashJoin;
use crate::pipelines::processors::transforms::JoinRuntimeFilterPacket;
use crate::pipelines::processors::transforms::RuntimeFilterDesc;
use crate::pipelines::processors::transforms::RuntimeFilterLocalBuilder;
use crate::pipelines::processors::transforms::build_runtime_filter_infos;
use crate::pipelines::processors::transforms::get_global_runtime_filter_packet;
use crate::pipelines::processors::transforms::merge_join_runtime_filter_packets;
use crate::sessions::QueryContext;
use crate::sessions::TableContextRuntimeFilter;
use crate::sessions::TableContextSettings;

/// Runtime filter state shared by all processors of one hash join, for the whole join lifetime.
///
/// Runtime filters are built from the raw build input, before the join decides between the
/// in-memory and the grace (spilling) path. Every processor adds its packet here, and the
/// leader merges them, so neither spilling nor a mix of memory and grace processors loses
/// part of the build side.
pub struct RuntimeFiltersDesc {
    ctx: Arc<QueryContext>,
    pub func_ctx: FunctionContext,

    pub bloom_threshold: usize,
    pub inlist_threshold: usize,
    pub min_max_threshold: usize,
    pub selectivity_threshold: u64,

    broadcast_id: Option<u32>,
    global_build_side_empty: AtomicBool,
    /// Build rows added by all processors of this join on this node.
    build_rows: AtomicUsize,
    /// Local runtime filter packets, one per processor that finished the build stage.
    packets: Mutex<Vec<JoinRuntimeFilterPacket>>,
    pub filters_desc: Vec<RuntimeFilterDesc>,
    runtime_filters_ready: Vec<Arc<RuntimeFilterReady>>,
}

impl RuntimeFiltersDesc {
    pub fn create(ctx: &Arc<QueryContext>, join: &HashJoin) -> Result<Arc<RuntimeFiltersDesc>> {
        let settings = ctx.get_settings();
        let bloom_threshold = settings.get_bloom_runtime_filter_threshold()? as usize;
        let inlist_threshold = settings.get_inlist_runtime_filter_threshold()? as usize;
        let min_max_threshold = settings.get_min_max_runtime_filter_threshold()? as usize;
        let selectivity_threshold = settings.get_join_runtime_filter_selectivity_threshold()?;
        let func_ctx = ctx.get_function_context()?;

        let mut filters_desc = Vec::with_capacity(join.runtime_filter.filters.len());
        let mut runtime_filters_ready = Vec::with_capacity(join.runtime_filter.filters.len());

        for filter_desc in &join.runtime_filter.filters {
            let filter_desc = RuntimeFilterDesc::from(filter_desc);
            let enable_statistics_pruning = (filter_desc.enable_min_max_runtime_filter
                && min_max_threshold > 0)
                || (filter_desc.enable_inlist_runtime_filter && inlist_threshold > 0);

            for (probe_key, scan_id) in &filter_desc.probe_targets {
                let ready = Arc::new(RuntimeFilterReady::for_statistics_probe_exprs(
                    enable_statistics_pruning,
                    [probe_key],
                ));
                runtime_filters_ready.push(ready.clone());
                ctx.set_runtime_filter_ready(*scan_id, ready);
            }

            filters_desc.push(filter_desc);
        }

        Ok(Arc::new(RuntimeFiltersDesc {
            func_ctx,
            filters_desc,
            bloom_threshold,
            inlist_threshold,
            min_max_threshold,
            selectivity_threshold,
            runtime_filters_ready,
            ctx: ctx.clone(),
            broadcast_id: join.broadcast_id,
            global_build_side_empty: AtomicBool::new(false),
            build_rows: AtomicUsize::new(0),
            packets: Mutex::new(Vec::new()),
        }))
    }

    pub fn create_local_builder(&self) -> Result<Option<RuntimeFilterLocalBuilder>> {
        Ok(RuntimeFilterLocalBuilder::try_create(
            &self.func_ctx,
            self.filters_desc.clone(),
            self.inlist_threshold,
            self.bloom_threshold,
            self.min_max_threshold,
        )?
        .map(|builder| builder.with_bloom_selectivity_threshold(self.selectivity_threshold)))
    }

    /// Count `rows` build rows of this join and return the rows counted before them.
    pub fn add_build_rows(&self, rows: usize) -> usize {
        self.build_rows.fetch_add(rows, Ordering::AcqRel)
    }

    pub fn add_packet(&self, packet: JoinRuntimeFilterPacket) {
        let mut packets = self.packets.lock().unwrap_or_else(PoisonError::into_inner);
        packets.push(packet);
    }

    /// Merge the packets of all processors into the local packet of this node.
    ///
    /// Must be called once, after every processor still running the join added its packet.
    pub fn merge_local_packets(&self) -> Result<JoinRuntimeFilterPacket> {
        let packets = {
            let mut packets = self.packets.lock().unwrap_or_else(PoisonError::into_inner);
            std::mem::take(&mut *packets)
        };

        merge_processor_packets(
            packets,
            self.build_rows.load(Ordering::Acquire),
            self.inlist_threshold,
            self.bloom_threshold,
            self.min_max_threshold,
        )
    }

    /// Close the broadcast source channel and notify runtime filter watchers.
    /// Called when all threads of a hash join are short-circuited (e.g., downstream
    /// LIMIT satisfied via sequential UNION ALL) and no thread will call `globalization`.
    pub fn close_broadcast(&self) {
        if let Some(broadcast_id) = self.broadcast_id {
            self.ctx.broadcast_source_sender(broadcast_id).close();
        }
        for ready in &self.runtime_filters_ready {
            let _ = ready.runtime_filter_watcher.send(Some(()));
        }
    }

    pub async fn globalization(&self) -> Result<()> {
        let mut packet = self.merge_local_packets()?;
        log::info!(
            "RUNTIME-FILTER: globalize runtime filter: build rows {}, total {}, disable_all_due_to_spill: {}",
            packet.build_rows,
            packet.packets.as_ref().map_or(0, |p| p.len()),
            packet.disable_all_due_to_spill
        );

        if let Some(broadcast_id) = self.broadcast_id {
            packet = get_global_runtime_filter_packet(broadcast_id, packet, &self.ctx).await?;
        }

        self.global_build_side_empty
            .store(packet.build_rows == 0, Ordering::Release);

        let runtime_filter_descs = self.filters_desc.iter().map(|r| (r.id, r)).collect();
        let runtime_filter_infos = build_runtime_filter_infos(
            packet,
            runtime_filter_descs,
            self.selectivity_threshold,
            self.ctx.get_settings().get_max_threads()? as usize,
        )
        .await?;

        self.ctx.set_runtime_filter(runtime_filter_infos);

        for runtime_filter_ready in self.runtime_filters_ready.iter() {
            runtime_filter_ready
                .runtime_filter_watcher
                .send(Some(()))
                .map_err(|_| ErrorCode::TokioError("watcher channel is closed"))?;
        }

        Ok(())
    }

    pub fn build_side_empty(&self) -> bool {
        self.global_build_side_empty.load(Ordering::Acquire)
    }
}

/// Merge the packets of the processors of one join on this node, which added `build_rows`
/// build rows in total.
fn merge_processor_packets(
    packets: Vec<JoinRuntimeFilterPacket>,
    build_rows: usize,
    inlist_threshold: usize,
    bloom_threshold: usize,
    min_max_threshold: usize,
) -> Result<JoinRuntimeFilterPacket> {
    let packet_rows = packets
        .iter()
        .map(|packet| packet.build_rows)
        .sum::<usize>();

    if packet_rows != build_rows {
        // A processor that left during the build stage (its output was finished) added rows
        // to the shared hash table without adding a packet. Filters built from the other
        // packets could prune probe rows matching those rows. Only `disable_all` survives the
        // merge with the packets of other nodes, so it marks this packet as incomplete.
        log::info!(
            "RUNTIME-FILTER: disable runtime filters, packets cover {} of {} build rows",
            packet_rows,
            build_rows
        );
        return Ok(JoinRuntimeFilterPacket::disable_all(build_rows));
    }

    // `build_rows` decides whether the build side is empty, so it counts every build row,
    // also when no runtime filter is planned.
    merge_join_runtime_filter_packets(
        packets,
        inlist_threshold,
        bloom_threshold,
        min_max_threshold,
    )
}

#[cfg(test)]
mod tests {
    use databend_common_expression::ColumnRef;
    use databend_common_expression::DataBlock;
    use databend_common_expression::Expr;
    use databend_common_expression::FromData;
    use databend_common_expression::types::DataType;
    use databend_common_expression::types::Int64Type;
    use databend_common_expression::types::NumberDataType;

    use super::*;

    /// The local packet of a processor that added `values`, with a min-max filter only.
    fn packet(values: Vec<i64>) -> JoinRuntimeFilterPacket {
        let desc = RuntimeFilterDesc {
            id: 0,
            build_key: Expr::ColumnRef(ColumnRef {
                span: None,
                id: 0,
                data_type: DataType::Number(NumberDataType::Int64),
                display_name: "k".to_string(),
            }),
            probe_targets: vec![],
            build_table_rows: None,
            probe_table_rows: None,
            enable_bloom_runtime_filter: false,
            enable_inlist_runtime_filter: false,
            enable_min_max_runtime_filter: true,
        };
        let mut builder = RuntimeFilterLocalBuilder::try_create(
            &FunctionContext::default(),
            vec![desc],
            0,
            0,
            usize::MAX,
        )
        .unwrap()
        .unwrap();
        builder
            .add_block(&DataBlock::new_from_columns(vec![Int64Type::from_data(
                values,
            )]))
            .unwrap();
        builder.finish(false).unwrap()
    }

    fn merge(packets: Vec<JoinRuntimeFilterPacket>, build_rows: usize) -> JoinRuntimeFilterPacket {
        merge_processor_packets(packets, build_rows, 0, 0, usize::MAX).unwrap()
    }

    #[test]
    fn test_merge_processor_packets_covering_all_rows() {
        let merged = merge(vec![packet(vec![1, 3, 5]), packet(vec![7, 9])], 5);
        assert!(!merged.disable_all_due_to_spill);
        assert_eq!(merged.build_rows, 5);
        assert!(merged.packets.unwrap()[&0].min_max.is_some());

        // Processors without planned filters still count their rows.
        let merged = merge(
            vec![
                JoinRuntimeFilterPacket::complete_without_filters(3),
                JoinRuntimeFilterPacket::complete_without_filters(0),
            ],
            3,
        );
        assert!(merged.packets.is_none());
        assert_eq!(merged.build_rows, 3);
    }

    #[test]
    fn test_merge_processor_packets_missing_rows() {
        // A processor left with 4 build rows and no packet.
        let local = merge(vec![packet(vec![1, 3, 5])], 7);
        assert!(local.packets.is_none());
        assert!(local.disable_all_due_to_spill);
        assert_eq!(local.build_rows, 7);

        // The packet of another node must not bring the filters back across the cluster.
        let global =
            merge_join_runtime_filter_packets(vec![local, packet(vec![1, 5])], 0, 0, usize::MAX)
                .unwrap();
        assert!(global.packets.is_none());
        assert_eq!(global.build_rows, 9);
    }
}
