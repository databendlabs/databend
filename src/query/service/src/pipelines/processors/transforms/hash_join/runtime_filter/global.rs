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

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;

use super::merge::merge_join_runtime_filter_packets;
use super::packet::JoinRuntimeFilterPacket;
use crate::sessions::QueryContext;
use crate::sessions::table_context_ext::*;

pub async fn get_global_runtime_filter_packet(
    broadcast_id: u32,
    local_packet: JoinRuntimeFilterPacket,
    ctx: &QueryContext,
) -> Result<JoinRuntimeFilterPacket> {
    let sender = ctx.broadcast_source_sender(broadcast_id);
    let receiver = ctx.broadcast_sink_receiver(broadcast_id);
    let mut received = vec![];

    sender
        .send(local_packet.try_into()?)
        .await
        .map_err(|_| ErrorCode::TokioError("send runtime filter shards failed"))?;
    sender.close();

    while let Ok(data_block) = receiver.recv().await {
        received.push(JoinRuntimeFilterPacket::try_from(data_block)?);
    }
    let settings = ctx.get_settings();
    merge_join_runtime_filter_packets(
        received,
        settings.get_inlist_runtime_filter_threshold()? as usize,
        settings.get_bloom_runtime_filter_threshold()? as usize,
        settings.get_min_max_runtime_filter_threshold()? as usize,
        settings.get_spatial_runtime_filter_threshold()? as usize,
    )
}
