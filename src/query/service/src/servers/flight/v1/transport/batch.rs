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

//! Wire format for merging several same-thread payloads into one `FlightData`.
//!
//! Both transports batch on the sending side and split on the receiving side, so the codec lives
//! here rather than in either transport. A batch carries its thread id once in `app_metadata` and
//! concatenates the original items into `data_body`:
//!
//! ```text
//! app_metadata: [tid: u16 le][item_count: u16 le][BATCH_MARKER]
//! data_body:    ([meta_len: u32 le][meta][header_len: u32 le][header][body_len: u32 le][body])*
//! ```
//!
//! Each item's `app_metadata` is stored without its 2-byte tid prefix, which `split` restores.

use arrow_flight::FlightData;
use bytes::Buf;
use bytes::BufMut;
use bytes::Bytes;
use bytes::BytesMut;

/// Trailing `app_metadata` byte marking a merged batch. Distinct from the fragment (0x01) and
/// dictionary (0x05) markers a single payload carries.
pub const BATCH_MARKER: u8 = 0x02;

const TID_LEN: usize = 2;
const BATCH_HEADER_LEN: usize = 5;
/// Per-item overhead in `data_body`: three u32 length prefixes.
const ITEM_LENGTH_PREFIXES: usize = 12;

/// Detects a merged batch by its trailing marker.
pub fn is_batch(data: &FlightData) -> bool {
    data.app_metadata.len() >= BATCH_HEADER_LEN
        && data.app_metadata[data.app_metadata.len() - 1] == BATCH_MARKER
}

/// Merges same-thread items into one batch payload.
///
/// The thread id is taken from the first item; callers must only merge items sharing a tid.
/// Panics if `items` is empty or its first item has no tid prefix.
pub fn merge(items: Vec<FlightData>) -> FlightData {
    let mut app_metadata = BytesMut::with_capacity(BATCH_HEADER_LEN);
    app_metadata.put_slice(&items[0].app_metadata[..TID_LEN]);
    app_metadata.put_u16_le(items.len() as u16);
    app_metadata.put_u8(BATCH_MARKER);

    let estimated = items
        .iter()
        .map(|item| {
            ITEM_LENGTH_PREFIXES
                + (item.app_metadata.len() - TID_LEN)
                + item.data_header.len()
                + item.data_body.len()
        })
        .sum();

    let mut body = BytesMut::with_capacity(estimated);
    for item in items {
        let metadata = &item.app_metadata[TID_LEN..];
        body.put_u32_le(metadata.len() as u32);
        body.put_slice(metadata);
        body.put_u32_le(item.data_header.len() as u32);
        body.put_slice(&item.data_header);
        body.put_u32_le(item.data_body.len() as u32);
        body.put_slice(&item.data_body);
    }

    FlightData {
        flight_descriptor: None,
        app_metadata: app_metadata.freeze(),
        data_header: Bytes::new(),
        data_body: body.freeze(),
    }
}

/// Splits a batch back into its individual items, restoring each tid prefix.
///
/// `data_header` and `data_body` are sliced without copying; only the small `app_metadata` is
/// rebuilt. Callers must check [`is_batch`] first.
pub fn split(data: FlightData) -> Vec<FlightData> {
    let meta = &data.app_metadata;
    let tid_bytes: [u8; TID_LEN] = [meta[0], meta[1]];
    let num_items = u16::from_le_bytes([meta[2], meta[3]]) as usize;

    let mut buf = data.data_body;
    let mut items = Vec::with_capacity(num_items);

    for _ in 0..num_items {
        let meta_len = buf.get_u32_le() as usize;
        let inner_meta = buf.split_to(meta_len);

        let header_len = buf.get_u32_le() as usize;
        let data_header = buf.split_to(header_len);

        let body_len = buf.get_u32_le() as usize;
        let data_body = buf.split_to(body_len);

        let mut app_metadata = BytesMut::with_capacity(TID_LEN + meta_len);
        app_metadata.put_slice(&tid_bytes);
        app_metadata.extend_from_slice(&inner_meta);

        items.push(FlightData {
            flight_descriptor: None,
            app_metadata: app_metadata.freeze(),
            data_header,
            data_body,
        });
    }

    items
}
