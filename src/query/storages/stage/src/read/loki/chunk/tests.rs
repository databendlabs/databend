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

use super::*;

fn fixture(name: &str) -> Vec<u8> {
    std::fs::read(
        std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../../../../tests/data/loki")
            .join(name),
    )
    .unwrap()
}

#[test]
fn official_versions_and_codecs() {
    for version in 1..=4 {
        for codec in [0, 1, 3, 4, 5, 6, 7, 8, 9] {
            if version == 1 && codec != 1 {
                continue;
            }
            let bytes = fixture(&format!("chunks/v{version}-codec{codec}"));
            let chunk =
                Chunk::parse(&bytes).unwrap_or_else(|e| panic!("v{version} codec{codec}: {e}"));
            assert_eq!(chunk.tenant_id, "test-tenant");
            assert_eq!(chunk.labels["service"], "api");
            assert!(
                chunk.block_count() > 1,
                "fixture must exercise multiple blocks"
            );
            let entries: Vec<_> = (0..chunk.block_count())
                .flat_map(|i| chunk.decode_block(i).unwrap())
                .collect::<DecodeResult<_>>()
                .unwrap();
            assert_eq!(entries.len(), 3);
            for (i, entry) in entries.iter().enumerate() {
                assert_eq!(entry.timestamp_ns, 1700000000123456789 + i as i64);
                if version == 4 {
                    assert_eq!(entry.structured_metadata["trace_id"], format!("trace-{i}"));
                } else {
                    assert!(entry.structured_metadata.is_empty());
                }
            }
            assert_eq!(entries[0].message, "hello loki");
            assert_eq!(entries[1].message, "你好 loki");
            assert_eq!(entries[2].message, r#"{"status":200}"#);
        }
    }
}

#[test]
fn empty_chunk() {
    let bytes = fixture("empty");
    assert_eq!(Chunk::parse(&bytes).unwrap().block_count(), 0);
}

#[test]
fn corrupt_and_truncated_chunks() {
    assert!(
        Chunk::parse(&fixture("bad-crc"))
            .err()
            .unwrap()
            .contains("checksum")
    );
    let bytes = fixture("chunks/v4-codec1");
    for end in 0..bytes.len() {
        assert!(
            Chunk::parse(&bytes[..end]).is_err(),
            "accepted truncation at {end}"
        );
    }
    assert!(Chunk::parse(b"{\"streams\":[]}").is_err());
    assert!(Chunk::parse(&[255; 32]).is_err());
}

#[test]
fn invalid_header_lengths_and_offsets() {
    let bytes = fixture("chunks/v4-codec0");
    let metadata_end = u32::from_be_bytes(bytes[..4].try_into().unwrap()) as usize;
    let inner = &bytes[metadata_end + 4..];
    for version in [0, 5, 255] {
        let mut data = inner.to_vec();
        data[4] = version;
        assert!(Chunk::parse_inner(&data, String::new(), Map::new()).is_err());
    }
    for codec in [2, 10, 255] {
        let mut data = inner.to_vec();
        data[5] = codec;
        assert!(Chunk::parse_inner(&data, String::new(), Map::new()).is_err());
    }
    for from_end in [8, 16, 24, 32] {
        let mut data = inner.to_vec();
        let offset = data.len() - from_end;
        data[offset..offset + 8].fill(255);
        assert!(Chunk::parse_inner(&data, String::new(), Map::new()).is_err());
    }
    let mut legacy = bytes.clone();
    legacy[..4].copy_from_slice(&((metadata_end - 4) as u32).to_be_bytes());
    assert_eq!(
        Chunk::parse(&legacy).unwrap().block_count(),
        Chunk::parse(&bytes).unwrap().block_count()
    );
}

#[test]
fn varint_and_section_bounds() {
    assert!(Cursor { bytes: &[255; 10] }.uvarint().is_err());
    assert!(Cursor { bytes: &[128] }.uvarint().is_err());
    assert!(Cursor { bytes: &[4, b'a'] }.string().is_err());
    assert_eq!(Cursor { bytes: &[1] }.varint().unwrap(), -1);
    assert_eq!(Cursor { bytes: &[2] }.varint().unwrap(), 1);
    assert!(checked_section(&[0; 16], usize::MAX, 1).is_err());
    assert!(checked_section(&[0; 16], 0, usize::MAX).is_err());
}

#[test]
fn decoded_sections_above_64_mib() {
    let len = 64 * 1024 * 1024 + 1;
    let input = vec![b'a'; len];
    let mut gzip = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
    std::io::Write::write_all(&mut gzip, &input).unwrap();
    let gzip = gzip.finish().unwrap();
    let zstd = zstd::stream::encode_all(&input[..], 1).unwrap();
    for (bytes, codec) in [(&input[..], 0), (&gzip[..], 1), (&zstd[..], 9)] {
        let decoded = decompress(bytes, codec).unwrap();
        assert_eq!(decoded, input);
    }
}

#[test]
fn outer_metadata_above_1_mib() {
    let bytes = fixture("chunks/v4-codec1");
    let metadata_end = u32::from_be_bytes(bytes[..4].try_into().unwrap()) as usize;
    let metadata = read_decoded(snap::read::FrameDecoder::new(&bytes[4..metadata_end])).unwrap();
    let mut metadata: Value = serde_json::from_slice(&metadata).unwrap();
    let label = "a".repeat(1024 * 1024 + 1);
    metadata["metric"]["large_label"] = Value::String(label.clone());
    let metadata = serde_json::to_vec(&metadata).unwrap();
    let mut encoder = snap::write::FrameEncoder::new(Vec::new());
    std::io::Write::write_all(&mut encoder, &metadata).unwrap();
    let metadata = encoder.into_inner().unwrap();
    let mut envelope = Vec::new();
    envelope.extend_from_slice(&u32::try_from(metadata.len() + 4).unwrap().to_be_bytes());
    envelope.extend_from_slice(&metadata);
    envelope.extend_from_slice(&bytes[metadata_end..]);
    let chunk = Chunk::parse(&envelope).unwrap();
    assert_eq!(chunk.labels["large_label"], label);
    let entries = (0..chunk.block_count())
        .flat_map(|i| chunk.decode_block(i).unwrap())
        .collect::<DecodeResult<Vec<_>>>()
        .unwrap();
    assert_eq!(entries.len(), 3);
}

#[test]
fn malformed_entries_and_metadata() {
    let mut block = Vec::new();
    // timestamp=0, line length=0, metadata length=3, count=1, two missing symbols.
    block.extend_from_slice(&[0, 0, 3, 1, 0, 0]);
    let chunk = Chunk {
        tenant_id: String::new(),
        labels: Map::new(),
        version: 4,
        codec: 0,
        symbols: Vec::new(),
        blocks: vec![Block {
            bytes: &block,
            entries: 1,
            mint: 0,
            maxt: 0,
        }],
    };
    assert!(
        chunk
            .decode_block(0)
            .unwrap()
            .next()
            .unwrap()
            .err()
            .unwrap()
            .contains("symbol")
    );
    let chunk = Chunk {
        version: 3,
        blocks: vec![Block {
            bytes: &[0, 1, 255],
            entries: 1,
            mint: 0,
            maxt: 0,
        }],
        ..chunk
    };
    assert!(chunk.decode_block(0).unwrap().next().unwrap().is_err());
}
