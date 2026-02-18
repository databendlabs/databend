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

//! Transparent zstd compression for protobuf-encoded meta values.
//!
//! Wire format (4-byte header):
//! ```text
//! [0x0F] [flags1] [flags2] [flags3] [payload]
//! ```
//! - Byte 0 = `0x0F`: sentinel (protobuf tag=1, wire-type=7, permanently invalid).
//! - Bytes 1–3: feature flag bits; all reserved bits must be zero.
//!
//! | Byte | Bit | Meaning |
//! |------|-----|---------|
//! | 1    | 0   | `FLAG_ZSTD` — payload is zstd-compressed |
//!
//! Bytes without `0x0F` at position 0 are raw protobuf (legacy, decoded as-is).

use std::borrow::Cow;
use std::io;

const MAGIC: u8 = 0x0F;
const HEADER_LEN: usize = 4;
const FLAG_ZSTD: u8 = 0x01;
pub const COMPRESS_THRESHOLD: usize = 4096;

/// Optionally compress `buf` with zstd.
///
/// Returns `buf` unchanged if `buf.len() < COMPRESS_THRESHOLD`.
/// Otherwise prepends `[0x0F, FLAG_ZSTD, 0x00, 0x00]` and returns the compressed payload.
/// Falls back to returning `buf` uncompressed on compression error.
pub fn encode_value(buf: Vec<u8>) -> Vec<u8> {
    if buf.len() < COMPRESS_THRESHOLD {
        return buf;
    }

    match zstd::encode_all(buf.as_slice(), 3) {
        Ok(compressed) => {
            let mut out = Vec::with_capacity(HEADER_LEN + compressed.len());
            out.extend_from_slice(&[MAGIC, FLAG_ZSTD, 0x00, 0x00]);
            out.extend_from_slice(&compressed);
            out
        }
        Err(_) => buf,
    }
}

/// Decode a value produced by [`encode_value`] or a legacy raw protobuf value.
///
/// Returns `Cow::Borrowed` for legacy/uncompressed data (zero copy),
/// and `Cow::Owned` for decompressed data.
/// Returns an error if the header has unknown flag bits set (forward-compat guard).
pub fn decode_value(buf: &[u8]) -> Result<Cow<'_, [u8]>, io::Error> {
    if buf.is_empty() || buf[0] != MAGIC {
        return Ok(Cow::Borrowed(buf));
    }

    if buf.len() < HEADER_LEN {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "custom-encoded value too short to contain 4-byte header",
        ));
    }

    let flag1 = buf[1];
    let flag2 = buf[2];
    let flag3 = buf[3];

    let unknown = (flag1 & !FLAG_ZSTD) | flag2 | flag3;
    if unknown != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "unknown flags in custom-encoded value header: [{:#04x}, {:#04x}, {:#04x}]",
                flag1, flag2, flag3
            ),
        ));
    }

    let payload = &buf[HEADER_LEN..];

    if flag1 & FLAG_ZSTD != 0 {
        let decompressed = zstd::decode_all(payload)?;
        return Ok(Cow::Owned(decompressed));
    }

    // No flags set: uncompressed custom format (reserved, not emitted by encode_value).
    Ok(Cow::Borrowed(payload))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_small_passthrough() {
        let data = b"small protobuf data".to_vec();
        let encoded = encode_value(data.clone());
        assert_eq!(encoded, data, "small values must not get a header");
    }

    #[test]
    fn test_large_has_header() {
        let data = vec![b'a'; COMPRESS_THRESHOLD + 1];
        let encoded = encode_value(data);
        assert_eq!(
            &encoded[..4],
            &[MAGIC, FLAG_ZSTD, 0x00, 0x00],
            "large values must start with the zstd header"
        );
    }

    #[test]
    fn test_round_trip_small() {
        let data = b"hello protobuf".to_vec();
        let encoded = encode_value(data.clone());
        let decoded = decode_value(&encoded).unwrap();
        assert_eq!(decoded.as_ref(), data.as_slice());
    }

    #[test]
    fn test_round_trip_large() {
        let data = vec![b'z'; COMPRESS_THRESHOLD + 100];
        let encoded = encode_value(data.clone());
        let decoded = decode_value(&encoded).unwrap();
        assert_eq!(decoded.as_ref(), data.as_slice());
    }

    #[test]
    fn test_legacy_raw_protobuf_zero_copy() {
        // bytes not starting with MAGIC are legacy raw protobuf → Cow::Borrowed
        let data = b"\x0a\x05hello".to_vec();
        let result = decode_value(&data).unwrap();
        match result {
            Cow::Borrowed(b) => assert_eq!(b, data.as_slice()),
            Cow::Owned(_) => panic!("expected Cow::Borrowed for legacy data"),
        }
    }

    #[test]
    fn test_unknown_flags_error() {
        // bit 1 of flag byte 1 is unknown
        let buf = vec![MAGIC, 0x02, 0x00, 0x00, 0x00];
        assert!(decode_value(&buf).is_err());
    }

    #[test]
    fn test_unknown_flags_error_byte2() {
        let buf = vec![MAGIC, 0x00, 0x01, 0x00, 0x00];
        assert!(decode_value(&buf).is_err());
    }
}
