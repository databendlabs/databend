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

//! Object-store chunk layout from grafana/loki v3.4.2, pkg/storage/chunk and pkg/chunkenc.
//! This is NOT the Push API JSON, an index, a WAL record, or a checkpoint.

use std::io::Read;

use serde_json::Map;
use serde_json::Value;

const MAGIC: u32 = 0x012e_e56a;
type DecodeResult<T> = std::result::Result<T, String>;

struct Cursor<'a> {
    bytes: &'a [u8],
}

impl<'a> Cursor<'a> {
    fn take(&mut self, len: usize) -> DecodeResult<&'a [u8]> {
        let value = self.bytes.get(..len).ok_or("truncated chunk section")?;
        self.bytes = &self.bytes[len..];
        Ok(value)
    }

    fn byte(&mut self) -> DecodeResult<u8> {
        Ok(self.take(1)?[0])
    }

    fn be32(&mut self) -> DecodeResult<u32> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn be64(&mut self) -> DecodeResult<u64> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }

    fn uvarint(&mut self) -> DecodeResult<u64> {
        let mut value = 0;
        for i in 0..10 {
            let byte = self.byte()?;
            if i == 9 && byte > 1 {
                return Err("overflowing varint".to_string());
            }
            value |= u64::from(byte & 0x7f) << (i * 7);
            if byte < 0x80 {
                return Ok(value);
            }
        }
        Err("overflowing varint".to_string())
    }

    fn length(&mut self) -> DecodeResult<usize> {
        usize::try_from(self.uvarint()?).map_err(|_| "length overflows usize".to_string())
    }

    // Go binary.Varint uses zigzag, including for absolute nanosecond timestamps.
    fn varint(&mut self) -> DecodeResult<i64> {
        let value = self.uvarint()?;
        Ok(((value >> 1) as i64) ^ -((value & 1) as i64))
    }

    fn string(&mut self) -> DecodeResult<String> {
        let len = self.length()?;
        String::from_utf8(self.take(len)?.to_vec()).map_err(|e| e.to_string())
    }

    fn finish(&self) -> DecodeResult<()> {
        if self.bytes.is_empty() {
            Ok(())
        } else {
            Err("unexpected trailing bytes in chunk section".to_string())
        }
    }
}

fn read_decoded(mut reader: impl Read) -> DecodeResult<Vec<u8>> {
    let mut bytes = Vec::new();
    reader
        .read_to_end(&mut bytes)
        .map_err(|e| format!("decompression failed: {e}"))?;
    Ok(bytes)
}

fn decompress(bytes: &[u8], codec: u8) -> DecodeResult<Vec<u8>> {
    match codec {
        0 => Ok(bytes.to_vec()),
        1 => read_decoded(flate2::read::MultiGzDecoder::new(bytes)),
        3 | 5 | 6 | 7 => read_decoded(lz4_flex::frame::FrameDecoder::new(bytes)),
        4 => read_decoded(snap::read::FrameDecoder::new(bytes)),
        8 => read_decoded(flate2::read::DeflateDecoder::new(bytes)),
        9 => read_decoded(zstd::stream::read::Decoder::new(bytes).map_err(|e| e.to_string())?),
        _ => Err(format!("unsupported Loki compression codec {codec}")),
    }
}

fn checked_section(bytes: &[u8], offset: usize, len: usize) -> DecodeResult<&[u8]> {
    let end = offset.checked_add(len).ok_or("section offset overflow")?;
    let checksum_end = end.checked_add(4).ok_or("checksum offset overflow")?;
    let section = bytes.get(offset..end).ok_or("section outside chunk")?;
    let checksum = bytes.get(end..checksum_end).ok_or("truncated checksum")?;
    if crc32c::crc32c(section) != u32::from_be_bytes(checksum.try_into().unwrap()) {
        return Err("Loki chunk CRC32C checksum mismatch".to_string());
    }
    Ok(section)
}

struct Block<'a> {
    bytes: &'a [u8],
    entries: usize,
    mint: i64,
    maxt: i64,
}

pub(super) struct Entry {
    pub timestamp_ns: i64,
    pub message: String,
    pub structured_metadata: Map<String, Value>,
}

pub(super) struct Chunk<'a> {
    pub tenant_id: String,
    pub labels: Map<String, Value>,
    version: u8,
    codec: u8,
    symbols: Vec<String>,
    blocks: Vec<Block<'a>>,
}

impl<'a> Chunk<'a> {
    pub fn parse(bytes: &'a [u8]) -> DecodeResult<Self> {
        let mut outer = Cursor { bytes };
        let metadata_len = outer.be32()? as usize;
        // Historical envelopes differ on whether metadata_len includes its four-byte prefix.
        for end in [Some(metadata_len), metadata_len.checked_add(4)]
            .into_iter()
            .flatten()
        {
            if end < 4 || end > bytes.len().saturating_sub(4) {
                continue;
            }
            let mut data_cursor = Cursor {
                bytes: &bytes[end..],
            };
            let data_len = data_cursor.be32()? as usize;
            if data_len != data_cursor.bytes.len() {
                continue;
            }
            let metadata = read_decoded(snap::read::FrameDecoder::new(&bytes[4..end]))?;
            let metadata: Value = serde_json::from_slice(&metadata).map_err(|e| e.to_string())?;
            let encoding = metadata.get("encoding").and_then(Value::as_u64);
            if !matches!(encoding, Some(128 | 129)) {
                return Err(format!("unsupported Loki outer encoding {encoding:?}"));
            }
            let tenant_id = metadata
                .get("userID")
                .and_then(Value::as_str)
                .ok_or("missing Loki tenant userID")?
                .to_string();
            let labels = metadata
                .get("metric")
                .and_then(Value::as_object)
                .ok_or("missing Loki stream metric labels")?
                .clone();
            if labels.values().any(|value| !value.is_string()) {
                return Err("Loki stream labels must be strings".to_string());
            }
            return Self::parse_inner(data_cursor.bytes, tenant_id, labels);
        }
        Err("not a Loki object-store chunk: invalid envelope lengths".to_string())
    }

    fn parse_inner(
        bytes: &'a [u8],
        tenant_id: String,
        labels: Map<String, Value>,
    ) -> DecodeResult<Self> {
        let mut header = Cursor { bytes };
        if header.be32()? != MAGIC {
            return Err("invalid Loki chunk magic".to_string());
        }
        let version = header.byte()?;
        let codec = match version {
            1 => 1,
            2..=4 => header.byte()?,
            _ => return Err(format!("unsupported Loki chunk version {version}")),
        };
        if !matches!(codec, 0 | 1 | 3..=9) {
            return Err(format!("unsupported Loki compression codec {codec}"));
        }
        let header_len = bytes.len() - header.bytes.len();
        let trailer_len = if version == 4 { 32 } else { 8 };
        let trailer_offset = bytes
            .len()
            .checked_sub(trailer_len)
            .ok_or("truncated trailer")?;
        let mut trailer = Cursor {
            bytes: &bytes[trailer_offset..],
        };
        let mut symbols = Vec::new();
        let mut expected_offset = header_len;
        if version == 4 {
            let len = usize::try_from(trailer.be64()?).map_err(|e| e.to_string())?;
            let offset = usize::try_from(trailer.be64()?).map_err(|e| e.to_string())?;
            if offset != header_len {
                return Err("invalid symbol table offset".to_string());
            }
            let section = checked_section(&bytes[..trailer_offset], offset, len)?;
            expected_offset = offset + len + 4;
            let mut section = Cursor { bytes: section };
            let count = section.length()?;
            let decoded = decompress(section.bytes, codec)?;
            if count > decoded.len() {
                return Err("symbol count exceeds section size".to_string());
            }
            let mut decoded = Cursor { bytes: &decoded };
            for _ in 0..count {
                symbols.push(decoded.string()?);
            }
            decoded.finish()?;
        }
        let metadata_len = if version == 4 {
            Some(usize::try_from(trailer.be64()?).map_err(|e| e.to_string())?)
        } else {
            None
        };
        let metadata_offset = usize::try_from(trailer.be64()?).map_err(|e| e.to_string())?;
        let metadata_len = match metadata_len {
            Some(len) => len,
            None => trailer_offset
                .checked_sub(4)
                .and_then(|end| end.checked_sub(metadata_offset))
                .ok_or("invalid block metadata offset")?,
        };
        if metadata_offset
            .checked_add(metadata_len)
            .and_then(|v| v.checked_add(4))
            != Some(trailer_offset)
        {
            return Err("invalid block metadata length".to_string());
        }
        let mut metas = Cursor {
            bytes: checked_section(&bytes[..trailer_offset], metadata_offset, metadata_len)?,
        };
        let count = metas.length()?;
        if count > metas.bytes.len() / 5 {
            return Err("block count exceeds metadata size".to_string());
        }
        let mut blocks = Vec::new();
        for _ in 0..count {
            let entries = metas.length()?;
            let mint = metas.varint()?;
            let maxt = metas.varint()?;
            let offset = metas.length()?;
            if version >= 3 {
                // Loki stores a logical size estimate, not the encoded varint byte length.
                let _uncompressed_size = metas.length()?;
            }
            let len = metas.length()?;
            if mint > maxt || offset != expected_offset {
                return Err("invalid Loki block bounds or offset".to_string());
            }
            let block = checked_section(&bytes[..metadata_offset], offset, len)?;
            expected_offset = offset + len + 4;
            blocks.push(Block {
                bytes: block,
                entries,
                mint,
                maxt,
            });
        }
        if expected_offset != metadata_offset {
            return Err("unexpected bytes between blocks and metadata".to_string());
        }
        metas.finish()?;
        Ok(Self {
            tenant_id,
            labels,
            version,
            codec,
            symbols,
            blocks,
        })
    }

    pub fn block_count(&self) -> usize {
        self.blocks.len()
    }

    pub fn decode_block(
        &self,
        index: usize,
    ) -> DecodeResult<impl Iterator<Item = DecodeResult<Entry>> + '_> {
        let block = &self.blocks[index];
        let decoded = decompress(block.bytes, self.codec)?;
        if block.entries > decoded.len() / 2 {
            return Err("entry count exceeds block size".to_string());
        }
        let mut remaining = block.entries;
        let mut position = 0;
        let mut done = false;
        Ok(std::iter::from_fn(move || {
            if done {
                return None;
            }
            let mut cursor = Cursor {
                bytes: &decoded[position..],
            };
            if remaining == 0 {
                done = true;
                return cursor.finish().err().map(Err);
            }
            let result = (|| {
                let timestamp_ns = cursor.varint()?;
                if timestamp_ns < block.mint || timestamp_ns > block.maxt {
                    return Err("entry timestamp outside block bounds".to_string());
                }
                let message = cursor.string()?;
                let mut structured_metadata = Map::new();
                if self.version == 4 {
                    let len = cursor.length()?;
                    let mut metadata = Cursor {
                        bytes: cursor.take(len)?,
                    };
                    let count = metadata.length()?;
                    if count > metadata.bytes.len() / 2 {
                        return Err("structured metadata count exceeds section size".to_string());
                    }
                    for _ in 0..count {
                        let name = metadata.length()?;
                        let value = metadata.length()?;
                        let name = self
                            .symbols
                            .get(name)
                            .ok_or("invalid metadata name symbol")?;
                        let value = self
                            .symbols
                            .get(value)
                            .ok_or("invalid metadata value symbol")?;
                        if structured_metadata
                            .insert(name.clone(), Value::String(value.clone()))
                            .is_some()
                        {
                            return Err("duplicate structured metadata name".to_string());
                        }
                    }
                    metadata.finish()?;
                }
                Ok(Entry {
                    timestamp_ns,
                    message,
                    structured_metadata,
                })
            })();
            remaining -= 1;
            position = decoded.len() - cursor.bytes.len();
            done = result.is_err();
            Some(result)
        }))
    }
}

#[cfg(test)]
mod tests;
