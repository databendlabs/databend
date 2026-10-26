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

//! One immutable object per table vector index: independently addressable components,
//! footer directory, fixed trailer. No Parquet schema, rows or column chunks.

use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::Arc;

use arrow_buffer::buffer::NullBuffer;
use bytes::Bytes;
use databend_common_column::bitmap::Bitmap;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::Column;
use half::f16;
use half::slice::HalfFloatSliceExt;
use serde::Deserialize;
use serde::Serialize;

use crate::IndexMeta;

/// Name suffixes of the per-column validity and FP16 components.
pub const VECTOR_VALIDITY_SUFFIX: &str = "_validity";
pub const VECTOR_FP16_DATA_SUFFIX: &str = "_fp16_data";

/// Components are identified by name; these names are part of the file format.
/// HNSW components use `hnsw_component_name`.
pub fn vector_validity_name(column_id: u32) -> String {
    format!("{column_id}{VECTOR_VALIDITY_SUFFIX}")
}

pub fn vector_fp16_data_name(column_id: u32) -> String {
    format!("{column_id}{VECTOR_FP16_DATA_SUFFIX}")
}
/// Values converted per step when writing FP16 data (16 KiB of FP16 on the stack).
const FP16_CONVERT_CHUNK: usize = 8192;

pub const VECTOR_BUNDLE_VERSION: u64 = 1;
pub const VECTOR_BUNDLE_TRAILER_SIZE: usize = 32;
pub const VECTOR_BUNDLE_MAX_FOOTER_SIZE: usize = 16 * 1024 * 1024;
/// Bytes read from the end of a bundle on a footer cache miss. A footer is a few
/// KiB per indexed column, so one read usually returns footer and trailer
/// together; a larger footer is read with a second request.
pub const VECTOR_BUNDLE_INITIAL_FOOTER_READ_SIZE: usize = 64 * 1024;
const MAGIC: &[u8; 8] = b"DBVINDEX";
const MAX_COMPONENT_SIZE: u64 = 1024 * 1024 * 1024;
/// ZSTD level for HNSW components. Level 1 matches the legacy Parquet default and,
/// for SQ8 data (few repeats, skewed bytes), compresses better than higher levels:
/// ~43% vs ~50% at level 3, and several times faster.
const HNSW_ZSTD_LEVEL: i32 = 1;

/// Codec of a component, persisted in its entry; readers only use the persisted
/// value. HNSW components are compressed (`VectorBundleBuilder::add_hnsw`); validity
/// and FP16 refine data are plain, so FP16 pages can be read by range.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VectorBundleCodec {
    Plain,
    Zstd,
}

impl VectorBundleCodec {
    /// Plain data is returned as is (no copy); compressed data is decompressed.
    fn decode(self, bytes: Bytes, raw_length: usize) -> Result<Bytes> {
        let decoded = match self {
            Self::Plain => return Ok(bytes),
            Self::Zstd => zstd::bulk::decompress(&bytes, raw_length)
                .map_err(|error| invalid(&format!("Invalid ZSTD vector component: {error}")))?,
        };
        Ok(Bytes::from(decoded))
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VectorBundleEntry {
    pub offset: u64,
    pub length: u64,
    /// Decoded size; equals `length` for plain components.
    pub raw_length: u64,
    pub codec: VectorBundleCodec,
    /// CRC32 of the decoded bytes. Not verified on FP16 page reads.
    pub checksum: u32,
    /// Only for FP16: plain fixed-width rows of `dimension * 2` bytes, read in pages
    /// of this many rows. The dimension follows from `raw_length` and the row count.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub rows_per_page: Option<u32>,
}

impl VectorBundleEntry {
    /// Entry of a non-paged component stored at `offset..offset + length`, whose
    /// decoded bytes are `raw`.
    fn new(offset: usize, length: usize, raw: &[u8], codec: VectorBundleCodec) -> Self {
        Self {
            offset: offset as u64,
            length: length as u64,
            raw_length: raw.len() as u64,
            codec,
            checksum: crc32fast::hash(raw),
            rows_per_page: None,
        }
    }

    pub fn range(&self) -> Result<Range<u64>> {
        let end = self
            .offset
            .checked_add(self.length)
            .ok_or_else(|| invalid("Component range overflow"))?;
        Ok(self.offset..end)
    }

    /// Read descriptor for the whole component, verified against its checksum.
    pub fn component_read(&self) -> VectorBundlePageRead {
        VectorBundlePageRead {
            offset: self.offset,
            length: self.length,
            raw_length: self.raw_length,
            codec: self.codec,
            checksum: Some(self.checksum),
        }
    }

    /// Rows per page of a paged (plain FP16) component. Page reads rely on this:
    /// they compute positions from it and read the bytes as plain FP16.
    pub fn rows_per_page(&self) -> Result<u32> {
        self.rows_per_page
            .filter(|rows| *rows != 0 && self.codec == VectorBundleCodec::Plain)
            .ok_or_else(|| invalid("Vector component is not plain paged FP16 data"))
    }

    /// FP16 dimension: `raw_length / (row_count * 2)`. Not defined for an empty block.
    pub fn dimension(&self, row_count: u64) -> Result<u32> {
        let row_bytes = row_count
            .checked_mul(2)
            .filter(|bytes| *bytes != 0)
            .ok_or_else(|| invalid("FP16 dimension of an empty block"))?;
        if !self.raw_length.is_multiple_of(row_bytes) {
            return Err(invalid("FP16 component size is not a whole number of rows"));
        }
        u32::try_from(self.raw_length / row_bytes)
            .ok()
            .filter(|dimension| *dimension != 0)
            .ok_or_else(|| invalid("Invalid FP16 dimension"))
    }

    pub fn page_row_range(&self, page: usize, total_rows: u64) -> Result<Range<u64>> {
        let rows_per_page = u64::from(self.rows_per_page()?);
        let start = (page as u64)
            .checked_mul(rows_per_page)
            .filter(|start| *start < total_rows)
            .ok_or_else(|| invalid("FP16 page ordinal out of bounds"))?;
        Ok(start..start.saturating_add(rows_per_page).min(total_rows))
    }

    /// Read descriptor for one page: `offset + row_start * row_bytes`, with the
    /// length of its rows. Plain bytes, no per-page checksum.
    pub fn page_read(&self, page: usize, total_rows: u64) -> Result<VectorBundlePageRead> {
        let rows = self.page_row_range(page, total_rows)?;
        let row_bytes = u64::from(self.dimension(total_rows)?) * 2;
        let start = rows.start * row_bytes;
        let length = (rows.end - rows.start) * row_bytes;
        if start + length > self.length {
            return Err(invalid("FP16 page range outside component"));
        }
        Ok(VectorBundlePageRead {
            offset: self
                .offset
                .checked_add(start)
                .ok_or_else(|| invalid("FP16 page offset overflow"))?,
            length,
            raw_length: length,
            codec: VectorBundleCodec::Plain,
            checksum: None,
        })
    }
}

/// Byte range plus decoding information for one read (a whole component or a page).
#[derive(Clone, Debug)]
pub struct VectorBundlePageRead {
    pub offset: u64,
    pub length: u64,
    pub raw_length: u64,
    pub codec: VectorBundleCodec,
    /// CRC32 of the decoded bytes; `None` for FP16 pages.
    pub checksum: Option<u32>,
}

impl VectorBundlePageRead {
    pub fn range(&self) -> Result<Range<u64>> {
        let end = self
            .offset
            .checked_add(self.length)
            .ok_or_else(|| invalid("Vector read range overflow"))?;
        Ok(self.offset..end)
    }

    /// Decodes `bytes` read from `range()`. The decoded size (and checksum, if any)
    /// is verified, which also rejects bytes of the wrong length.
    pub fn decode(&self, bytes: Bytes) -> Result<Bytes> {
        // Bounds the decompression buffer.
        if self.raw_length > MAX_COMPONENT_SIZE {
            return Err(invalid("Vector component exceeds size limit"));
        }
        let decoded = self.codec.decode(bytes, self.raw_length as usize)?;
        if decoded.len() as u64 != self.raw_length
            || self
                .checksum
                .is_some_and(|checksum| crc32fast::hash(&decoded) != checksum)
        {
            return Err(invalid("Vector component checksum or size mismatch"));
        }
        Ok(decoded)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct VectorBundleFooter {
    pub index_name: String,
    pub index_version: String,
    pub row_count: u64,
    pub components: BTreeMap<String, VectorBundleEntry>,
}

impl VectorBundleFooter {
    pub fn new(index_name: String, index_version: String, row_count: u64) -> Self {
        Self {
            index_name,
            index_version,
            row_count,
            components: BTreeMap::new(),
        }
    }

    /// Decodes the footer at `trailer.footer`; `bytes` are those footer bytes.
    pub fn decode(bytes: &[u8], trailer: &VectorBundleTrailer) -> Result<Self> {
        if crc32fast::hash(bytes) != trailer.checksum {
            return Err(invalid("Vector footer checksum mismatch"));
        }
        let footer: Self = serde_json::from_slice(bytes)
            .map_err(|error| invalid(&format!("Invalid vector footer: {error}")))?;
        footer.validate(trailer.footer.start)?;
        Ok(footer)
    }

    /// Decodes the footer of a complete bundle held in memory.
    pub fn from_bundle(bundle: &[u8]) -> Result<Self> {
        let size = bundle.len();
        let trailer_start = size
            .checked_sub(VECTOR_BUNDLE_TRAILER_SIZE)
            .ok_or_else(|| invalid("Truncated vector bundle"))?;
        let trailer = VectorBundleTrailer::parse(&bundle[trailer_start..], size as u64)?;
        let footer = &bundle[trailer.footer.start as usize..trailer.footer.end as usize];
        Self::decode(footer, &trailer)
    }

    /// Every component must lie before the footer, so no read goes past the data.
    /// Component contents are checked when decoded (sizes, checksums) or used (FP16
    /// page layout, validity length).
    fn validate(&self, footer_start: u64) -> Result<()> {
        for (name, entry) in &self.components {
            if entry.range()?.end > footer_start {
                return Err(invalid(&format!("Vector component {name} out of bounds")));
            }
        }
        Ok(())
    }
}

/// Parsed metadata of a vector index file, cached in `VectorIndexMetaCache`.
#[derive(Clone)]
pub enum VectorIndexMeta {
    /// Legacy combined Parquet index file (all vector indexes of a block).
    Legacy(IndexMeta),
    /// Vector bundle file (one index per file).
    Bundle(Arc<VectorBundleFooter>),
}

// Disk-tier codec. Legacy metas retain their bincode encoding; bundle footers use
// the same JSON as in the file. Until the cache has an explicit format tag, try
// JSON first, then bincode. Invalid payloads must return errors to invalidate the
// disk entry rather than panic during either attempt.
impl TryFrom<&VectorIndexMeta> for Vec<u8> {
    type Error = ErrorCode;

    fn try_from(value: &VectorIndexMeta) -> Result<Self> {
        match value {
            VectorIndexMeta::Legacy(meta) => Vec::<u8>::try_from(meta),
            VectorIndexMeta::Bundle(footer) => Ok(serde_json::to_vec(footer.as_ref())?),
        }
    }
}

impl TryFrom<Bytes> for VectorIndexMeta {
    type Error = ErrorCode;

    fn try_from(value: Bytes) -> Result<Self> {
        match serde_json::from_slice::<VectorBundleFooter>(&value) {
            Ok(footer) => Ok(Self::Bundle(Arc::new(footer))),
            Err(_) => Ok(Self::Legacy(IndexMeta::try_from(value)?)),
        }
    }
}

/// The fixed-size bundle trailer: where the footer is and its checksum.
pub struct VectorBundleTrailer {
    pub footer: Range<u64>,
    checksum: u32,
}

impl VectorBundleTrailer {
    /// Parses the last `VECTOR_BUNDLE_TRAILER_SIZE` bytes of a bundle of
    /// `object_size` bytes.
    pub fn parse(trailer: &[u8], object_size: u64) -> Result<Self> {
        if trailer.len() != VECTOR_BUNDLE_TRAILER_SIZE || &trailer[24..32] != MAGIC {
            return Err(invalid("Invalid vector bundle trailer"));
        }
        let read_u64 = |at: usize| u64::from_le_bytes(trailer[at..at + 8].try_into().unwrap());
        let read_u32 = |at: usize| u32::from_le_bytes(trailer[at..at + 4].try_into().unwrap());
        if u64::from(read_u32(16)) != VECTOR_BUNDLE_VERSION {
            return Err(invalid("Unsupported vector bundle version"));
        }
        let (offset, length) = (read_u64(0), read_u64(8));
        // The footer ends where the trailer starts.
        let end = object_size
            .checked_sub(VECTOR_BUNDLE_TRAILER_SIZE as u64)
            .ok_or_else(|| invalid("Truncated vector bundle"))?;
        if length == 0
            || length > VECTOR_BUNDLE_MAX_FOOTER_SIZE as u64
            || offset.checked_add(length) != Some(end)
        {
            return Err(invalid("Invalid vector footer range"));
        }
        Ok(Self {
            footer: offset..end,
            checksum: read_u32(20),
        })
    }
}

pub struct VectorBundleBuilder {
    bytes: Vec<u8>,
    footer: VectorBundleFooter,
    // ZSTD context shared by the compressed components of the file.
    zstd: Option<zstd::bulk::Compressor<'static>>,
}

impl VectorBundleBuilder {
    pub fn new(index_name: String, index_version: String, row_count: u64) -> Self {
        Self {
            bytes: Vec::new(),
            footer: VectorBundleFooter::new(index_name, index_version, row_count),
            zstd: None,
        }
    }

    /// ZSTD-compressed HNSW component. HNSW components are decoded once when the
    /// index is loaded into the object cache, so compression only costs on misses.
    /// A component that does not shrink (e.g. the tiny SQ8 meta) is stored plain.
    pub fn add_hnsw(&mut self, name: String, raw: &[u8]) -> Result<()> {
        let start = self.begin(&name, raw.len())?;
        let zstd = &mut self.zstd;
        let compressor = match zstd {
            Some(compressor) => compressor,
            None => zstd.insert(
                zstd::bulk::Compressor::new(HNSW_ZSTD_LEVEL)
                    .map_err(|error| invalid(&format!("ZSTD context creation failed: {error}")))?,
            ),
        };
        // Compress straight into the file buffer: a cursor positioned at the end
        // appends into the reserved capacity.
        self.bytes
            .reserve(zstd::zstd_safe::compress_bound(raw.len()));
        let mut cursor = std::io::Cursor::new(&mut self.bytes);
        cursor.set_position(start as u64);
        compressor
            .compress_to_buffer(raw, &mut cursor)
            .map_err(|error| invalid(&format!("ZSTD compression failed: {error}")))?;
        if self.bytes.len() - start >= raw.len() {
            self.bytes.truncate(start);
            return self.add_plain(name, raw);
        }
        let entry = VectorBundleEntry::new(
            start,
            self.bytes.len() - start,
            raw,
            VectorBundleCodec::Zstd,
        );
        self.footer.components.insert(name, entry);
        Ok(())
    }

    /// The column bitmap, written only when the column has NULLs. `Bitmap ->
    /// NullBuffer` shares the bytes; `sliced()` realigns a nonzero bit offset
    /// (zero-copy when byte-aligned). Bits after `row_count` are not cleared:
    /// readers never access them.
    pub fn add_validity(&mut self, column_id: u32, bitmap: &Bitmap) -> Result<()> {
        let bytes = NullBuffer::from(bitmap.clone()).into_inner().sliced();
        self.add_plain(vector_validity_name(column_id), bytes.as_slice())
    }

    /// The rows of a `Vector(Float32)` column (nullable or not) as plain
    /// fixed-width little-endian FP16. NULL slots are converted like any other row;
    /// they are never scored. Values beyond the FP16 range become +-Inf and score
    /// as the sentinel. `rows_per_page` is only the read granularity.
    pub fn add_fp16_data(
        &mut self,
        column_id: u32,
        column: &Column,
        rows_per_page: usize,
    ) -> Result<()> {
        let column = column
            .as_nullable()
            .map_or(column, |nullable| &nullable.column);
        let (values, dimension) = column
            .as_vector()
            .and_then(|vector| vector.as_float32())
            .ok_or_else(|| invalid("FP16 refine requires Vector(Float32)"))?;
        // Page size limits are validated with the index options; the format only
        // needs a nonzero `u32` page size.
        if *dimension == 0
            || column.len() as u64 != self.footer.row_count
            || rows_per_page == 0
            || rows_per_page > u32::MAX as usize
        {
            return Err(invalid("Invalid FP16 component"));
        }
        let name = vector_fp16_data_name(column_id);
        let start = self.begin(&name, values.len() * 2)?;
        // `OrderedFloat<f32>` is `repr(transparent)`; its `Pod` impl (ordered-float's
        // `bytemuck` feature) gives a zero-copy `&[f32]` view.
        let values: &[f32] = bytemuck::cast_slice(values.as_ref());
        self.bytes.reserve(values.len() * 2);
        // The file buffer is at an arbitrary byte offset, so convert through a small
        // aligned scratch buffer and append its bytes.
        let mut scratch = [f16::ZERO; FP16_CONVERT_CHUNK];
        for chunk in values.chunks(FP16_CONVERT_CHUNK) {
            let out = &mut scratch[..chunk.len()];
            out.convert_from_f32_slice(chunk);
            // The stored format is little-endian. `to_le` is the identity on
            // little-endian targets, so this loop compiles away there.
            let bits: &mut [u16] = out.reinterpret_cast_mut();
            for value in bits.iter_mut() {
                *value = value.to_le();
            }
            self.bytes.extend_from_slice(bytemuck::cast_slice(bits));
        }
        let raw = &self.bytes[start..];
        let entry = VectorBundleEntry {
            rows_per_page: Some(rows_per_page as u32),
            ..VectorBundleEntry::new(start, raw.len(), raw, VectorBundleCodec::Plain)
        };
        self.footer.components.insert(name, entry);
        Ok(())
    }

    pub fn finish(mut self) -> Result<Vec<u8>> {
        // Components were appended in order, so they lie before the footer.
        let offset = self.bytes.len() as u64;
        let footer = serde_json::to_vec(&self.footer)?;
        if footer.len() > VECTOR_BUNDLE_MAX_FOOTER_SIZE {
            return Err(invalid("Vector footer too large"));
        }
        self.bytes.extend_from_slice(&footer);
        self.bytes.extend_from_slice(&offset.to_le_bytes());
        self.bytes
            .extend_from_slice(&(footer.len() as u64).to_le_bytes());
        self.bytes
            .extend_from_slice(&(VECTOR_BUNDLE_VERSION as u32).to_le_bytes());
        self.bytes
            .extend_from_slice(&crc32fast::hash(&footer).to_le_bytes());
        self.bytes.extend_from_slice(MAGIC);
        Ok(self.bytes)
    }

    fn add_plain(&mut self, name: String, raw: &[u8]) -> Result<()> {
        let start = self.begin(&name, raw.len())?;
        self.bytes.extend_from_slice(raw);
        let entry = VectorBundleEntry::new(start, raw.len(), raw, VectorBundleCodec::Plain);
        self.footer.components.insert(name, entry);
        Ok(())
    }

    /// Checks that `name` is new and `raw_length` within limits, and returns the
    /// offset the component starts at.
    fn begin(&self, name: &str, raw_length: usize) -> Result<usize> {
        if self.footer.components.contains_key(name) {
            return Err(invalid(&format!("Duplicate vector component {name}")));
        }
        if raw_length as u64 > MAX_COMPONENT_SIZE {
            return Err(invalid(&format!(
                "Vector component {name} exceeds size limit"
            )));
        }
        Ok(self.bytes.len())
    }
}

fn invalid(message: &str) -> ErrorCode {
    ErrorCode::StorageOther(message.to_owned())
}

#[cfg(test)]
mod tests {
    use databend_common_expression::types::F32;
    use databend_common_expression::types::VectorColumn;

    use super::*;

    const DIM: usize = 4;
    const ROWS: usize = 100;

    /// Every 10th row is NULL.
    fn column() -> Column {
        let values = (0..ROWS * DIM)
            .map(|i| F32::from(i as f32 * 0.25))
            .collect::<Vec<_>>();
        Column::Vector(VectorColumn::Float32((values.into(), DIM)))
            .wrap_nullable(Some((0..ROWS).map(|row| row % 10 != 0).collect()))
    }

    fn read(bytes: &[u8], read: VectorBundlePageRead) -> Result<Bytes> {
        let range = read.range()?;
        read.decode(Bytes::copy_from_slice(
            &bytes[range.start as usize..range.end as usize],
        ))
    }

    #[test]
    fn test_bundle_round_trip() -> Result<()> {
        let column = column();
        let sq8 = vec![7u8; 1000];
        let mut builder = VectorBundleBuilder::new("vi".to_owned(), "v1".to_owned(), ROWS as u64);
        builder.add_hnsw("0-l2_graph_links".to_owned(), b"links")?;
        builder.add_hnsw("0-l2_encoded_u8_data".to_owned(), &sq8)?;
        builder.add_validity(0, &column.as_nullable().unwrap().validity)?;
        builder.add_fp16_data(0, &column, 64)?;
        let bytes = builder.finish()?;

        let footer = VectorBundleFooter::from_bundle(&bytes)?;
        assert_eq!(footer.index_name, "vi");
        assert_eq!(footer.index_version, "v1");
        assert_eq!(footer.row_count, ROWS as u64);

        // Incompressible components fall back to plain.
        let links = &footer.components["0-l2_graph_links"];
        assert_eq!(links.codec, VectorBundleCodec::Plain);
        assert_eq!(&read(&bytes, links.component_read())?[..], b"links");
        let data = &footer.components["0-l2_encoded_u8_data"];
        assert_eq!(data.codec, VectorBundleCodec::Zstd);
        assert!(data.length < data.raw_length);
        assert_eq!(&read(&bytes, data.component_read())?[..], sq8.as_slice());

        // Components are found by name.
        let validity = &footer.components[&vector_validity_name(0)];
        let validity = read(&bytes, validity.component_read())?;
        assert_eq!(validity.len(), ROWS.div_ceil(8));
        // Row 0 is NULL, row 1 is valid.
        assert_eq!(validity[0] & 0b11, 0b10);

        // Plain fixed-width FP16 rows; pages are located by row number.
        let fp16 = &footer.components[&vector_fp16_data_name(0)];
        assert!(!footer.components.contains_key(&vector_fp16_data_name(1)));
        assert_eq!(fp16.dimension(ROWS as u64)?, DIM as u32);
        assert_eq!(fp16.page_row_range(1, ROWS as u64)?, 64..100);
        let page = read(&bytes, fp16.page_read(1, ROWS as u64)?)?;
        assert_eq!(page.len(), 36 * DIM * 2);
        let first = f16::from_le_bytes([page[0], page[1]]).to_f32();
        assert_eq!(first, (64 * DIM) as f32 * 0.25);
        assert!(fp16.page_read(2, ROWS as u64).is_err());

        // The disk-tier codec of the meta cache keeps a bundle footer a bundle.
        let meta = VectorIndexMeta::Bundle(Arc::new(footer));
        let encoded = Vec::<u8>::try_from(&meta)?;
        assert!(matches!(
            VectorIndexMeta::try_from(Bytes::from(encoded))?,
            VectorIndexMeta::Bundle(footer) if footer.row_count == ROWS as u64
        ));
        Ok(())
    }

    #[test]
    fn test_bundle_rejects_invalid_data() -> Result<()> {
        // Invalid JSON falls back to a complete empty bincode meta plus one byte.
        // The trailing byte must return an error, not panic on the decoded length.
        assert!(VectorIndexMeta::try_from(Bytes::from_static(&[0, 0, 0])).is_err());

        let column = column();
        let mut builder = VectorBundleBuilder::new("vi".to_owned(), "v1".to_owned(), ROWS as u64);
        builder.add_hnsw("a".to_owned(), b"x")?;
        assert!(builder.add_hnsw("a".to_owned(), b"y").is_err());
        let bytes = builder.finish()?;

        // The footer is covered by its checksum, the trailer by the magic.
        let footer_end = bytes.len() - VECTOR_BUNDLE_TRAILER_SIZE;
        let mut corrupted = bytes.clone();
        corrupted[footer_end - 1] ^= 1;
        assert!(VectorBundleFooter::from_bundle(&corrupted).is_err());
        let mut corrupted = bytes;
        *corrupted.last_mut().unwrap() ^= 1;
        assert!(VectorBundleFooter::from_bundle(&corrupted).is_err());

        // Pages need at least one row (limits are validated with the index options).
        let mut builder = VectorBundleBuilder::new("vi".to_owned(), "v1".to_owned(), ROWS as u64);
        assert!(builder.add_fp16_data(0, &column, 0).is_err());

        // A component past the footer is rejected when the footer is read.
        let mut footer = VectorBundleFooter::new("vi".to_owned(), "v1".to_owned(), 1);
        footer.components.insert(
            "a".to_owned(),
            VectorBundleEntry::new(0, 10, b"0123456789", VectorBundleCodec::Plain),
        );
        assert!(footer.validate(9).is_err());
        footer.validate(10)?;
        Ok(())
    }
}
