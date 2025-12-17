// Copyright Qdrant
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

use std::iter::Copied;

use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use itertools::Either;
use itertools::Itertools as _;
use zerocopy::FromBytes;
use zerocopy::Immutable;
use zerocopy::native_endian::U64 as NativeU64;

use super::GraphLinksFormat;
use super::header::HEADER_VERSION_COMPRESSED;
use super::header::HeaderCompressed;
use super::header::HeaderPlain;
use crate::hnsw_index::common::bitpacking::packed_bits;
use crate::hnsw_index::common::bitpacking_links::MIN_BITS_PER_VALUE;
use crate::hnsw_index::common::bitpacking_links::PackedLinksIterator;
use crate::hnsw_index::common::bitpacking_links::iterate_packed_links;
use crate::hnsw_index::common::bitpacking_ordered;
use crate::hnsw_index::common::types::PointOffsetType;

/// An (almost) zero-copy, non-owning view into serialized graph links stored
/// as a `&[u8]` slice.
#[derive(Debug)]
pub(super) struct GraphLinksView<'a> {
    pub(super) reindex: &'a [PointOffsetType],
    pub(super) compression: CompressionInfo<'a>,
    /// Level offsets, copied into RAM for faster access.
    /// Has at least two elements:
    /// - [`super::GraphLinksSerializer`] always writes `0` as the first element.
    /// - Additional element is added during deserialization.
    pub(super) level_offsets: Vec<u64>,
}

/// An iterator type returned by [`GraphLinksView::links`].
pub type LinksIterator<'a> = Either<Copied<std::slice::Iter<'a, u32>>, PackedLinksIterator<'a>>;

#[derive(Debug)]
pub(super) enum CompressionInfo<'a> {
    Uncompressed {
        links: &'a [u32],
        offsets: &'a [NativeU64],
    },
    Compressed {
        compressed_links: &'a [u8],
        offsets: bitpacking_ordered::Reader<'a>,
        m: usize,
        m0: usize,
        bits_per_unsorted: u8,
    },
}

impl GraphLinksView<'_> {
    pub(super) fn load(data: &[u8], format: GraphLinksFormat) -> Result<GraphLinksView<'_>> {
        match format {
            GraphLinksFormat::Compressed => Self::load_compressed(data),
            GraphLinksFormat::Plain => Self::load_plain(data),
        }
    }

    fn load_plain(data: &[u8]) -> Result<GraphLinksView<'_>> {
        let (header, data) =
            HeaderPlain::ref_from_prefix(data).map_err(|_| error_unsufficent_size())?;
        let (level_offsets, data) =
            read_level_offsets(data, header.levels_count, header.total_offset_count)?;
        let (reindex, data) = get_slice::<PointOffsetType>(data, header.point_count)?;
        let (links, data) = get_slice::<u32>(data, header.total_links_count)?;
        let (_, data) = get_slice::<u8>(data, header.offsets_padding_bytes)?;
        let (offsets, _bytes) = get_slice::<NativeU64>(data, header.total_offset_count)?;
        Ok(GraphLinksView {
            reindex,
            compression: CompressionInfo::Uncompressed { links, offsets },
            level_offsets,
        })
    }

    fn load_compressed(data: &[u8]) -> Result<GraphLinksView<'_>> {
        let (header, data) =
            HeaderCompressed::ref_from_prefix(data).map_err(|_| error_unsufficent_size())?;
        debug_assert_eq!(header.version.get(), HEADER_VERSION_COMPRESSED);
        let (level_offsets, data) = read_level_offsets(
            data,
            header.levels_count.get(),
            header.offsets_parameters.length.get(),
        )?;
        let (reindex, data) = get_slice::<PointOffsetType>(data, header.point_count.get())?;
        let (compressed_links, data) = get_slice::<u8>(data, header.total_links_bytes.get())?;
        let (offsets, _bytes) = bitpacking_ordered::Reader::new(header.offsets_parameters, data)
            .map_err(|e| ErrorCode::Internal(format!("Can't create decompressor: {e}")))?;
        Ok(GraphLinksView {
            reindex,
            compression: CompressionInfo::Compressed {
                compressed_links,
                offsets,
                m: header.m.get() as usize,
                m0: header.m0.get() as usize,
                bits_per_unsorted: MIN_BITS_PER_VALUE.max(packed_bits(
                    u32::try_from(header.point_count.get().saturating_sub(1))
                        .map_err(|_| ErrorCode::Internal("Too many points in GraphLinks file"))?,
                )),
            },
            level_offsets,
        })
    }

    pub(super) fn links(&self, point_id: PointOffsetType, level: usize) -> LinksIterator<'_> {
        let idx = if level == 0 {
            point_id as usize
        } else {
            self.level_offsets[level] as usize + self.reindex[point_id as usize] as usize
        };

        match self.compression {
            CompressionInfo::Uncompressed { links, offsets } => {
                let links_range = offsets[idx].get() as usize..offsets[idx + 1].get() as usize;
                Either::Left(links[links_range].iter().copied())
            }
            CompressionInfo::Compressed {
                compressed_links,
                ref offsets,
                m,
                m0,
                bits_per_unsorted,
            } => {
                let links_range =
                    offsets.get(idx).unwrap() as usize..offsets.get(idx + 1).unwrap() as usize;
                Either::Right(iterate_packed_links(
                    &compressed_links[links_range],
                    bits_per_unsorted,
                    if level == 0 { m0 } else { m },
                ))
            }
        }
    }

    pub(super) fn point_level(&self, point_id: PointOffsetType) -> usize {
        let reindexed_point_id = u64::from(self.reindex[point_id as usize]);
        for (level, (&a, &b)) in self
            .level_offsets
            .iter()
            .skip(1)
            .tuple_windows()
            .enumerate()
        {
            if reindexed_point_id >= b - a {
                return level;
            }
        }
        // See the doc comment on `level_offsets`.
        self.level_offsets.len() - 2
    }
}

fn read_level_offsets(
    bytes: &[u8],
    levels_count: u64,
    total_offset_count: u64,
) -> Result<(Vec<u64>, &[u8])> {
    let (level_offsets, bytes) = get_slice::<u64>(bytes, levels_count)?;
    let mut result = Vec::with_capacity(level_offsets.len() + 1);
    result.extend_from_slice(level_offsets);
    result.push(total_offset_count.checked_sub(1).ok_or_else(|| {
        ErrorCode::Internal("Total offset count should be at least 1 in GraphLinks file")
    })?);
    Ok((result, bytes))
}

fn get_slice<T: FromBytes + Immutable>(data: &[u8], length: u64) -> Result<(&[T], &[u8])> {
    <[T]>::ref_from_prefix_with_elems(data, length as usize).map_err(|_| error_unsufficent_size())
}

fn error_unsufficent_size() -> ErrorCode {
    ErrorCode::Internal("Unsufficent file size for GraphLinks file")
}
