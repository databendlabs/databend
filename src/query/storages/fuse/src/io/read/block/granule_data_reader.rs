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
use std::collections::VecDeque;
use std::ops::Range;

use databend_common_base::runtime::profile::Profile;
use databend_common_base::runtime::profile::ProfileStatisticsName;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::ColumnId;
use databend_common_metrics::storage::metrics_inc_remote_io_read_bytes;
use databend_common_metrics::storage::metrics_inc_remote_io_read_parts;
use databend_storages_common_io::MergeIOReadResult;
use databend_storages_common_io::OperatorRangeReader;
use databend_storages_common_io::OwnerMemory;
use databend_storages_common_io::ReadSettings;
use opendal::Buffer;

use super::BlockReadContext;
use super::BlockReadResult;
use crate::FuseBlockPartInfo;
use crate::io::GranuleRangeBounds;
use crate::io::OffsetsIndex;

struct GranuleColumnReader {
    column_id: ColumnId,
    has_dictionary: bool,
    dictionary: Option<Buffer>,
    reader: OperatorRangeReader,
}

impl GranuleColumnReader {
    fn read_next(&mut self) -> Result<Buffer> {
        if self.has_dictionary && self.dictionary.is_none() {
            self.dictionary = Some(self.reader.read()?);
        }
        let data = self.reader.read()?;
        if let Some(dictionary) = &self.dictionary {
            let mut bytes = Vec::with_capacity(dictionary.len() + data.len());
            bytes.extend_from_slice(&dictionary.to_bytes());
            bytes.extend_from_slice(&data.to_bytes());
            Ok(Buffer::from(bytes))
        } else {
            Ok(data)
        }
    }
}

pub(crate) struct GranuleRangeRead {
    pub(crate) range: Range<usize>,
    pub(crate) data: BlockReadResult,
}

pub(crate) struct GranuleDataReader {
    location: String,
    ranges: VecDeque<(Range<usize>, GranuleRangeBounds)>,
    column_readers: Vec<GranuleColumnReader>,
}

impl GranuleDataReader {
    pub(crate) fn create(
        read_context: &BlockReadContext,
        settings: &ReadSettings,
        part: &FuseBlockPartInfo,
        groups: &[Vec<Range<usize>>],
        offsets: &OffsetsIndex,
    ) -> Result<Self> {
        let ranges = groups
            .iter()
            .flat_map(|group| group.iter().cloned())
            .collect::<Vec<_>>();
        let bounds = ranges
            .iter()
            .cloned()
            .map(|range| offsets.ranges_for_granules(&part.columns_meta, range, part.nums_rows))
            .collect::<Result<Vec<_>>>()?;

        let mut column_readers = Vec::new();
        for (column_id, ..) in read_context.project_indices().values() {
            let column_bounds = bounds
                .iter()
                .map(|bounds| {
                    bounds
                        .columns
                        .iter()
                        .find(|column| column.column_id == *column_id)
                        .ok_or_else(|| {
                            ErrorCode::Internal(format!(
                                "granule offset index has no bounds for projected column {column_id}"
                            ))
                        })
                })
                .collect::<Result<Vec<_>>>()?;
            let dictionary = column_bounds[0].dict_range.clone();
            if column_bounds
                .iter()
                .any(|bounds| bounds.dict_range != dictionary)
            {
                return Err(ErrorCode::Internal(format!(
                    "granule data bounds disagree on dictionary range for column {column_id}"
                )));
            }

            let mut byte_ranges: Vec<Range<u64>> =
                Vec::with_capacity(column_bounds.len() + usize::from(dictionary.is_some()));
            if let Some(range) = &dictionary {
                byte_ranges.push(range.clone());
            }
            byte_ranges.extend(column_bounds.iter().map(|bounds| bounds.data_range.clone()));
            let total_bytes = byte_ranges
                .iter()
                .map(|range| range.end - range.start)
                .sum::<u64>();
            metrics_inc_remote_io_read_bytes(total_bytes);
            Profile::record_usize_profile(
                ProfileStatisticsName::ScanBytesFromRemote,
                total_bytes as usize,
            );

            column_readers.push(GranuleColumnReader {
                column_id: *column_id,
                has_dictionary: dictionary.is_some(),
                dictionary: None,
                reader: OperatorRangeReader::create(
                    settings,
                    read_context.operator().clone(),
                    part.location.clone(),
                    &byte_ranges,
                    1,
                )?,
            });
        }

        Ok(Self {
            location: part.location.clone(),
            ranges: ranges.into_iter().zip(bounds).collect(),
            column_readers,
        })
    }

    pub(crate) fn read_next(&mut self) -> Result<Option<GranuleRangeRead>> {
        let Some((granule_range, bounds)) = self.ranges.pop_front() else {
            return Ok(None);
        };

        metrics_inc_remote_io_read_parts(1);
        let mut chunks = Vec::with_capacity(self.column_readers.len());
        let mut columns_chunk_offsets = HashMap::with_capacity(self.column_readers.len());
        for (chunk_index, column_reader) in self.column_readers.iter_mut().enumerate() {
            let buffer = column_reader.read_next()?;
            let len = buffer.len();
            chunks.push((chunk_index, buffer));
            columns_chunk_offsets.insert(column_reader.column_id, (chunk_index, 0..len));
        }

        let result = MergeIOReadResult::create(
            OwnerMemory::create(chunks),
            columns_chunk_offsets,
            self.location.clone(),
        );
        Ok(Some(GranuleRangeRead {
            range: granule_range,
            data: BlockReadResult::create_with_row_range(result, bounds.row_range),
        }))
    }
}
