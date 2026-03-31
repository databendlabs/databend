// SPDX-License-Identifier: Apache-2.0
// SPDX-FileCopyrightText: Copyright The Lance Authors

use crate::{format::pb, rowids::bitmap::Bitmap};
use lance_core::{Error, Result};
use snafu::location;

use super::{encoded_array::EncodedU64Array, RowIdSequence, U64Segment};
use prost::Message;

impl TryFrom<pb::RowIdSequence> for RowIdSequence {
    type Error = Error;

    fn try_from(pb: pb::RowIdSequence) -> Result<Self> {
        Ok(Self(
            pb.segments
                .into_iter()
                .map(U64Segment::try_from)
                .collect::<Result<Vec<_>>>()?,
        ))
    }
}

impl TryFrom<pb::U64Segment> for U64Segment {
    type Error = Error;

    fn try_from(pb: pb::U64Segment) -> Result<Self> {
        use pb::u64_segment as pb_seg;
        use pb::u64_segment::Segment::*;
        match pb.segment {
            Some(Range(pb_seg::Range { start, end })) => Ok(Self::Range(start..end)),
            Some(RangeWithHoles(pb_seg::RangeWithHoles { start, end, holes })) => {
                let holes = holes
                    .ok_or_else(|| Error::invalid_input("missing hole", location!()))?
                    .try_into()?;
                Ok(Self::RangeWithHoles {
                    range: start..end,
                    holes,
                })
            }
            Some(RangeWithBitmap(pb_seg::RangeWithBitmap { start, end, bitmap })) => {
                Ok(Self::RangeWithBitmap {
                    range: start..end,
                    bitmap: Bitmap {
                        data: bitmap,
                        len: (end - start) as usize,
                    },
                })
            }
            Some(SortedArray(array)) => Ok(Self::SortedArray(EncodedU64Array::try_from(array)?)),
            Some(Array(array)) => Ok(Self::Array(EncodedU64Array::try_from(array)?)),
            // TODO: why non-exhaustive?
            // Some(_) => Err(Error::invalid_input("unknown segment type", location!())),
            None => Err(Error::invalid_input("missing segment type", location!())),
        }
    }
}

impl TryFrom<pb::EncodedU64Array> for EncodedU64Array {
    type Error = Error;

    fn try_from(pb: pb::EncodedU64Array) -> Result<Self> {
        use pb::encoded_u64_array as pb_arr;
        use pb::encoded_u64_array::Array::*;
        match pb.array {
            Some(U16Array(pb_arr::U16Array { base, offsets })) => {
                assert!(
                    offsets.len() % 2 == 0,
                    "Must have even number of bytes to store u16 array"
                );
                let offsets = offsets
                    .chunks_exact(2)
                    .map(|chunk| u16::from_le_bytes([chunk[0], chunk[1]]))
                    .collect();
                Ok(Self::U16 { base, offsets })
            }
            Some(U32Array(pb_arr::U32Array { base, offsets })) => {
                assert!(
                    offsets.len() % 4 == 0,
                    "Must have even number of bytes to store u32 array"
                );
                let offsets = offsets
                    .chunks_exact(4)
                    .map(|chunk| u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
                    .collect();
                Ok(Self::U32 { base, offsets })
            }
            Some(U64Array(pb_arr::U64Array { values })) => {
                assert!(
                    values.len() % 8 == 0,
                    "Must have even number of bytes to store u64 array"
                );
                let values = values
                    .chunks_exact(8)
                    .map(|chunk| {
                        u64::from_le_bytes([
                            chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6],
                            chunk[7],
                        ])
                    })
                    .collect();
                Ok(Self::U64(values))
            }
            // TODO: shouldn't this enum be non-exhaustive?
            // Some(_) => Err(Error::invalid_input("unknown array type", location!())),
            None => Err(Error::invalid_input("missing array type", location!())),
        }
    }
}

impl From<RowIdSequence> for pb::RowIdSequence {
    fn from(sequence: RowIdSequence) -> Self {
        Self {
            segments: sequence.0.into_iter().map(pb::U64Segment::from).collect(),
        }
    }
}

impl From<U64Segment> for pb::U64Segment {
    fn from(segment: U64Segment) -> Self {
        match segment {
            U64Segment::Range(range) => Self {
                segment: Some(pb::u64_segment::Segment::Range(pb::u64_segment::Range {
                    start: range.start,
                    end: range.end,
                })),
            },
            U64Segment::RangeWithHoles { range, holes } => Self {
                segment: Some(pb::u64_segment::Segment::RangeWithHoles(
                    pb::u64_segment::RangeWithHoles {
                        start: range.start,
                        end: range.end,
                        holes: Some(holes.into()),
                    },
                )),
            },
            U64Segment::RangeWithBitmap { range, bitmap } => Self {
                segment: Some(pb::u64_segment::Segment::RangeWithBitmap(
                    pb::u64_segment::RangeWithBitmap {
                        start: range.start,
                        end: range.end,
                        bitmap: bitmap.data,
                    },
                )),
            },
            U64Segment::SortedArray(array) => Self {
                segment: Some(pb::u64_segment::Segment::SortedArray(array.into())),
            },
            U64Segment::Array(array) => Self {
                segment: Some(pb::u64_segment::Segment::Array(array.into())),
            },
        }
    }
}

impl From<EncodedU64Array> for pb::EncodedU64Array {
    fn from(array: EncodedU64Array) -> Self {
        match array {
            EncodedU64Array::U16 { base, offsets } => Self {
                array: Some(pb::encoded_u64_array::Array::U16Array(
                    pb::encoded_u64_array::U16Array {
                        base,
                        offsets: offsets
                            .iter()
                            .flat_map(|&offset| offset.to_le_bytes().to_vec())
                            .collect(),
                    },
                )),
            },
            EncodedU64Array::U32 { base, offsets } => Self {
                array: Some(pb::encoded_u64_array::Array::U32Array(
                    pb::encoded_u64_array::U32Array {
                        base,
                        offsets: offsets
                            .iter()
                            .flat_map(|&offset| offset.to_le_bytes().to_vec())
                            .collect(),
                    },
                )),
            },
            EncodedU64Array::U64(values) => Self {
                array: Some(pb::encoded_u64_array::Array::U64Array(
                    pb::encoded_u64_array::U64Array {
                        values: values
                            .iter()
                            .flat_map(|&value| value.to_le_bytes().to_vec())
                            .collect(),
                    },
                )),
            },
        }
    }
}

/// Serialize a rowid sequence to a buffer.
pub fn write_row_ids(sequence: &RowIdSequence) -> Vec<u8> {
    let pb_sequence = pb::RowIdSequence::from(sequence.clone());
    pb_sequence.encode_to_vec()
}

/// Deserialize a rowid sequence from some bytes.
pub fn read_row_ids(reader: &[u8]) -> Result<RowIdSequence> {
    let pb_sequence = pb::RowIdSequence::decode(reader)?;
    RowIdSequence::try_from(pb_sequence)
}

#[cfg(test)]
mod test {
    use super::*;
    use pretty_assertions::assert_eq;

    #[test]
    fn test_write_read_row_ids() {
        let mut sequence = RowIdSequence::from(0..20);
        sequence.0.push(U64Segment::Range(30..100));
        sequence.0.push(U64Segment::RangeWithHoles {
            range: 100..200,
            holes: EncodedU64Array::U64(vec![104, 108, 150]),
        });
        sequence.0.push(U64Segment::RangeWithBitmap {
            range: 200..300,
            bitmap: Bitmap::new_empty(100),
        });
        sequence
            .0
            .push(U64Segment::SortedArray(EncodedU64Array::U16 {
                base: 200,
                offsets: vec![1, 2, 3],
            }));
        sequence
            .0
            .push(U64Segment::Array(EncodedU64Array::U64(vec![1, 2, 3])));

        let serialized = write_row_ids(&sequence);

        let sequence2 = read_row_ids(&serialized).unwrap();

        assert_eq!(sequence.0, sequence2.0);
    }
}
