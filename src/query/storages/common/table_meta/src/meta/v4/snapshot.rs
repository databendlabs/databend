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

use std::io::Cursor;
use std::io::Read;
use std::sync::Arc;

use chrono::DateTime;
use chrono::TimeDelta;
use chrono::Utc;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_expression::TableSchema;
use databend_common_io::prelude::BinaryRead;
use serde::Deserialize;
use serde::Serialize;

use crate::meta::format::compress;
use crate::meta::format::encode;
use crate::meta::format::read_and_deserialize;
use crate::meta::format::MetaCompression;
use crate::meta::monotonically_increased_timestamp;
use crate::meta::uuid_from_date_time;
use crate::meta::v2;
use crate::meta::v3;
use crate::meta::ClusterKey;
use crate::meta::FormatVersion;
use crate::meta::Location;
use crate::meta::MetaEncoding;
use crate::meta::SnapshotId;
use crate::meta::Statistics;
use crate::meta::TableMetaTimestamps;
use crate::meta::Versioned;
use crate::readers::snapshot_reader::TableSnapshotAccessor;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct TableSnapshot {
    /// format version of TableSnapshot metadata
    ///
    /// Note that:
    ///
    /// - A instance of v3::TableSnapshot may have a value of v2/v1::TableSnapshot::VERSION for this field.
    ///
    ///   That indicates this instance is converted from a v2/v1::TableSnapshot.
    ///
    /// - The meta writers are responsible for only writing down the latest version of TableSnapshot, and
    ///   the format_version being written is of the latest version.
    ///
    ///   e.g. if the current version of TableSnapshot is v3::TableSnapshot, then the format_version
    ///   that will be written down to object storage as part of TableSnapshot table meta data,
    ///   should always be v3::TableSnapshot::VERSION (which is 3)
    pub format_version: FormatVersion,

    /// id of snapshot
    pub snapshot_id: SnapshotId,

    /// timestamp of this snapshot
    //  for backward compatibility, `Option` is used
    pub timestamp: Option<DateTime<Utc>>,

    // The table seq before snapshot commit.
    pub prev_table_seq: Option<u64>,

    /// previous snapshot
    pub prev_snapshot_id: Option<(SnapshotId, FormatVersion)>,

    /// For each snapshot, we keep a schema for it (in case of schema evolution)
    pub schema: TableSchema,

    /// Summary Statistics
    pub summary: Statistics,

    /// Pointers to SegmentInfos (maybe of different format)
    ///
    /// We rely on background merge tasks to keep merging segments, so that
    /// this the size of this vector could be kept reasonable
    pub segments: Vec<Location>,

    /// The metadata of the cluster keys.
    /// **This field is deprecated and will be removed in the next version.**
    pub cluster_key_meta: Option<ClusterKey>,
    pub table_statistics_location: Option<String>,
}

impl TableSnapshot {
    /// Note that table_meta_timestamps is not always equal to prev_timestamp.
    pub fn try_new(
        prev_table_seq: Option<u64>,
        prev_snapshot: Option<Arc<TableSnapshot>>,
        schema: TableSchema,
        summary: Statistics,
        segments: Vec<Location>,
        table_statistics_location: Option<String>,
        table_meta_timestamps: TableMetaTimestamps,
    ) -> Result<Self> {
        let TableMetaTimestamps {
            segment_block_timestamp,
            snapshot_timestamp,
            snapshot_timestamp_validation_context,
        } = table_meta_timestamps;

        let snapshot_timestamp_adjusted =
            monotonically_increased_timestamp(snapshot_timestamp, &prev_snapshot.timestamp());

        if segment_block_timestamp < snapshot_timestamp_adjusted {
            let mut err_msg = format!(
                "Unresolvable conflict: Transaction conflicts with commit at {:?}. Can only merge with commits before {:?}.",
                snapshot_timestamp_adjusted, segment_block_timestamp
            );

            if let Some(ctx) = snapshot_timestamp_validation_context {
                if ctx.is_transient {
                    err_msg.push_str(
                        &format!(" Transient table (ID: {}) detected. Concurrent mutations same transient table likely cause conflicts. Consider using regular tables.", ctx.table_id)
                    );
                } else {
                    let delta = snapshot_timestamp - segment_block_timestamp;
                    if delta < TimeDelta::hours(1) {
                        // TODO give user a doc url, which describes this situation more clearly, such as increasing the value of setting 'max_execute_time_in_seconds' also work, and what the tradeoffs are.
                        err_msg.push_str(&format!(
                            " Conflict window too narrow ({:?}). Consider increasing the value of setting 'data_retention_time_in_days'.",
                            delta
                        ));
                    }
                }
            }

            return Err(ErrorCode::TransactionTimeout(err_msg));
        }

        Ok(Self {
            format_version: TableSnapshot::VERSION,
            snapshot_id: uuid_from_date_time(snapshot_timestamp_adjusted),
            timestamp: Some(snapshot_timestamp_adjusted),
            prev_table_seq,
            prev_snapshot_id: prev_snapshot.snapshot_id(),
            schema,
            summary,
            segments,
            cluster_key_meta: None,
            table_statistics_location,
        })
    }

    /// used in ut
    #[cfg(test)]
    pub fn new_empty_snapshot(schema: TableSchema, prev_table_seq: Option<u64>) -> Self {
        Self::try_new(
            prev_table_seq,
            None,
            schema,
            Statistics::default(),
            vec![],
            None,
            Default::default(),
        )
        .unwrap()
    }

    pub fn try_from_previous(
        previous: Arc<TableSnapshot>,
        prev_table_seq: Option<u64>,
        table_meta_timestamps: TableMetaTimestamps,
    ) -> Result<Self> {
        // the timestamp of the new snapshot will be adjusted by the `new` method
        Self::try_new(
            prev_table_seq,
            Some(previous.clone()),
            previous.schema.clone(),
            previous.summary.clone(),
            previous.segments.clone(),
            previous.table_statistics_location.clone(),
            table_meta_timestamps,
        )
    }

    /// Serializes the struct to a byte vector.
    ///
    /// The byte vector contains the format version, encoding, compression, and compressed data. The encoding
    /// and compression are set to default values. The data is encoded and compressed.
    ///
    /// # Returns
    ///
    /// A Result containing the serialized data as a byte vector. If any errors occur during
    /// encoding, compression, or writing to the byte vector, an error will be returned.
    pub fn to_bytes(&self) -> Result<Vec<u8>> {
        let encoding = MetaEncoding::MessagePack;
        let compression = MetaCompression::default();

        let data = encode(&encoding, &self)?;
        let data_compress = compress(&compression, data)?;

        let data_size = self.format_version.to_le_bytes().len()
            + 2
            + data_compress.len().to_le_bytes().len()
            + data_compress.len();
        let mut buf = Vec::with_capacity(data_size);

        buf.extend_from_slice(&self.format_version.to_le_bytes());
        buf.push(encoding as u8);
        buf.push(compression as u8);
        buf.extend_from_slice(&data_compress.len().to_le_bytes());

        buf.extend(data_compress);

        Ok(buf)
    }

    /// Reads a snapshot from Vec<u8> and returns a `TableSnapshot` object.
    ///
    /// This function reads the following fields from the stream and constructs a `TableSnapshot` object:
    ///
    /// * `version` (u64): The version number of the snapshot.
    /// * `encoding` (u8): The encoding format used to serialize the snapshot's data.
    /// * `compression` (u8): The compression format used to compress the snapshot's data.
    /// * `snapshot_size` (u64): The size (in bytes) of the compressed snapshot data.
    ///
    /// The function then reads the compressed snapshot data from the stream, decompresses it using
    /// the specified compression format, and deserializes it using the specified encoding format.
    /// Finally, it constructs a `TableSnapshot` object using the deserialized data and returns it.
    pub fn from_slice(buffer: &[u8]) -> Result<TableSnapshot> {
        Self::from_read(Cursor::new(buffer))
    }

    pub fn from_read(mut r: impl Read) -> Result<TableSnapshot> {
        let version = r.read_scalar::<u64>()?;
        assert_eq!(version, TableSnapshot::VERSION);
        let encoding = MetaEncoding::try_from(r.read_scalar::<u8>()?)?;
        let compression = MetaCompression::try_from(r.read_scalar::<u8>()?)?;
        let snapshot_size: u64 = r.read_scalar::<u64>()?;

        read_and_deserialize(&mut r, snapshot_size, &encoding, &compression)
    }

    #[inline]
    pub fn encoding() -> MetaEncoding {
        MetaEncoding::MessagePack
    }
}

// use the chain of converters, for versions before v3
impl From<v2::TableSnapshot> for TableSnapshot {
    fn from(s: v2::TableSnapshot) -> Self {
        Self {
            // NOTE: it is important to let the format_version return from here
            // carries the format_version of snapshot being converted.
            format_version: s.format_version,
            snapshot_id: s.snapshot_id,
            timestamp: s.timestamp,
            prev_table_seq: None,
            prev_snapshot_id: s.prev_snapshot_id,
            schema: s.schema,
            summary: s.summary,
            segments: s.segments,
            cluster_key_meta: s.cluster_key_meta,
            table_statistics_location: s.table_statistics_location,
        }
    }
}

impl<T> From<T> for TableSnapshot
where T: Into<v3::TableSnapshot>
{
    fn from(s: T) -> Self {
        let s: v3::TableSnapshot = s.into();
        Self {
            // NOTE: it is important to let the format_version return from here
            // carries the format_version of snapshot being converted.
            format_version: s.format_version,
            snapshot_id: s.snapshot_id,
            timestamp: s.timestamp,
            prev_table_seq: None,
            prev_snapshot_id: s.prev_snapshot_id,
            schema: s.schema.into(),
            summary: s.summary.into(),
            segments: s.segments,
            cluster_key_meta: s.cluster_key_meta,
            table_statistics_location: s.table_statistics_location,
        }
    }
}

// A memory light version of TableSnapshot(Without segments)
// This *ONLY* used for some optimize operation, like PURGE/FUSE_SNAPSHOT function to avoid OOM.
#[derive(Clone, Debug)]
pub struct TableSnapshotLite {
    pub format_version: FormatVersion,
    pub snapshot_id: SnapshotId,
    pub timestamp: Option<DateTime<Utc>>,
    pub prev_snapshot_id: Option<(SnapshotId, FormatVersion)>,
    pub row_count: u64,
    pub block_count: u64,
    pub index_size: u64,
    pub bloom_index_size: Option<u64>,
    pub ngram_index_size: Option<u64>,
    pub inverted_index_size: Option<u64>,
    pub vector_index_size: Option<u64>,
    pub virtual_column_size: Option<u64>,
    pub uncompressed_byte_size: u64,
    pub compressed_byte_size: u64,
    pub segment_count: u64,
}

impl From<(&TableSnapshot, FormatVersion)> for TableSnapshotLite {
    fn from((value, ver): (&TableSnapshot, FormatVersion)) -> Self {
        TableSnapshotLite {
            format_version: ver,
            snapshot_id: value.snapshot_id,
            timestamp: value.timestamp,
            prev_snapshot_id: value.prev_snapshot_id,
            row_count: value.summary.row_count,
            block_count: value.summary.block_count,
            index_size: value.summary.index_size,
            bloom_index_size: value.summary.bloom_index_size,
            ngram_index_size: value.summary.ngram_index_size,
            inverted_index_size: value.summary.inverted_index_size,
            vector_index_size: value.summary.vector_index_size,
            virtual_column_size: value.summary.virtual_column_size,
            uncompressed_byte_size: value.summary.uncompressed_byte_size,
            segment_count: value.segments.len() as u64,
            compressed_byte_size: value.summary.compressed_byte_size,
        }
    }
}

#[cfg(test)]
mod tests {
    use databend_common_exception::Result;

    use crate::meta::TableSnapshotV4;

    #[test]
    fn test_decode_snapshot() -> Result<()> {
        let data = [
            4, 0, 0, 0, 0, 0, 0, 0, 2, 1, 184, 2, 0, 0, 0, 0, 0, 0, 40, 181, 47, 253, 0, 88, 125,
            21, 0, 166, 106, 146, 67, 32, 113, 155, 3, 11, 8, 18, 194, 162, 33, 44, 249, 11, 112,
            151, 138, 127, 227, 65, 161, 154, 29, 13, 252, 118, 41, 21, 60, 51, 195, 137, 101, 101,
            119, 74, 154, 72, 41, 126, 21, 251, 59, 133, 76, 173, 71, 48, 197, 134, 48, 133, 177,
            137, 154, 136, 136, 168, 145, 4, 132, 1, 224, 132, 253, 237, 127, 239, 135, 0, 125, 0,
            128, 0, 93, 215, 42, 62, 24, 129, 28, 244, 188, 236, 239, 73, 42, 133, 96, 199, 229,
            87, 177, 63, 159, 41, 122, 132, 219, 190, 36, 58, 148, 228, 89, 157, 126, 162, 13, 109,
            125, 227, 101, 145, 101, 71, 72, 129, 1, 26, 18, 44, 34, 112, 100, 24, 225, 161, 36,
            229, 178, 43, 23, 60, 75, 175, 93, 191, 228, 120, 8, 162, 76, 192, 48, 236, 162, 101,
            33, 153, 21, 178, 85, 17, 50, 87, 2, 174, 74, 160, 93, 213, 5, 3, 180, 253, 217, 45,
            125, 210, 201, 6, 10, 28, 168, 28, 11, 70, 14, 242, 12, 21, 33, 55, 66, 80, 3, 100, 76,
            42, 31, 111, 50, 216, 55, 163, 176, 92, 76, 115, 232, 28, 249, 109, 67, 252, 161, 124,
            8, 164, 146, 127, 104, 174, 45, 126, 72, 57, 42, 33, 126, 207, 57, 43, 166, 127, 232,
            156, 75, 85, 74, 29, 138, 92, 105, 49, 174, 93, 74, 0, 144, 149, 142, 207, 75, 79, 8,
            0, 176, 210, 226, 226, 31, 154, 165, 191, 167, 147, 211, 115, 246, 16, 180, 216, 187,
            226, 139, 73, 190, 231, 95, 232, 160, 202, 199, 136, 135, 107, 135, 2, 228, 175, 136,
            61, 137, 1, 135, 36, 16, 158, 176, 173, 21, 179, 9, 15, 249, 29, 191, 40, 26, 250, 123,
            246, 187, 29, 81, 221, 244, 23, 65, 139, 189, 147, 105, 82, 121, 173, 88, 177, 71, 186,
            176, 191, 13, 74, 132, 222, 89, 241, 129, 8, 228, 53, 86, 196, 243, 200, 227, 3, 99,
            60, 48, 81, 212, 7, 69, 153, 140, 162, 241, 25, 20, 48, 25, 179, 123, 116, 94, 106, 98,
            36, 25, 61, 135, 13, 66, 16, 141, 160, 113, 34, 25, 61, 178, 137, 170, 32, 97, 210, 49,
            98, 92, 140, 8, 178, 201, 70, 173, 155, 80, 233, 98, 250, 237, 225, 58, 154, 252, 67,
            51, 9, 14, 253, 110, 101, 95, 76, 159, 206, 217, 46, 74, 161, 127, 22, 171, 119, 50,
            125, 100, 23, 107, 115, 228, 135, 229, 123, 151, 126, 166, 147, 5, 1, 79, 136, 173, 91,
            121, 41, 202, 84, 171, 120, 79, 88, 58, 109, 67, 20, 232, 82, 27, 234, 64, 145, 228,
            239, 207, 178, 98, 47, 23, 103, 11, 151, 140, 35, 243, 203, 239, 195, 186, 195, 220,
            218, 38, 167, 101, 213, 212, 108, 238, 155, 213, 25, 174, 49, 13, 147, 127, 171, 33,
            179, 71, 63, 211, 239, 143, 81, 242, 248, 72, 138, 86, 105, 26, 102, 101, 152, 87, 142,
            181, 213, 92, 230, 189, 185, 87, 93, 153, 143, 251, 52, 88, 166, 155, 220, 53, 179, 12,
            55, 171, 106, 235, 170, 203, 186, 203, 50, 2, 218, 75, 198, 145, 243, 194, 58, 255, 60,
            235, 50, 91, 117, 77, 173, 49, 143, 219, 52, 89, 214, 105, 180, 118, 78, 99, 85, 27,
            140, 72, 150, 24, 14, 179, 182, 11, 99, 101, 53, 74, 76, 242, 95, 15, 38, 32, 80, 66,
            68, 118, 30, 213, 197, 176, 178, 224, 25, 34, 40, 127, 18, 163, 208, 38, 234, 239, 242,
            119, 228, 17, 11, 187, 85, 20, 66, 230, 19, 241, 227, 180, 153, 231, 16, 179, 230, 108,
            107, 124, 224, 172, 203, 52, 10, 205, 235, 199, 202, 217, 21, 240, 131, 249, 51, 17,
            216, 137, 104, 114, 27, 197, 161, 56, 114, 146, 232, 84, 56, 102, 116, 160, 177, 15,
            194, 131, 175, 2, 160, 147, 115, 174, 74, 23, 233, 31, 54, 156, 67, 182, 24, 148, 117,
            5, 51,
        ];

        let _ = TableSnapshotV4::from_slice(&data)?;
        Ok(())
    }
}
