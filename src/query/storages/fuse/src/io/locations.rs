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

use std::marker::PhantomData;

use databend_common_exception::Result;
use databend_common_expression::DataBlock;
use databend_storages_common_table_meta::meta::Location;
use databend_storages_common_table_meta::meta::SegmentInfo;
use databend_storages_common_table_meta::meta::SegmentStatistics;
use databend_storages_common_table_meta::meta::SnapshotVersion;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::meta::TableSnapshotStatisticsVersion;
use databend_storages_common_table_meta::meta::VACUUM2_OBJECT_KEY_PREFIX;
use databend_storages_common_table_meta::meta::Versioned;
use databend_storages_common_table_meta::meta::trim_object_prefix;
use databend_storages_common_table_meta::meta::uuid_from_date_time;
use uuid::Uuid;
use uuid::Version;

use crate::FUSE_TBL_AGG_INDEX_PREFIX;
use crate::FUSE_TBL_GRANULE_BLOOM_INDEX_PREFIX;
use crate::FUSE_TBL_GRANULE_INDEX_PREFIX;
use crate::FUSE_TBL_INVERTED_INDEX_PREFIX;
use crate::FUSE_TBL_LAST_SNAPSHOT_HINT_V2;
use crate::FUSE_TBL_SEGMENT_STATISTICS_PREFIX;
use crate::FUSE_TBL_SPATIAL_INDEX_PREFIX;
use crate::FUSE_TBL_VECTOR_INDEX_PREFIX;
use crate::FUSE_TBL_XOR_BLOOM_INDEX_PREFIX;
use crate::LEGACY_FUSE_TBL_REF_PREFIX;
use crate::constants::FUSE_TBL_BLOCK_PREFIX;
use crate::constants::FUSE_TBL_SEGMENT_PREFIX;
use crate::constants::FUSE_TBL_SNAPSHOT_PREFIX;
use crate::constants::FUSE_TBL_SNAPSHOT_STATISTICS_PREFIX;
use crate::constants::FUSE_TBL_VIRTUAL_BLOCK_PREFIX;
use crate::constants::FUSE_TBL_VIRTUAL_BLOCK_PREFIX_V1;
use crate::index::InvertedIndexFile;
use crate::index::filters::BlockFilter;

static SNAPSHOT_V0: SnapshotVersion = SnapshotVersion::V0(PhantomData);
static SNAPSHOT_V1: SnapshotVersion = SnapshotVersion::V1(PhantomData);
static SNAPSHOT_V2: SnapshotVersion = SnapshotVersion::V2(PhantomData);
static SNAPSHOT_V3: SnapshotVersion = SnapshotVersion::V3(PhantomData);
static SNAPSHOT_V4: SnapshotVersion = SnapshotVersion::V4(PhantomData);

static SNAPSHOT_STATISTICS_V0: TableSnapshotStatisticsVersion =
    TableSnapshotStatisticsVersion::V0(PhantomData);
static SNAPSHOT_STATISTICS_V2: TableSnapshotStatisticsVersion =
    TableSnapshotStatisticsVersion::V2(PhantomData);

static SNAPSHOT_STATISTICS_V3: TableSnapshotStatisticsVersion =
    TableSnapshotStatisticsVersion::V3(PhantomData);
static SNAPSHOT_STATISTICS_V4: TableSnapshotStatisticsVersion =
    TableSnapshotStatisticsVersion::V4(PhantomData);

#[derive(Clone)]
pub struct TableMetaLocationGenerator {
    prefix: String,

    block_location_prefix: String,
    segment_info_location_prefix: String,
    bloom_index_location_prefix: String,
    snapshot_location_prefix: String,
    agg_index_location_prefix: String,
    inverted_index_location_prefix: String,
    vector_index_location_prefix: String,
    spatial_index_location_prefix: String,
    granule_index_location_prefix: String,
    segment_statistics_location_prefix: String,
    // legacy ref prefix.
    ref_snapshot_location_prefix: String,
}

impl TableMetaLocationGenerator {
    pub fn new(prefix: String) -> Self {
        let block_location_prefix = format!("{}/{}/", &prefix, FUSE_TBL_BLOCK_PREFIX,);
        let bloom_index_location_prefix =
            format!("{}/{}/", &prefix, FUSE_TBL_XOR_BLOOM_INDEX_PREFIX);
        let segment_info_location_prefix = format!("{}/{}/", &prefix, FUSE_TBL_SEGMENT_PREFIX);
        let snapshot_location_prefix = format!("{}/{}/", &prefix, FUSE_TBL_SNAPSHOT_PREFIX);
        let agg_index_location_prefix = format!("{}/{}/", &prefix, FUSE_TBL_AGG_INDEX_PREFIX);
        let inverted_index_location_prefix =
            format!("{}/{}/", &prefix, FUSE_TBL_INVERTED_INDEX_PREFIX);
        let vector_index_location_prefix = format!("{}/{}/", &prefix, FUSE_TBL_VECTOR_INDEX_PREFIX);
        let spatial_index_location_prefix =
            format!("{}/{}/", &prefix, FUSE_TBL_SPATIAL_INDEX_PREFIX);
        let granule_index_location_prefix =
            format!("{}/{}/", &prefix, FUSE_TBL_GRANULE_INDEX_PREFIX);
        let segment_statistics_location_prefix =
            format!("{}/{}/", &prefix, FUSE_TBL_SEGMENT_STATISTICS_PREFIX);
        let ref_snapshot_location_prefix = format!("{}/{}/", &prefix, LEGACY_FUSE_TBL_REF_PREFIX);
        Self {
            prefix,
            block_location_prefix,
            segment_info_location_prefix,
            bloom_index_location_prefix,
            snapshot_location_prefix,
            agg_index_location_prefix,
            inverted_index_location_prefix,
            vector_index_location_prefix,
            spatial_index_location_prefix,
            granule_index_location_prefix,
            segment_statistics_location_prefix,
            ref_snapshot_location_prefix,
        }
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    pub fn block_location_prefix(&self) -> &str {
        &self.block_location_prefix
    }

    pub fn block_bloom_index_prefix(&self) -> &str {
        &self.bloom_index_location_prefix
    }

    pub fn block_vector_index_prefix(&self) -> &str {
        &self.vector_index_location_prefix
    }

    pub fn block_spatial_index_prefix(&self) -> &str {
        &self.spatial_index_location_prefix
    }

    pub fn block_granule_index_prefix(&self) -> &str {
        &self.granule_index_location_prefix
    }

    pub fn block_granule_bloom_index_prefix(&self) -> String {
        format!("{}/{}/", self.prefix, FUSE_TBL_GRANULE_BLOOM_INDEX_PREFIX)
    }

    pub fn segment_location_prefix(&self) -> &str {
        &self.segment_info_location_prefix
    }

    pub fn snapshot_location_prefix(&self) -> &str {
        &self.snapshot_location_prefix
    }

    pub fn segment_statistics_location_prefix(&self) -> &str {
        &self.segment_statistics_location_prefix
    }

    pub fn ref_snapshot_location_prefix(&self) -> &str {
        &self.ref_snapshot_location_prefix
    }

    pub fn gen_unique_block_location(&self) -> (Location, Uuid) {
        let block_id = Uuid::now_v7();
        (
            (
                format!(
                    "{}{}{}_v{}.parquet",
                    self.block_location_prefix(),
                    VACUUM2_OBJECT_KEY_PREFIX,
                    block_id.as_simple(),
                    DataBlock::VERSION,
                ),
                DataBlock::VERSION,
            ),
            block_id,
        )
    }

    pub fn gen_block_location(
        &self,
        table_meta_timestamps: TableMetaTimestamps,
    ) -> (Location, Uuid) {
        let part_uuid = uuid_from_date_time(table_meta_timestamps.segment_block_timestamp);
        let location_path = format!(
            "{}{}{}_v{}.parquet",
            self.block_location_prefix(),
            VACUUM2_OBJECT_KEY_PREFIX,
            part_uuid.as_simple(),
            DataBlock::VERSION,
        );

        ((location_path, DataBlock::VERSION), part_uuid)
    }

    pub fn block_bloom_index_location(&self, block_id: &Uuid) -> Location {
        (
            format!(
                "{}{}_v{}.parquet",
                self.block_bloom_index_prefix(),
                block_id.as_simple(),
                BlockFilter::VERSION,
            ),
            BlockFilter::VERSION,
        )
    }

    pub fn block_vector_index_location(&self) -> Location {
        let uuid = Uuid::now_v7();
        (
            format!(
                "{}{}_v{}.parquet",
                self.block_vector_index_prefix(),
                uuid.as_simple(),
                BlockFilter::VERSION,
            ),
            BlockFilter::VERSION,
        )
    }

    pub fn block_spatial_index_location(&self) -> Location {
        let uuid = Uuid::now_v7();
        (
            format!(
                "{}{}_v{}.parquet",
                self.block_spatial_index_prefix(),
                uuid.as_simple(),
                BlockFilter::VERSION,
            ),
            BlockFilter::VERSION,
        )
    }

    /// Derive the sparse granule-mins location from its data block location.
    ///
    /// Keeping the block object key (including the vacuum2 prefix) makes the granule index file lifecycle
    /// deterministic: block GC can remove the granule index file before removing the block without listing
    /// the granule-index directory.
    pub fn gen_granule_mins_location_from_block_location(loc: &str) -> Location {
        Self::gen_granule_index_location_from_block_location(loc, "mins")
    }

    /// Derive the sparse granule-offsets location from its data block location.
    pub fn gen_granule_offsets_location_from_block_location(loc: &str) -> Location {
        Self::gen_granule_index_location_from_block_location(loc, "offsets")
    }

    /// Return all sparse granule index files whose lifetime is anchored to this block.
    pub fn gen_granule_index_locations_from_block_location(loc: &str) -> [String; 2] {
        [
            Self::gen_granule_mins_location_from_block_location(loc).0,
            Self::gen_granule_offsets_location_from_block_location(loc).0,
        ]
    }

    fn gen_granule_index_location_from_block_location(loc: &str, kind: &str) -> Location {
        let splits = loc.split('/').collect::<Vec<_>>();
        let len = splits.len();
        let prefix = splits[..len - 2].join("/");
        let block_object_key = splits[len - 1].split('_').next().unwrap_or(splits[len - 1]);
        (
            format!(
                "{}/{}/{}_{}_v{}.parquet",
                prefix,
                FUSE_TBL_GRANULE_INDEX_PREFIX,
                block_object_key,
                kind,
                BlockFilter::VERSION,
            ),
            BlockFilter::VERSION,
        )
    }

    pub fn gen_segment_info_location(
        &self,
        table_meta_timestamps: TableMetaTimestamps,
        is_column_oriented: bool,
    ) -> String {
        let segment_uuid = uuid_from_date_time(table_meta_timestamps.segment_block_timestamp);
        match is_column_oriented {
            true => format!(
                "{}{}{}.col",
                &self.segment_location_prefix(),
                VACUUM2_OBJECT_KEY_PREFIX,
                segment_uuid.as_simple(),
            ),
            false => format!(
                "{}{}{}_v{}.mpk",
                &self.segment_location_prefix(),
                VACUUM2_OBJECT_KEY_PREFIX,
                segment_uuid.as_simple(),
                SegmentInfo::VERSION,
            ),
        }
    }

    pub fn gen_snapshot_location(&self, id: &Uuid, version: u64) -> Result<String> {
        let snapshot_version = SnapshotVersion::try_from(version)?;
        let location = snapshot_version.create(id, &self.prefix);
        Ok(location)
    }

    pub fn snapshot_version(location: impl AsRef<str>) -> u64 {
        if location.as_ref().ends_with(SNAPSHOT_V4.suffix().as_str()) {
            SNAPSHOT_V4.version()
        } else if location.as_ref().ends_with(SNAPSHOT_V3.suffix().as_str()) {
            SNAPSHOT_V3.version()
        } else if location.as_ref().ends_with(SNAPSHOT_V2.suffix().as_str()) {
            SNAPSHOT_V2.version()
        } else if location.as_ref().ends_with(SNAPSHOT_V1.suffix().as_str()) {
            SNAPSHOT_V1.version()
        } else {
            SNAPSHOT_V0.version()
        }
    }

    pub fn snapshot_statistics_location_from_uuid(
        &self,
        id: &Uuid,
        version: u64,
    ) -> Result<String> {
        let statistics_version = TableSnapshotStatisticsVersion::try_from(version)?;
        Ok(statistics_version.create(id, &self.prefix))
    }

    pub fn gen_last_snapshot_hint_location(&self) -> String {
        format!("{}/{}", &self.prefix, FUSE_TBL_LAST_SNAPSHOT_HINT_V2)
    }

    pub fn gen_virtual_block_location(location: &str) -> String {
        location.replace(FUSE_TBL_BLOCK_PREFIX, FUSE_TBL_VIRTUAL_BLOCK_PREFIX)
    }

    pub fn is_legacy_virtual_block_location(location: &str) -> bool {
        location
            .split('/')
            .any(|segment| segment == FUSE_TBL_VIRTUAL_BLOCK_PREFIX_V1)
    }

    pub fn table_statistics_version(table_statistics_location: impl AsRef<str>) -> u64 {
        let version_map = [
            (
                SNAPSHOT_STATISTICS_V0.suffix(),
                SNAPSHOT_STATISTICS_V0.version(),
            ),
            (
                SNAPSHOT_STATISTICS_V2.suffix(),
                SNAPSHOT_STATISTICS_V2.version(),
            ),
            (
                SNAPSHOT_STATISTICS_V3.suffix(),
                SNAPSHOT_STATISTICS_V3.version(),
            ),
            (
                SNAPSHOT_STATISTICS_V4.suffix(),
                SNAPSHOT_STATISTICS_V4.version(),
            ),
        ];

        version_map
            .iter()
            .find(|(suffix, _)| table_statistics_location.as_ref().ends_with(suffix))
            .map(|(_, version)| *version)
            .unwrap_or(SNAPSHOT_STATISTICS_V4.version())
    }

    pub fn gen_agg_index_location_from_block_location(loc: &str, index_id: u64) -> String {
        let splits = loc.split('/').collect::<Vec<_>>();
        let len = splits.len();
        let prefix = splits[..len - 2].join("/");
        let block_name = trim_object_prefix(splits[len - 1]);
        format!("{prefix}/{FUSE_TBL_AGG_INDEX_PREFIX}/{index_id}/{block_name}")
    }

    pub fn agg_index_location_prefix(&self) -> &str {
        &self.agg_index_location_prefix
    }

    pub fn inverted_index_location_prefix(&self) -> &str {
        &self.inverted_index_location_prefix
    }

    pub fn gen_specific_inverted_index_prefix(
        &self,
        index_name: &str,
        index_version: &str,
    ) -> String {
        let short_ver: String = index_version.chars().take(7).collect();
        format!(
            "{}/{}/{}",
            self.inverted_index_location_prefix(),
            index_name,
            short_ver,
        )
    }

    pub fn gen_inverted_index_location_from_block_location(
        loc: &str,
        index_name: &str,
        index_version: &str,
    ) -> String {
        let splits = loc.split('/').collect::<Vec<_>>();
        let len = splits.len();
        let prefix = splits[..len - 2].join("/");
        let block_name = trim_object_prefix(splits[len - 1]);
        let id: String = block_name.chars().take(32).collect();
        let short_ver: String = index_version.chars().take(7).collect();
        format!(
            "{}/{}/{}/{}/{}_v{}.index",
            prefix,
            FUSE_TBL_INVERTED_INDEX_PREFIX,
            index_name,
            short_ver,
            id,
            InvertedIndexFile::VERSION,
        )
    }

    /// Derive the granule-bloom payload file location for one indexed column of one block.
    /// Layout mirrors the inverted index: `.../_i_gb/<index_name>/<ver>/<block_id>_<col>.gbloom`.
    /// The `version` (a per-CREATE uuid) makes drop/recreate produce a distinct path, so stale
    /// payloads never collide and are reclaimed by GC. It is compacted to a short base-62 string
    /// (see [`compact_index_version`]) rather than truncated, so distinct versions never alias.
    pub fn gen_granule_bloom_location_from_block_location(
        loc: &str,
        index_name: &str,
        index_version: &str,
        col_id: u32,
    ) -> String {
        let splits = loc.split('/').collect::<Vec<_>>();
        let len = splits.len();
        let prefix = splits[..len - 2].join("/");
        let block_name = trim_object_prefix(splits[len - 1]);
        let id: String = block_name.chars().take(32).collect();
        let ver = compact_index_version(index_version);
        format!(
            "{}/{}/{}/{}/{}_{}.gbloom",
            prefix, FUSE_TBL_GRANULE_BLOOM_INDEX_PREFIX, index_name, ver, id, col_id,
        )
    }

    pub fn gen_bloom_index_location_from_block_location(loc: &str) -> String {
        let splits = loc.split('/').collect::<Vec<_>>();
        let len = splits.len();
        let prefix = splits[..len - 2].join("/");
        let block_name = trim_object_prefix(splits[len - 1]);
        let id: String = block_name.chars().take(32).collect();
        format!(
            "{}/{}/{}_v{}.parquet",
            prefix,
            FUSE_TBL_XOR_BLOOM_INDEX_PREFIX,
            id,
            BlockFilter::VERSION,
        )
    }

    pub fn gen_segment_stats_location_from_segment_location(loc: &str) -> String {
        let splits = loc.split('/').collect::<Vec<_>>();
        let len = splits.len();
        let prefix = splits[..len - 2].join("/");
        let segment_name = trim_object_prefix(splits[len - 1]);
        let id: String = segment_name.chars().take(32).collect();
        format!(
            "{}/{}/{}_v{}.mpk",
            prefix,
            FUSE_TBL_SEGMENT_STATISTICS_PREFIX,
            id,
            SegmentStatistics::VERSION,
        )
    }
}

/// Base-62 alphabet (0-9 a-z A-Z) for compacting a 128-bit index version into a short, filesystem-
/// and column-name-safe token.
const BASE62_ALPHABET: &[u8; 62] =
    b"0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";

/// Compact a per-CREATE index `version` (a `Uuid::simple()` hex string) into a short base-62 token.
/// Parsing the full 128-bit value keeps it collision-free (unlike truncating to a hex prefix), so
/// distinct versions never alias in payload paths or mark column names. Non-UUID input is returned
/// unchanged.
pub fn compact_index_version(version: &str) -> String {
    let Ok(uuid) = Uuid::parse_str(version) else {
        return version.to_string();
    };
    let mut n = uuid.as_u128();
    if n == 0 {
        return "0".to_string();
    }
    let mut buf = Vec::with_capacity(22);
    while n > 0 {
        buf.push(BASE62_ALPHABET[(n % 62) as usize]);
        n /= 62;
    }
    buf.reverse();
    String::from_utf8(buf).expect("base62 alphabet is ascii")
}

trait SnapshotLocationCreator {
    fn create(&self, id: &Uuid, prefix: impl AsRef<str>) -> String;
    fn suffix(&self) -> String;
}

impl SnapshotLocationCreator for SnapshotVersion {
    // todo rename this
    fn create(&self, id: &Uuid, prefix: impl AsRef<str>) -> String {
        let vacuum_prefix = if id
            .get_version()
            .is_some_and(|v| matches!(v, Version::SortRand))
        {
            VACUUM2_OBJECT_KEY_PREFIX
        } else {
            ""
        };
        format!(
            "{}/{}/{vacuum_prefix}{}{}",
            prefix.as_ref(),
            FUSE_TBL_SNAPSHOT_PREFIX,
            id.simple(),
            self.suffix(),
        )
    }

    fn suffix(&self) -> String {
        match self {
            SnapshotVersion::V0(_) => "".to_string(),
            SnapshotVersion::V1(_) => "_v1.json".to_string(),
            SnapshotVersion::V2(_) => "_v2.json".to_string(),
            SnapshotVersion::V3(_) => "_v3.bincode".to_string(),
            SnapshotVersion::V4(_) => "_v4.mpk".to_string(),
        }
    }
}

impl SnapshotLocationCreator for TableSnapshotStatisticsVersion {
    fn create(&self, id: &Uuid, prefix: impl AsRef<str>) -> String {
        format!(
            "{}/{}/{}{}",
            prefix.as_ref(),
            FUSE_TBL_SNAPSHOT_STATISTICS_PREFIX,
            id.simple(),
            self.suffix(),
        )
    }

    fn suffix(&self) -> String {
        match self {
            TableSnapshotStatisticsVersion::V0(_) => "_ts_v0.json".to_string(),
            TableSnapshotStatisticsVersion::V2(_) => "_ts_v2.json".to_string(),
            TableSnapshotStatisticsVersion::V3(_) => "_ts_v3.json".to_string(),
            TableSnapshotStatisticsVersion::V4(_) => "_ts_v4.json".to_string(),
        }
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;

    use super::BASE62_ALPHABET;
    use super::TableMetaLocationGenerator;
    use super::compact_index_version;

    /// Decode a base-62 token back to its u128 value, so tests can assert round-trip fidelity.
    fn base62_decode(s: &str) -> u128 {
        let mut n: u128 = 0;
        for c in s.bytes() {
            let d = BASE62_ALPHABET.iter().position(|&a| a == c).unwrap() as u128;
            n = n * 62 + d;
        }
        n
    }

    #[test]
    fn test_granule_index_locations_derive_from_block_location() {
        let block = "1/2/_b/h0191114d30fd78b89fae8e5c88327725_v2.parquet";
        let mins = TableMetaLocationGenerator::gen_granule_mins_location_from_block_location(block);
        let offsets =
            TableMetaLocationGenerator::gen_granule_offsets_location_from_block_location(block);

        assert_eq!(
            mins.0,
            format!(
                "1/2/_i_p/h0191114d30fd78b89fae8e5c88327725_mins_v{}.parquet",
                mins.1
            )
        );
        assert_eq!(
            offsets.0,
            format!(
                "1/2/_i_p/h0191114d30fd78b89fae8e5c88327725_offsets_v{}.parquet",
                offsets.1
            )
        );
        assert_eq!(
            TableMetaLocationGenerator::gen_granule_index_locations_from_block_location(block),
            [mins.0, offsets.0]
        );
    }

    #[test]
    fn test_compact_index_version_roundtrip_and_bounds() {
        // A `simple()` UUID (32 hex, no dashes) round-trips through base-62 with no loss and stays
        // within 22 chars — the max for a 128-bit value.
        for _ in 0..1000 {
            let uuid = Uuid::new_v4();
            let token = compact_index_version(&uuid.simple().to_string());
            assert!(token.len() <= 22, "token {token} exceeds 22 chars");
            assert_eq!(base62_decode(&token), uuid.as_u128());
        }
    }

    #[test]
    fn test_compact_index_version_distinct() {
        // Two versions that share a long hex prefix (which a truncate-to-7 scheme would alias) must
        // still produce distinct tokens.
        let a = "00000000000000000000000000000001";
        let b = "00000000000000000000000000000002";
        assert_ne!(compact_index_version(a), compact_index_version(b));
    }

    #[test]
    fn test_compact_index_version_edge_cases() {
        // All-zero UUID -> "0".
        assert_eq!(
            compact_index_version("00000000000000000000000000000000"),
            "0"
        );
        // Non-UUID input is returned verbatim (deterministic fallback).
        assert_eq!(compact_index_version("not-a-uuid"), "not-a-uuid");
    }
}
