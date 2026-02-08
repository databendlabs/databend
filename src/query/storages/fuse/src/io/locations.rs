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
use crate::FUSE_TBL_INVERTED_INDEX_PREFIX;
use crate::FUSE_TBL_LAST_SNAPSHOT_HINT_V2;
use crate::FUSE_TBL_REF_PREFIX;
use crate::FUSE_TBL_SEGMENT_STATISTICS_PREFIX;
use crate::FUSE_TBL_VECTOR_INDEX_PREFIX;
use crate::FUSE_TBL_XOR_BLOOM_INDEX_PREFIX;
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
    segment_statistics_location_prefix: String,
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
        let segment_statistics_location_prefix =
            format!("{}/{}/", &prefix, FUSE_TBL_SEGMENT_STATISTICS_PREFIX);
        let ref_snapshot_location_prefix = format!("{}/{}/", &prefix, FUSE_TBL_REF_PREFIX);
        Self {
            prefix,
            block_location_prefix,
            segment_info_location_prefix,
            bloom_index_location_prefix,
            snapshot_location_prefix,
            agg_index_location_prefix,
            inverted_index_location_prefix,
            vector_index_location_prefix,
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

    pub fn gen_snapshot_location(
        &self,
        branch_id: Option<u64>,
        id: &Uuid,
        version: u64,
    ) -> Result<String> {
        let snapshot_version = SnapshotVersion::try_from(version)?;
        let location = match branch_id {
            Some(branch) => snapshot_version.create_ref(branch, id, &self.prefix),
            _ => snapshot_version.create(id, &self.prefix),
        };
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

trait SnapshotLocationCreator {
    fn create(&self, id: &Uuid, prefix: impl AsRef<str>) -> String;
    fn create_ref(&self, table_ref: u64, id: &Uuid, prefix: impl AsRef<str>) -> String;
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

    fn create_ref(&self, table_ref: u64, id: &Uuid, prefix: impl AsRef<str>) -> String {
        format!(
            "{}/{}/{}/{}{}{}",
            prefix.as_ref(),
            FUSE_TBL_REF_PREFIX,
            table_ref,
            VACUUM2_OBJECT_KEY_PREFIX,
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

    fn create_ref(&self, _table_ref: u64, _id: &Uuid, _prefix: impl AsRef<str>) -> String {
        unimplemented!()
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
