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

use std::pin::Pin;
use std::sync::Arc;

use databend_common_exception::ErrorCode;
use databend_storages_common_table_meta::meta::FormatVersion;
use databend_storages_common_table_meta::meta::TableSnapshot;
use futures_util::stream;
use log::info;
use opendal::Operator;

use crate::io::SnapshotCache;
use crate::io::SnapshotReader;
use crate::io::TableMetaLocationGenerator;

pub type TableSnapshotStream = Pin<
    Box<
        dyn stream::Stream<
                Item = databend_common_exception::Result<(Arc<TableSnapshot>, FormatVersion)>,
            > + Send,
    >,
>;

/// Walks the snapshot chain back from `location`, stopping at the first missing snapshot.
pub fn snapshot_history(
    cache: &SnapshotCache,
    operator: Operator,
    location: String,
    format_version: u64,
    location_gen: TableMetaLocationGenerator,
) -> TableSnapshotStream {
    let state = (
        cache.clone(),
        SnapshotReader::new(operator),
        location_gen,
        Some((location, format_version)),
    );
    let stream = stream::try_unfold(state, |(cache, reader, loc_gen, next)| async move {
        let Some((loc, ver)) = next else {
            return Ok(None);
        };
        let snapshot = match cache.get(&reader, loc.clone(), ver).await {
            Ok(s) => s,
            Err(e) if e.code() == ErrorCode::STORAGE_NOT_FOUND => {
                info!(
                    "traverse snapshot history break at location ({}, {}), err {}",
                    loc, ver, e
                );
                return Ok(None);
            }
            Err(e) => return Err(e),
        };

        let next = snapshot
            .prev_snapshot_id
            .map(|(prev_id, prev_ver)| {
                let next_loc = loc_gen.gen_snapshot_location(&prev_id, prev_ver);
                next_loc.map(|loc| (loc, prev_ver))
            })
            .transpose()?;

        Ok(Some(((snapshot, ver), (cache, reader, loc_gen, next))))
    });
    Box::pin(stream)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use databend_common_exception::Result;
    use databend_common_expression::TableSchema;
    use databend_storages_common_cache::CacheKind;
    use databend_storages_common_cache::Reader;
    use databend_storages_common_cache::cache_manager::CacheManager;
    use databend_storages_common_cache::legacy::LegacyCapacity;
    use databend_storages_common_cache::legacy::LegacyStore;
    use databend_storages_common_table_meta::meta::Statistics;
    use databend_storages_common_table_meta::meta::TableMetaTimestamps;
    use databend_storages_common_table_meta::meta::Versioned;
    use futures_util::TryStreamExt;
    use opendal::services::Memory;
    use uuid::Uuid;

    use super::*;
    use crate::io::FuseCaches;

    fn caches() -> Arc<FuseCaches> {
        let store = Arc::new(LegacyStore::new([(
            CacheKind::TableSnapshot,
            LegacyCapacity::Items(10),
        )]));
        FuseCaches::new(&CacheManager::new(store, HashMap::new()))
    }

    fn snapshot(prev: Option<Arc<TableSnapshot>>) -> Arc<TableSnapshot> {
        let timestamps = TableMetaTimestamps::new(None, chrono::Duration::hours(1));
        let snapshot = TableSnapshot::try_new(
            None,
            prev,
            TableSchema::empty(),
            Statistics::default(),
            vec![],
            None,
            None,
            timestamps,
        )
        .unwrap();
        Arc::new(snapshot)
    }

    /// Writes `first <- second` and returns their locations.
    async fn write_chain(
        operator: &Operator,
        location_gen: &TableMetaLocationGenerator,
    ) -> Result<[String; 2]> {
        let first = snapshot(None);
        let second = snapshot(Some(first.clone()));
        let mut locations = Vec::new();
        for s in [first, second] {
            let location =
                location_gen.gen_snapshot_location(&s.snapshot_id, TableSnapshot::VERSION)?;
            operator.write(&location, s.to_bytes()?).await?;
            locations.push(location);
        }
        Ok([locations[0].clone(), locations[1].clone()])
    }

    async fn history(
        caches: &FuseCaches,
        operator: &Operator,
        location_gen: &TableMetaLocationGenerator,
        head: &str,
    ) -> Result<Vec<Uuid>> {
        let stream = snapshot_history(
            &caches.snapshots,
            operator.clone(),
            head.to_string(),
            TableSnapshot::VERSION,
            location_gen.clone(),
        );
        let snapshots: Vec<_> = stream.try_collect().await?;
        Ok(snapshots.iter().map(|(s, _)| s.snapshot_id).collect())
    }

    #[tokio::test]
    async fn test_snapshot_reader_decodes_and_reports_not_found() -> Result<()> {
        let operator = Operator::new(Memory::default())?.finish();
        let location_gen = TableMetaLocationGenerator::new("1/2".to_string());
        let [first, _] = write_chain(&operator, &location_gen).await?;

        let reader = SnapshotReader::new(operator);
        let decoded = reader.read(&(first, TableSnapshot::VERSION)).await?;
        assert!(decoded.prev_snapshot_id.is_none());

        let missing = reader
            .read(&("1/2/missing".to_string(), TableSnapshot::VERSION))
            .await;
        assert_eq!(missing.unwrap_err().code(), ErrorCode::STORAGE_NOT_FOUND);
        Ok(())
    }

    #[tokio::test]
    async fn test_snapshot_history_reads_through_cache() -> Result<()> {
        let operator = Operator::new(Memory::default())?.finish();
        let location_gen = TableMetaLocationGenerator::new("1/2".to_string());
        let [first, second] = write_chain(&operator, &location_gen).await?;
        let caches = caches();

        let ids = history(&caches, &operator, &location_gen, &second).await?;
        assert_eq!(ids.len(), 2);

        // Served from the cache once the files are gone.
        operator.delete(&first).await?;
        operator.delete(&second).await?;
        assert_eq!(
            history(&caches, &operator, &location_gen, &second).await?,
            ids
        );

        // A missing predecessor ends the walk instead of failing it.
        caches.snapshots.evict(&first);
        assert_eq!(
            history(&caches, &operator, &location_gen, &second).await?,
            ids[..1]
        );
        Ok(())
    }
}
