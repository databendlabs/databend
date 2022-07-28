//  Copyright 2022 Datafuse Labs.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

use std::pin::Pin;
use std::sync::Arc;

use common_exception::ErrorCode;
use common_fuse_meta::meta::TableSnapshot;
use futures_util::stream;

use crate::storages::fuse::io::TableMetaLocationGenerator;
use crate::storages::fuse::io::TableSnapshotReader;

impl<'a> TableSnapshotReader<'a> {
    pub async fn read_snapshot_history(
        &self,
        latest_snapshot_location: Option<impl AsRef<str>>,
        format_version: u64,
        location_gen: TableMetaLocationGenerator,
        limit: Option<usize>,
    ) -> common_exception::Result<Vec<Arc<TableSnapshot>>> {
        let mut snapshots = vec![];
        let mut l = limit.map(|v| v as u64).unwrap_or(u64::MAX);

        if l == 0 {
            return Ok(snapshots);
        }

        if let Some(loc) = latest_snapshot_location {
            let mut ver = format_version;
            let mut loc = loc.as_ref().to_string();
            loop {
                let snapshot = match self.read(loc, None, ver).await {
                    Ok(s) => s,
                    Err(e) => {
                        if e.code() == ErrorCode::storage_not_found_code() {
                            break;
                        } else {
                            return Err(e);
                        }
                    }
                };
                if let Some((id, v)) = snapshot.prev_snapshot_id {
                    ver = v;
                    loc = location_gen.snapshot_location_from_uuid(&id, v)?;
                    snapshots.push(snapshot);

                    // break if reach the limit
                    l -= 1;
                    if l == 0 {
                        break;
                    }
                } else {
                    snapshots.push(snapshot);
                    break;
                }
            }
        }

        Ok(snapshots)
    }

    pub fn snapshot_history(
        &'a self,
        location: String,
        format_version: u64,
        location_gen: TableMetaLocationGenerator,
    ) -> Pin<
        Box<
            dyn futures::stream::Stream<Item = common_exception::Result<Arc<TableSnapshot>>>
                + 'a
                + Send,
        >,
    > {
        let stream = stream::try_unfold(
            (self, location_gen, Some((location, format_version))),
            |(reader, gen, next)| async move {
                if let Some((loc, ver)) = next {
                    let snapshot = match reader.read(loc, None, ver).await {
                        Ok(s) => Ok(Some(s)),
                        Err(e) => {
                            if e.code() == ErrorCode::storage_not_found_code() {
                                Ok(None)
                            } else {
                                Err(e)
                            }
                        }
                    };
                    match snapshot {
                        Ok(Some(snapshot)) => {
                            if let Some((id, v)) = snapshot.prev_snapshot_id {
                                let new_ver = v;
                                let new_loc = gen.snapshot_location_from_uuid(&id, v)?;
                                Ok(Some((snapshot, (reader, gen, Some((new_loc, new_ver))))))
                            } else {
                                Ok(Some((snapshot, (reader, gen, None))))
                            }
                        }
                        Ok(None) => Ok(None),
                        Err(e) => Err(e),
                    }
                } else {
                    Ok(None)
                }
            },
        );
        Box::pin(stream)
    }
}
