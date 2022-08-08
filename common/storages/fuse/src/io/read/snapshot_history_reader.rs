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

use crate::io::TableMetaLocationGenerator;
use crate::io::TableSnapshotReader;

pub type TableSnapshotStream =
    Pin<Box<dyn stream::Stream<Item = common_exception::Result<Arc<TableSnapshot>>> + Send>>;
impl TableSnapshotReader {
    pub fn snapshot_history(
        self,
        location: String,
        format_version: u64,
        location_gen: TableMetaLocationGenerator,
    ) -> TableSnapshotStream {
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
