//  Copyright 2021 Datafuse Labs.
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
//

use common_dal::DataAccessor;
use common_exception::ErrorCode;
use common_exception::Result;
use serde::de::DeserializeOwned;

use crate::storages::fuse::io::locations::snapshot_location;
use crate::storages::fuse::meta::TableSnapshot;

pub async fn read_obj<T: DeserializeOwned>(
    da: &dyn DataAccessor,
    loc: impl AsRef<str>,
) -> Result<T> {
    let bytes = da.read(loc.as_ref()).await?;
    let t = serde_json::from_slice::<T>(&bytes)?;
    Ok(t)
}

pub async fn snapshot_history(
    data_accessor: &dyn DataAccessor,
    latest_snapshot_location: Option<&String>,
) -> Result<Vec<TableSnapshot>> {
    let mut snapshots = vec![];
    let mut current_snapshot_location = latest_snapshot_location.cloned();
    while let Some(loc) = current_snapshot_location {
        let r: Result<TableSnapshot> = read_obj(data_accessor, loc).await;

        let snapshot = match r {
            Ok(s) => s,
            Err(e) if e.code() == ErrorCode::DALPathNotFoundCode() => {
                // snapshot has been truncated
                break;
            }
            Err(e) => return Err(e),
        };
        let prev = snapshot.prev_snapshot_id;
        snapshots.push(snapshot);
        current_snapshot_location = prev.map(|id| snapshot_location(&id));
    }
    Ok(snapshots)
}
