// Copyright 2021 Datafuse Labs.
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

use common_exception::Result;
use common_tracing::tracing::warn;
use opendal::Operator;
use serde::Serialize;

use crate::storages::fuse::io::retry;
use crate::storages::fuse::io::retry::Retryable;

pub async fn write_meta<T>(data_accessor: &Operator, location: &str, meta: &T) -> Result<()>
where T: Serialize {
    let bytes = &serde_json::to_vec(&meta)?;
    let op = || async {
        data_accessor.object(location).write(bytes).await?;
        data_accessor
            .object(location)
            .write(bytes)
            .await
            .map_err(retry::from_io_error)
    };

    let notify = |e: std::io::Error, duration| {
        warn!(
            "transient error encountered while writing meta, location {}, at duration {:?} : {}",
            location, duration, e,
        )
    };

    op.retry_with_notify(notify).await?;
    Ok(())
}
