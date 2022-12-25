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

use std::io::Error;

use backon::ExponentialBackoff;
use backon::Retryable;
use common_exception::Result;
use opendal::Operator;
use serde::Serialize;
use tracing::warn;

pub async fn write_meta<T>(data_accessor: &Operator, location: &str, meta: T) -> Result<()>
where T: Serialize {
    let bs = serde_json::to_vec(&meta).map_err(Error::other)?;
    let object = data_accessor.object(location);
    { || object.write(bs.as_slice()) }
        .retry(ExponentialBackoff::default().with_jitter())
        .when(|err| err.is_temporary())
        .notify(|err, dur| {
            warn!(
                "write_meta retry after {}s for error {:?}",
                dur.as_secs(),
                err
            )
        })
        .await?;
    Ok(())
}
