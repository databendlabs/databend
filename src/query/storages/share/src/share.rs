// Copyright 2022 Datafuse Labs.
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

use std::collections::BTreeMap;

use common_exception::Result;
use common_meta_app::share::ShareSpec;
use opendal::Operator;

pub const SHARE_CONFIG_PREFIX: &str = "_share_config";

#[derive(serde::Serialize, serde::Deserialize, Clone, Debug, Default, Eq, PartialEq)]
pub struct ShareSpecVec {
    share_specs: BTreeMap<u64, ShareSpec>,
}

pub async fn save_share_spec(
    tenant: String,
    share_id: u64,
    operator: Operator,
    spec: Option<ShareSpec>,
) -> Result<()> {
    let location = format!("{}/{}/share_specs.json", SHARE_CONFIG_PREFIX, tenant);

    if let Some(spec) = spec {
        let data = operator.object(&location).read().await?;
        let mut spec_vec: ShareSpecVec = serde_json::from_slice(&data)?;
        spec_vec.share_specs.remove(&share_id);
        operator
            .object(&location)
            .write(serde_json::to_vec(&spec)?)
            .await?;
    } else {
        operator.object(&location).delete().await?;
    }

    Ok(())
}
