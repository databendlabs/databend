// Copyright 2020 Datafuse Labs.
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

use serde_json;

use crate::cmds::clusters::cluster::ClusterProfile;
use crate::cmds::Status;
use crate::error::CliError;
use crate::error::Result;

// TODO(zhihanz) unit tests
pub fn get_profile(status: Status, profile: Option<&str>) -> Result<ClusterProfile> {
    match profile {
        Some("local") => Ok(ClusterProfile::Local),
        Some("cluster") => Ok(ClusterProfile::Cluster),
        None => {
            if status.current_profile.is_none() {
                return Err(CliError::Unknown(
                    "Currently there is no profile in use, please create or use a profile"
                        .parse()
                        .unwrap(),
                ));
            }
            Ok(serde_json::from_str::<ClusterProfile>(
                &*status.current_profile.unwrap(),
            )?)
        }
        _ => Err(CliError::Unknown(
            "Currently there is no profile in use, please create or use a profile"
                .parse()
                .unwrap(),
        )),
    }
}
