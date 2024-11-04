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

use databend_common_exception::Result;
use databend_common_meta_app::principal::UserSetting;
use databend_common_meta_types::seq_value::SeqV;
use databend_common_meta_types::MatchSeq;

#[async_trait::async_trait]
pub trait SettingApi: Sync + Send {
    /// Add a setting to /tenant/cluster/setting-name.
    /// Returns the `seq` of the updated setting.
    async fn set_setting(&self, setting: UserSetting) -> Result<u64>;

    /// Get all the settings for tenant/cluster.
    async fn get_settings(&self) -> Result<Vec<UserSetting>>;

    async fn get_setting(&self, name: &str, seq: MatchSeq) -> Result<SeqV<UserSetting>>;

    /// Drop the setting by name.
    async fn try_drop_setting(&self, name: &str, seq: MatchSeq) -> Result<()>;
}
