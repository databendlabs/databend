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

use databend_common_catalog::table_context::TableContext;
use databend_common_meta_app::share::ShareTableInfoMap;

use crate::sessions::QueryContext;

pub async fn save_share_table_info(
    ctx: &QueryContext,
    share_table_info: &Option<Vec<ShareTableInfoMap>>,
) -> databend_common_exception::Result<()> {
    if let Some(share_table_info) = share_table_info {
        databend_common_storages_share::save_share_table_info(
            ctx.get_tenant().tenant_name(),
            ctx.get_application_level_data_operator()?.operator(),
            share_table_info,
        )
        .await?;
    }
    Ok(())
}
