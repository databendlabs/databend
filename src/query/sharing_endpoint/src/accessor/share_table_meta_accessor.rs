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

use std::collections::BTreeMap;

use bytes::Buf;
use databend_common_exception::Result;
use databend_common_meta_app::share::TableInfoMap;
use databend_common_storages_share::share_table_info_location;

use crate::accessor::SharingAccessor;
use crate::models::TableMetaLambdaInput;

// Methods for access share table meta.
impl SharingAccessor {
    #[async_backtrace::framed]
    pub async fn get_share_table_meta(input: &TableMetaLambdaInput) -> Result<TableInfoMap> {
        let sharing_accessor = Self::instance();
        let share_table_meta_loc =
            share_table_info_location(&sharing_accessor.config.tenant, &input.share_name);
        let data = sharing_accessor.op.read(&share_table_meta_loc).await?;
        let share_table_map: TableInfoMap = serde_json::from_reader(data.reader())?;

        if input.request_tables.is_empty() {
            Ok(share_table_map)
        } else {
            Ok(BTreeMap::from_iter(share_table_map.into_iter().filter(
                |(table_name, _table_info)| input.request_tables.contains(table_name),
            )))
        }
    }
}
