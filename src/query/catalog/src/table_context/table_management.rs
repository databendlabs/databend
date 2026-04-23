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

use std::sync::Arc;

use databend_common_exception::Result;
use databend_common_expression::TableSchema;
use databend_common_meta_app::principal::OnErrorMode;
use databend_common_meta_app::principal::StageInfo;
use databend_common_meta_app::storage::StorageParams;
use databend_common_storage::StageFileInfo;
use databend_common_storage::StageFilesInfo;
use databend_storages_common_table_meta::meta::TableMetaTimestamps;
use databend_storages_common_table_meta::meta::TableSnapshot;

use crate::table::Table;

#[async_trait::async_trait]
pub trait TableContextTableManagement: Send + Sync {
    fn evict_table_from_cache(
        &self,
        catalog: &str,
        database: &str,
        table: &str,
        branch: Option<&str>,
    ) -> Result<()>;

    fn get_table_meta_timestamps(
        &self,
        table: &dyn Table,
        previous_snapshot: Option<Arc<TableSnapshot>>,
    ) -> Result<TableMetaTimestamps>;

    async fn load_datalake_schema(
        &self,
        kind: &str,
        sp: &StorageParams,
    ) -> Result<(TableSchema, String)> {
        let _ = (kind, sp);
        unimplemented!()
    }

    async fn create_stage_table(
        &self,
        stage_info: StageInfo,
        files_info: StageFilesInfo,
        files_to_copy: Option<Vec<StageFileInfo>>,
        max_column_position: usize,
        on_error_mode: Option<OnErrorMode>,
    ) -> Result<Arc<dyn Table>> {
        let _ = (
            stage_info,
            files_info,
            files_to_copy,
            max_column_position,
            on_error_mode,
        );
        unimplemented!()
    }
}
