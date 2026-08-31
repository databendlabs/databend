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

use databend_common_catalog::table_context::TableContext;
use databend_common_exception::Result;
use databend_common_meta_app::schema::TableMeta;
use databend_storages_common_table_meta::meta::TableSnapshot;
use databend_storages_common_table_meta::table::OPT_KEY_CHANGE_TRACKING_BEGIN_VER;
use databend_storages_common_table_meta::table::OPT_KEY_DATABASE_ID;
use databend_storages_common_table_meta::table::OPT_KEY_LEGACY_SNAPSHOT_LOC;
use databend_storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION;
use databend_storages_common_table_meta::table::OPT_KEY_SNAPSHOT_LOCATION_FIXED_FLAG;

use crate::FuseTable;

impl FuseTable {
    /// Build metadata for a zero-copy clone before its target-owned snapshot anchor is created.
    ///
    /// The source table provides inheritable metadata, while source identity and data-root state
    /// are cleared here. The create interpreter installs the target clone group, temporary source
    /// root protection, target-owned anchor, timestamps, and change-tracking boundary.
    pub async fn build_clone_table_meta(
        &self,
        ctx: Arc<dyn TableContext>,
        target_database_id: String,
        snapshot: Option<&TableSnapshot>,
    ) -> Result<TableMeta> {
        let mut table_meta = self.table_info.meta.clone();

        // These values identify the source table or one of its snapshot roots and must never be
        // inherited by the target table.
        table_meta.part_prefix.clear();
        table_meta.drop_on = None;
        table_meta.options.remove(OPT_KEY_SNAPSHOT_LOCATION);
        table_meta.options.remove(OPT_KEY_LEGACY_SNAPSHOT_LOC);
        table_meta
            .options
            .remove(OPT_KEY_SNAPSHOT_LOCATION_FIXED_FLAG);
        table_meta.options.remove(OPT_KEY_CHANGE_TRACKING_BEGIN_VER);
        table_meta
            .options
            .insert(OPT_KEY_DATABASE_ID.to_string(), target_database_id);

        if let Some(snapshot) = snapshot {
            self.apply_snapshot_versioned_metadata_to_meta(&mut table_meta, snapshot)?;
            Self::apply_snapshot_statistics(&mut table_meta, snapshot);
        }

        Self::prepare_persistent_navigation_metadata(&mut table_meta);
        self.validate_persistent_navigation_metadata(ctx, &table_meta, "clone")
            .await?;
        Ok(table_meta)
    }
}
