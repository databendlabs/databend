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

use databend_common_catalog::catalog::CATALOG_DEFAULT;
use databend_common_exception::ErrorCode;
use databend_common_exception::Result;
use databend_common_meta_app::schema::DmlDeduplicationId;
use databend_common_meta_app::schema::DmlDeduplicationOperation;
use databend_common_storages_fuse::FuseTable;

use crate::sessions::TableContext;

pub enum DmlDeduplication {
    AlreadyApplied,
    Proceed(Option<DmlDeduplicationId>),
}

/// Call after the interpreter factory authorizes the statement and the target
/// is resolved. Carry the returned identity unchanged to the commit sink.
pub async fn check_dml_deduplication(
    ctx: &dyn TableContext,
    table: &dyn databend_common_catalog::table::Table,
    operation: DmlDeduplicationOperation,
) -> Result<DmlDeduplication> {
    let Some(token) = (unsafe { ctx.get_settings().get_deduplicate_label()? }) else {
        return Ok(DmlDeduplication::Proceed(None));
    };
    let info = table.get_table_info();
    if info.catalog() != CATALOG_DEFAULT || table.as_any().downcast_ref::<FuseTable>().is_none() {
        return Err(ErrorCode::InvalidOperation(
            "deduplicate_label requires a Fuse table in the default catalog",
        ));
    }

    let id = DmlDeduplicationId {
        principal: ctx.get_current_user()?.identity(),
        catalog_id: info.catalog_info.id.catalog_id,
        table_id: info.ident.table_id,
        operation,
        token,
    };
    let tenant = ctx.get_tenant();
    let local_match = {
        let txn_mgr = ctx.txn_mgr();
        let txn = txn_mgr.lock();
        txn.is_active()
            .then(|| txn.tenant().as_ref() == Some(&tenant) && txn.contains_deduplicated_label(&id))
    };
    let already_applied = if let Some(local_match) = local_match {
        local_match
    } else if table.is_temp() {
        false
    } else {
        ctx.get_default_catalog()?
            .has_dml_deduplication_marker(&tenant, &id)
            .await?
    };
    Ok(if already_applied {
        DmlDeduplication::AlreadyApplied
    } else {
        DmlDeduplication::Proceed(Some(id))
    })
}
