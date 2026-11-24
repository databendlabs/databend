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

use crate::tenant_key::ident::TIdent;
use crate::tenant_key::resource::TenantResource;

/// The tenant-scoped key of a committed DML deduplication label.
pub type TableDeduplicationLabelIdent = TIdent<TableDeduplicationLabelResource>;

pub struct TableDeduplicationLabelResource;

impl TenantResource for TableDeduplicationLabelResource {
    const PREFIX: &'static str = "__fd_table_deduplication_label";
    const HAS_TENANT: bool = true;
    type ValueType = Vec<u8>;
}
