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

use common_catalog::plan::Projection;
use common_catalog::plan::VirtualColumnInfo;
use common_catalog::table_context::TableContext;
use common_exception::Result;
use common_expression::TableSchemaRef;
use common_expression::TableSchemaRefExt;
use opendal::Operator;
use storages_common_table_meta::table::TableCompression;

use crate::io::BlockReader;

#[derive(Clone)]
pub struct VirtualColumnReader {
    pub(super) ctx: Arc<dyn TableContext>,
    pub(super) dal: Operator,
    pub(super) source_schema: TableSchemaRef,
    pub(super) reader: Arc<BlockReader>,
    pub(super) compression: TableCompression,
    pub virtual_column_infos: Vec<VirtualColumnInfo>,
}

impl VirtualColumnReader {
    pub fn try_create(
        ctx: Arc<dyn TableContext>,
        dal: Operator,
        source_schema: TableSchemaRef,
        virtual_column_infos: Vec<VirtualColumnInfo>,
        compression: TableCompression,
    ) -> Result<Self> {
        let reader = BlockReader::create(
            dal.clone(),
            TableSchemaRefExt::create(vec![]),
            Projection::Columns(vec![]),
            ctx.clone(),
            false,
        )?;

        Ok(Self {
            ctx,
            dal,
            source_schema,
            reader,
            compression,
            virtual_column_infos,
        })
    }
}
