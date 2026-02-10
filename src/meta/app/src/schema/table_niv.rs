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

use databend_meta_types::SeqV;

use crate::schema::DBIdTableName;
use crate::schema::TableId;
use crate::schema::TableMeta;

/// The **Name, ID, Value** for a table metadata stored in meta-service.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableNIV {
    name: DBIdTableName,
    id: TableId,
    value: SeqV<TableMeta>,
}

impl TableNIV {
    pub fn new(name: DBIdTableName, id: TableId, value: SeqV<TableMeta>) -> Self {
        TableNIV { name, id, value }
    }

    pub fn name(&self) -> &DBIdTableName {
        &self.name
    }

    pub fn id(&self) -> &TableId {
        &self.id
    }

    pub fn value(&self) -> &SeqV<TableMeta> {
        &self.value
    }

    pub fn unpack(self) -> (DBIdTableName, TableId, SeqV<TableMeta>) {
        (self.name, self.id, self.value)
    }
}
