// Copyright 2023 Datafuse Labs.
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
use std::sync::Arc;

use common_base::base::GlobalInstance;
use common_catalog::plan::InternalColumn;
use common_catalog::plan::InternalColumnType;
use common_catalog::plan::BLOCK_NAME;
use common_catalog::plan::ROW_ID;
use common_catalog::plan::SEGMENT_NAME;
use common_catalog::plan::SNAPSHOT_NAME;
use common_exception::Result;
use common_expression::types::DataType;

pub struct InternalColumnFactory {
    virtual_columns: BTreeMap<String, InternalColumn>,
}

impl InternalColumnFactory {
    pub fn init() -> Result<()> {
        let mut virtual_columns = BTreeMap::new();

        virtual_columns.insert(
            ROW_ID.to_string(),
            InternalColumn::new(ROW_ID, InternalColumnType::RowId),
        );

        virtual_columns.insert(
            BLOCK_NAME.to_string(),
            InternalColumn::new(BLOCK_NAME, InternalColumnType::BlockName),
        );

        virtual_columns.insert(
            SEGMENT_NAME.to_string(),
            InternalColumn::new(SEGMENT_NAME, InternalColumnType::SegmentName),
        );

        virtual_columns.insert(
            SNAPSHOT_NAME.to_string(),
            InternalColumn::new(SNAPSHOT_NAME, InternalColumnType::SnapshotName),
        );

        GlobalInstance::set(Arc::new(InternalColumnFactory { virtual_columns }));
        Ok(())
    }

    pub fn instance() -> Arc<InternalColumnFactory> {
        GlobalInstance::get()
    }

    pub fn get_data_type(&self, name: &str) -> Option<DataType> {
        self.virtual_columns
            .get(name)
            .map(|virtual_column| virtual_column.data_type())
    }

    pub fn get_virtual_column(&self, name: &str) -> Option<InternalColumn> {
        self.virtual_columns
            .get(name)
            .map(|virtual_column| virtual_column.to_owned())
    }

    pub fn all_virtual_columns(&self) -> Vec<InternalColumn> {
        self.virtual_columns.values().cloned().collect()
    }

    pub fn virtual_columns(&self) -> &BTreeMap<String, InternalColumn> {
        &self.virtual_columns
    }
}
