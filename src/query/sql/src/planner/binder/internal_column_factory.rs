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

use ctor::ctor;
use databend_common_catalog::plan::InternalColumn;
use databend_common_catalog::plan::InternalColumnType;
use databend_common_expression::BASE_BLOCK_IDS_COL_NAME;
use databend_common_expression::BASE_ROW_ID_COL_NAME;
use databend_common_expression::BLOCK_NAME_COL_NAME;
use databend_common_expression::CHANGE_ACTION_COL_NAME;
use databend_common_expression::CHANGE_IS_UPDATE_COL_NAME;
use databend_common_expression::CHANGE_ROW_ID_COL_NAME;
use databend_common_expression::ROW_ID_COL_NAME;
use databend_common_expression::SEGMENT_NAME_COL_NAME;
use databend_common_expression::SNAPSHOT_NAME_COL_NAME;

#[ctor]
pub static INTERNAL_COLUMN_FACTORY: InternalColumnFactory = InternalColumnFactory::init();

pub struct InternalColumnFactory {
    internal_columns: BTreeMap<String, InternalColumn>,
}

impl InternalColumnFactory {
    pub fn init() -> InternalColumnFactory {
        let mut internal_columns = BTreeMap::new();

        internal_columns.insert(
            ROW_ID_COL_NAME.to_string(),
            InternalColumn::new(ROW_ID_COL_NAME, InternalColumnType::RowId),
        );

        internal_columns.insert(
            BLOCK_NAME_COL_NAME.to_string(),
            InternalColumn::new(BLOCK_NAME_COL_NAME, InternalColumnType::BlockName),
        );

        internal_columns.insert(
            SEGMENT_NAME_COL_NAME.to_string(),
            InternalColumn::new(SEGMENT_NAME_COL_NAME, InternalColumnType::SegmentName),
        );

        internal_columns.insert(
            SNAPSHOT_NAME_COL_NAME.to_string(),
            InternalColumn::new(SNAPSHOT_NAME_COL_NAME, InternalColumnType::SnapshotName),
        );

        internal_columns.insert(
            BASE_ROW_ID_COL_NAME.to_string(),
            InternalColumn::new(BASE_ROW_ID_COL_NAME, InternalColumnType::BaseRowId),
        );

        internal_columns.insert(
            BASE_BLOCK_IDS_COL_NAME.to_string(),
            InternalColumn::new(BASE_BLOCK_IDS_COL_NAME, InternalColumnType::BaseBlockIds),
        );

        internal_columns.insert(
            CHANGE_ACTION_COL_NAME.to_string(),
            InternalColumn::new(CHANGE_ACTION_COL_NAME, InternalColumnType::ChangeAction),
        );

        internal_columns.insert(
            CHANGE_IS_UPDATE_COL_NAME.to_string(),
            InternalColumn::new(
                CHANGE_IS_UPDATE_COL_NAME,
                InternalColumnType::ChangeIsUpdate,
            ),
        );

        internal_columns.insert(
            CHANGE_ROW_ID_COL_NAME.to_string(),
            InternalColumn::new(CHANGE_ROW_ID_COL_NAME, InternalColumnType::ChangeRowId),
        );

        InternalColumnFactory { internal_columns }
    }

    pub fn get_internal_column(&self, name: &str) -> Option<InternalColumn> {
        self.internal_columns
            .get(name)
            .map(|internal_column| internal_column.to_owned())
    }
}
