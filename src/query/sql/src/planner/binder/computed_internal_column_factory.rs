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
use databend_common_expression::CHANGE_ROW_ID_COL_NAME;
use databend_common_expression::CHANGE_ROW_ID_COLUMN_ID;
use databend_common_expression::ColumnId;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ComputedInternalColumn {
    ChangeRowId,
}

impl ComputedInternalColumn {
    pub fn name(&self) -> &'static str {
        match self {
            ComputedInternalColumn::ChangeRowId => CHANGE_ROW_ID_COL_NAME,
        }
    }

    pub fn column_id(&self) -> ColumnId {
        match self {
            ComputedInternalColumn::ChangeRowId => CHANGE_ROW_ID_COLUMN_ID,
        }
    }
}

#[ctor]
pub static COMPUTED_INTERNAL_COLUMN_FACTORY: ComputedInternalColumnFactory =
    ComputedInternalColumnFactory::init();

pub struct ComputedInternalColumnFactory {
    columns: BTreeMap<&'static str, ComputedInternalColumn>,
}

impl ComputedInternalColumnFactory {
    fn init() -> Self {
        Self {
            columns: BTreeMap::from([(
                CHANGE_ROW_ID_COL_NAME,
                ComputedInternalColumn::ChangeRowId,
            )]),
        }
    }

    pub fn get(&self, name: &str) -> Option<ComputedInternalColumn> {
        self.columns.get(name).cloned()
    }
}
