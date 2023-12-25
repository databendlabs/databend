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

use arrow_schema::Schema as ArrowSchema;
use databend_common_arrow::arrow::datatypes::Field as Arrow2Field;

use crate::TableField;
use crate::TableSchema;

impl From<&ArrowSchema> for TableSchema {
    fn from(a_schema: &ArrowSchema) -> TableSchema {
        let fields = a_schema
            .fields
            .iter()
            .map(|arrow_f| TableField::from(&Arrow2Field::from(arrow_f)))
            .collect();
        TableSchema::new(fields)
    }
}
