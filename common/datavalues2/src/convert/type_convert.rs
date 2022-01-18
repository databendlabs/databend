// Copyright 2021 Datafuse Labs.
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
// limitations under the License.pub use data_type::*;

use common_datavalues::prelude::DataField as OldDataField;

use crate::from_arrow_field;
use crate::DataField;

impl From<OldDataField> for DataField {
    fn from(old: OldDataField) -> Self {
        let arrow_field = old.to_arrow();
        let new_type = from_arrow_field(&arrow_field);

        DataField::new(old.name(), new_type)
    }
}

impl From<DataField> for OldDataField {
    fn from(field: DataField) -> OldDataField {
        let arrow_field = field.to_arrow();

        From::from(&arrow_field)
    }
}
