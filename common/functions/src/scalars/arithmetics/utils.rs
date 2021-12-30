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
// limitations under the License.

use common_datavalues::prelude::DataColumnWithField;

pub fn validate_input<'a>(
    col0: &'a DataColumnWithField,
    col1: &'a DataColumnWithField,
) -> (&'a DataColumnWithField, &'a DataColumnWithField) {
    if col0.data_type().is_integer() || col0.data_type().is_interval() {
        (col0, col1)
    } else {
        (col1, col0)
    }
}
