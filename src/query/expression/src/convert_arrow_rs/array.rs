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

use arrow_array::Array;
use arrow_schema::ArrowError;

use crate::Column;
use crate::DataField;

impl Column {
    pub fn into_arrow_rs(self) -> Result<Arc<dyn Array>, ArrowError> {
        let arrow2_array = self.as_arrow();
        let arrow_array: Arc<dyn Array> = arrow2_array.into();
        Ok(arrow_array)
    }

    pub fn from_arrow_rs(array: Arc<dyn Array>, field: &DataField) -> Result<Self, ArrowError> {
        let arrow2_array: Box<dyn databend_common_arrow::arrow::array::Array> = array.into();

        Ok(Column::from_arrow(arrow2_array.as_ref(), field.data_type()))
    }
}
