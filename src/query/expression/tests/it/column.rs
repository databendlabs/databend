// Copyright 2022 Datafuse Labs.
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

use databend_common_arrow::arrow::array::new_empty_array;
use databend_common_arrow::arrow::datatypes::DataType as ArrowDataType;
use databend_common_exception::Result;
use databend_common_expression::types::DataType;
use databend_common_expression::types::NumberDataType;
use databend_common_expression::Column;

#[test]
fn test_from_arrow_extension_to_column() -> Result<()> {
    let data_type = DataType::Number(NumberDataType::Int8);
    let extension_data_type =
        ArrowDataType::Extension("a".to_string(), Box::new(ArrowDataType::Int8), None);

    let arrow_col = new_empty_array(extension_data_type);
    let _ = Column::from_arrow(arrow_col.as_ref(), &data_type);

    Ok(())
}
