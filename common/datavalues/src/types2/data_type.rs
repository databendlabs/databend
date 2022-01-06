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

use common_arrow::arrow::datatypes::DataType as ArrowType;
use common_arrow::arrow::datatypes::Field as ArrowField;

use super::type_id::TypeID;

pub trait IDataType {
    fn type_id(&self) -> TypeID;
    fn is_nullable(&self) -> bool {
        false
    }

    /// arrow_type did not have nullable sign, it's nullable sign is in the field
    fn arrow_type(&self) -> ArrowType;
    fn to_arrow_field(&self, name: &str) -> ArrowField {
        ArrowField::new(name, self.arrow_type(), self.is_nullable())
    }
}
