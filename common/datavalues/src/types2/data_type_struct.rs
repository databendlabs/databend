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
use common_arrow::arrow::datatypes::Field;

use super::data_type::IDataType;
use super::type_id::TypeID;

pub struct DataTypeDateStruct {
    names: Vec<String>,
    types: Vec<Box<dyn IDataType>>,
}

impl IDataType for DataTypeDateStruct {
    fn type_id(&self) -> TypeID {
        TypeID::Struct
    }

    fn arrow_type(&self) -> ArrowType {
        let fields = self
            .names
            .iter()
            .zip(self.types.iter())
            .map(|(name, type_)| type_.to_arrow_field(name))
            .collect();

        ArrowType::Struct(fields)
    }
}
