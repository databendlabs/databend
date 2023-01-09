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

use super::data_type::DataType;
use super::data_type::DataTypeImpl;
use super::type_id::TypeID;

#[derive(Clone, Hash, serde::Deserialize, serde::Serialize)]
pub struct ArrayType {
    inner: Box<DataTypeImpl>,
}

impl ArrayType {
    pub fn new_impl(inner: DataTypeImpl) -> DataTypeImpl {
        DataTypeImpl::Array(Self::create(inner))
    }

    pub fn create(inner: DataTypeImpl) -> Self {
        ArrayType {
            inner: Box::new(inner),
        }
    }

    pub fn inner_type(&self) -> &DataTypeImpl {
        &self.inner
    }
}

impl DataType for ArrayType {
    fn data_type_id(&self) -> TypeID {
        TypeID::Array
    }

    fn name(&self) -> String {
        format!("Array({})", self.inner.name())
    }

    fn arrow_type(&self) -> ArrowType {
        let field = Field::new(
            "_array".to_string(),
            self.inner.arrow_type(),
            self.inner.is_nullable(),
        );
        ArrowType::LargeList(Box::new(field))
    }
}

impl std::fmt::Debug for ArrayType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}
