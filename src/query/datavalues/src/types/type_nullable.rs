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

use super::data_type::DataType;
use super::data_type::DataTypeImpl;
use super::type_id::TypeID;

#[derive(Clone, Hash, serde::Deserialize, serde::Serialize)]
pub struct NullableType {
    inner: Box<DataTypeImpl>,
}

impl NullableType {
    pub fn new_impl(inner: DataTypeImpl) -> DataTypeImpl {
        DataTypeImpl::Nullable(Self::create(inner))
    }

    pub fn create(inner: DataTypeImpl) -> Self {
        debug_assert!(
            inner.can_inside_nullable(),
            "{} can't be inside of nullable.",
            inner.name()
        );
        NullableType {
            inner: Box::new(inner),
        }
    }

    pub fn inner_type(&self) -> &DataTypeImpl {
        &self.inner
    }
}

impl DataType for NullableType {
    fn data_type_id(&self) -> TypeID {
        TypeID::Nullable
    }

    fn name(&self) -> String {
        format!("Nullable({})", self.inner.name())
    }

    fn is_nullable(&self) -> bool {
        true
    }

    fn can_inside_nullable(&self) -> bool {
        false
    }
}

impl std::fmt::Debug for NullableType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}
