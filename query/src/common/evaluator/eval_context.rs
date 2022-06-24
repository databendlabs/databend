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

use std::fmt::Debug;

use common_datablocks::DataBlock;
use common_datavalues::ColumnRef;
use common_datavalues::DataTypeImpl;
use common_exception::ErrorCode;
use common_exception::Result;

pub trait EvalContext: Debug {
    type VectorID: PartialEq + Eq + Clone + Debug;

    fn get_vector(&self, id: &Self::VectorID) -> Result<TypedVector>;

    fn tuple_count(&self) -> usize;
}

impl EvalContext for DataBlock {
    type VectorID = String;

    fn get_vector(&self, id: &String) -> Result<TypedVector> {
        let column = self.try_column_by_name(id.as_str())?;
        let field = self.schema().field_with_name(id.as_str())?;
        Ok(TypedVector {
            vector: column.clone(),
            logical_type: field.data_type().clone(),
        })
    }

    fn tuple_count(&self) -> usize {
        self.num_rows()
    }
}

// An empty EvalContext, should only be used in constant folding
#[derive(Debug)]
pub(super) struct EmptyEvalContext<T> {
    _phantom: std::marker::PhantomData<T>,
}

impl<T> EmptyEvalContext<T> {
    pub fn new() -> Self {
        Self {
            _phantom: Default::default(),
        }
    }
}

impl<T> EvalContext for EmptyEvalContext<T>
where T: PartialEq + Eq + Clone + Debug
{
    type VectorID = T;

    fn get_vector(&self, _id: &Self::VectorID) -> Result<TypedVector> {
        Err(ErrorCode::Ok("Try to get vector from an empty context"))
    }

    fn tuple_count(&self) -> usize {
        1
    }
}

#[derive(Clone, Debug)]
pub struct TypedVector {
    pub(super) vector: ColumnRef,
    pub(super) logical_type: DataTypeImpl,
}

impl TypedVector {
    pub fn new(data: ColumnRef, logical_type: DataTypeImpl) -> Self {
        Self {
            vector: data,
            logical_type,
        }
    }

    pub fn logical_type(&self) -> DataTypeImpl {
        self.logical_type.clone()
    }

    pub fn physical_type(&self) -> DataTypeImpl {
        self.vector.data_type()
    }

    pub fn vector(&self) -> &ColumnRef {
        &self.vector
    }
}
