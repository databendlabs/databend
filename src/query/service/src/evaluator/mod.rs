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

mod eval_node;
mod physical_scalar;
mod scalar;

use common_datavalues::ColumnRef;
use common_datavalues::DataTypeImpl;
pub use eval_node::EvalNode;

pub struct Evaluator;

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
